package probe

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

const (
	CalibrationPending   = "pending"
	CalibrationSucceeded = "succeeded"
	CalibrationFailed    = "failed"
)

var (
	ErrCalibrationInFlight = errors.New("probe already has a pending calibration request")
	ErrCalibrationNotFound = errors.New("calibration request not found")
	ErrCalibrationStale    = errors.New("calibration request targets an obsolete calibration epoch")
	ErrCalibrationResolved = errors.New("calibration request has already been resolved")
)

// CalibrationNotice describes a committed calibration epoch change.
type CalibrationNotice struct {
	RequestID string
	ProbeID   string
	Kind      model.ProbeKind
	VesselID  string
	OldEpoch  uint64
	NewEpoch  uint64
	Operator  string
	ChangedAt time.Time
}

// CalibrationListener is notified after a calibration has committed on every
// epoch-bearing component (probe registry and every registered sink).
type CalibrationListener func(CalibrationNotice)

// EpochSink applies a calibration epoch change to external epoch-bearing
// state (for example the pressure stability window). Returning a non-nil
// error promises that the sink kept its previous epoch: the calibration is
// rolled back on the probe, so probe and sink epochs never diverge.
type EpochSink func(notice CalibrationNotice) error

// CalibrationRecord captures one calibration request from application through
// execution, including who operated it and the epochs it moved between.
type CalibrationRecord struct {
	ID          string    `json:"id"`
	ProbeID     string    `json:"probe_id"`
	VesselID    string    `json:"vessel_id"`
	Operator    string    `json:"operator"`
	OldEpoch    uint64    `json:"old_epoch"`
	NewEpoch    uint64    `json:"new_epoch"`
	Status      string    `json:"status"`
	Reason      string    `json:"reason,omitempty"`
	RequestedAt time.Time `json:"requested_at"`
	ResolvedAt  time.Time `json:"resolved_at,omitempty"`
}

type CalibrationService struct {
	registry  *Registry
	mu        sync.Mutex
	listeners []CalibrationListener
	sinks     []EpochSink
	records   map[string]*CalibrationRecord
	order     []string
	pending   map[string]string
	now       func() time.Time
}

func NewCalibrationService(registry *Registry, now func() time.Time) *CalibrationService {
	if now == nil {
		now = time.Now
	}
	return &CalibrationService{
		registry: registry,
		records:  make(map[string]*CalibrationRecord),
		pending:  make(map[string]string),
		now:      now,
	}
}

func (s *CalibrationService) Subscribe(listener CalibrationListener) error {
	if listener == nil {
		return errors.New("calibration listener is required")
	}
	s.mu.Lock()
	s.listeners = append(s.listeners, listener)
	s.mu.Unlock()
	return nil
}

// AddEpochSink registers a component that must move to the new calibration
// epoch together with the probe. Sinks run before the calibration commits.
func (s *CalibrationService) AddEpochSink(sink EpochSink) error {
	if sink == nil {
		return errors.New("calibration epoch sink is required")
	}
	s.mu.Lock()
	s.sinks = append(s.sinks, sink)
	s.mu.Unlock()
	return nil
}

// Request opens a calibration request for a probe. The operator and the
// probe's current epoch are captured up front. Only one pending request per
// probe is allowed, so duplicate submissions cannot stack epoch changes.
func (s *CalibrationService) Request(probeID, operator string) (CalibrationRecord, error) {
	probeID = strings.TrimSpace(probeID)
	operator = strings.TrimSpace(operator)
	if operator == "" {
		return CalibrationRecord{}, errors.New("calibration operator is required")
	}
	probeState, ok := s.registry.Get(probeID)
	if !ok {
		return CalibrationRecord{}, errors.New("probe not found")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pending[probeID]; exists {
		return CalibrationRecord{}, ErrCalibrationInFlight
	}
	record := &CalibrationRecord{
		ID:          uuid.NewString(),
		ProbeID:     probeState.ID,
		VesselID:    probeState.VesselID,
		Operator:    operator,
		OldEpoch:    probeState.CalibrationEpoch,
		Status:      CalibrationPending,
		RequestedAt: s.now().UTC(),
	}
	s.records[record.ID] = record
	s.order = append(s.order, record.ID)
	s.pending[probeID] = record.ID
	return *record, nil
}

// Execute commits a previously requested calibration. It is idempotent:
// re-executing a succeeded request returns the stored record without bumping
// any epoch. If an epoch sink fails, the probe is rolled back to the old
// epoch and the request is marked failed, so probe and pressure window epochs
// never diverge. A failed request is terminal; a fresh Request is required.
func (s *CalibrationService) Execute(ctx context.Context, requestID, operator string) (CalibrationRecord, CalibrationNotice, error) {
	if err := ctx.Err(); err != nil {
		return CalibrationRecord{}, CalibrationNotice{}, err
	}
	operator = strings.TrimSpace(operator)

	s.mu.Lock()
	record, ok := s.records[requestID]
	if !ok {
		s.mu.Unlock()
		return CalibrationRecord{}, CalibrationNotice{}, ErrCalibrationNotFound
	}
	if record.Status == CalibrationSucceeded {
		// Duplicate submission: report the existing result, change nothing.
		result := *record
		s.mu.Unlock()
		return result, CalibrationNotice{}, nil
	}
	if record.Status == CalibrationFailed {
		result := *record
		s.mu.Unlock()
		return result, CalibrationNotice{}, ErrCalibrationResolved
	}
	if operator != "" {
		record.Operator = operator
	}
	probeState, exists := s.registry.Get(record.ProbeID)
	if !exists {
		s.failLocked(record, "probe is no longer registered")
		result := *record
		s.mu.Unlock()
		return result, CalibrationNotice{}, errors.New("probe not found")
	}
	if probeState.CalibrationEpoch != record.OldEpoch {
		s.failLocked(record, fmt.Sprintf("probe moved to epoch %d after request captured %d",
			probeState.CalibrationEpoch, record.OldEpoch))
		result := *record
		s.mu.Unlock()
		return result, CalibrationNotice{}, fmt.Errorf("%w: probe epoch %d, request base epoch %d",
			ErrCalibrationStale, probeState.CalibrationEpoch, record.OldEpoch)
	}

	committedAt := s.now().UTC()
	updated, err := s.registry.Update(record.ProbeID, func(current model.Probe) (model.Probe, error) {
		return current.Calibrate(committedAt), nil
	})
	if err != nil {
		s.failLocked(record, err.Error())
		result := *record
		s.mu.Unlock()
		return result, CalibrationNotice{}, err
	}

	notice := CalibrationNotice{
		RequestID: record.ID,
		ProbeID:   updated.ID,
		Kind:      updated.Kind,
		VesselID:  updated.VesselID,
		OldEpoch:  record.OldEpoch,
		NewEpoch:  updated.CalibrationEpoch,
		Operator:  record.Operator,
		ChangedAt: committedAt,
	}

	// Move every epoch-bearing component to the new epoch before commit.
	// A sink failure rolls the probe back while execution is still
	// serialized, so no new-epoch sample can reach the vessel or window.
	var sinkErr error
	for _, sink := range s.sinks {
		if sinkErr = sink(notice); sinkErr != nil {
			break
		}
	}
	if sinkErr != nil {
		reason := sinkErr.Error()
		if _, rbErr := s.registry.Update(record.ProbeID, func(current model.Probe) (model.Probe, error) {
			current.CalibrationEpoch = notice.OldEpoch
			current.UpdatedAt = s.now().UTC()
			return current, nil
		}); rbErr != nil {
			reason = fmt.Sprintf("epoch sink failed: %v; rollback also failed: %v", sinkErr, rbErr)
		}
		s.failLocked(record, reason)
		result := *record
		s.mu.Unlock()
		return result, CalibrationNotice{}, fmt.Errorf("calibration %s failed: %w", record.ID, sinkErr)
	}

	record.VesselID = updated.VesselID
	record.NewEpoch = updated.CalibrationEpoch
	s.succeedLocked(record)
	listeners := append([]CalibrationListener(nil), s.listeners...)
	result := *record
	s.mu.Unlock()

	for _, listener := range listeners {
		listener(notice)
	}
	return result, notice, nil
}

// failLocked marks a request failed and releases its per-probe pending slot.
// Caller must hold s.mu.
func (s *CalibrationService) failLocked(record *CalibrationRecord, reason string) {
	record.Status = CalibrationFailed
	record.Reason = reason
	record.NewEpoch = 0
	record.ResolvedAt = s.now().UTC()
	if s.pending[record.ProbeID] == record.ID {
		delete(s.pending, record.ProbeID)
	}
}

// succeedLocked marks a request successful and releases its pending slot.
// Caller must hold s.mu.
func (s *CalibrationService) succeedLocked(record *CalibrationRecord) {
	record.Status = CalibrationSucceeded
	record.Reason = ""
	record.ResolvedAt = s.now().UTC()
	if s.pending[record.ProbeID] == record.ID {
		delete(s.pending, record.ProbeID)
	}
}

func (s *CalibrationService) Get(requestID string) (CalibrationRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[requestID]
	if !ok {
		return CalibrationRecord{}, ErrCalibrationNotFound
	}
	return *record, nil
}

func (s *CalibrationService) List() []CalibrationRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked(s.order)
}

func (s *CalibrationService) ListByProbe(probeID string) []CalibrationRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0)
	for _, id := range s.order {
		if s.records[id].ProbeID == probeID {
			ids = append(ids, id)
		}
	}
	return s.listLocked(ids)
}

// listLocked copies records in the given id order. Caller must hold s.mu.
func (s *CalibrationService) listLocked(ids []string) []CalibrationRecord {
	result := make([]CalibrationRecord, 0, len(ids))
	for _, id := range ids {
		if record, ok := s.records[id]; ok {
			result = append(result, *record)
		}
	}
	return result
}
