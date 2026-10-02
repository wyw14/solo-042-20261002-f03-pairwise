package probe

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type CalibrationNotice struct {
	ProbeID   string
	VesselID  string
	OldEpoch  uint64
	NewEpoch  uint64
	OrderID   uuid.UUID
	Operator  string
	ChangedAt time.Time
}

type CalibrationListener func(CalibrationNotice)

// PressureWindowResetter clears the pressure stability window when a probe
// calibration advances, so pre-calibration samples cannot drive soak checks.
type PressureWindowResetter interface {
	Reset(vesselID string, epoch uint64) error
}

type CalibrationService struct {
	registry  *Registry
	windows   PressureWindowResetter
	mu        sync.Mutex
	orders    map[uuid.UUID]model.CalibrationOrder
	byProbe   map[string][]uuid.UUID
	listeners []CalibrationListener
	now       func() time.Time
}

func NewCalibrationService(registry *Registry, windows PressureWindowResetter, now func() time.Time) *CalibrationService {
	if now == nil {
		now = time.Now
	}
	return &CalibrationService{
		registry: registry,
		windows:  windows,
		orders:   make(map[uuid.UUID]model.CalibrationOrder),
		byProbe:  make(map[string][]uuid.UUID),
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

// Request opens a calibration order for a probe. A second request while an
// order is still pending is rejected as a duplicate submission.
func (s *CalibrationService) Request(probeID, operator string) (model.CalibrationOrder, error) {
	probeState, ok := s.registry.Get(probeID)
	if !ok {
		return model.CalibrationOrder{}, errors.New("probe not found")
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingLocked(probeState.ID) != uuid.Nil {
		return model.CalibrationOrder{}, errors.New("calibration already requested for this probe")
	}
	order, err := model.NewCalibrationOrder(probeState.ID, operator, probeState.CalibrationEpoch, now)
	if err != nil {
		return model.CalibrationOrder{}, err
	}
	s.orders[order.ID] = order
	s.byProbe[probeState.ID] = append(s.byProbe[probeState.ID], order.ID)
	return order, nil
}

// Execute advances the probe calibration epoch and clears the pressure window
// as one step. If the window cannot be cleared the probe epoch is rolled back,
// so a failed calibration never leaves the probe newer than its pressure
// window. Resubmitting an order that already ran changes nothing.
func (s *CalibrationService) Execute(orderID uuid.UUID) (model.CalibrationOrder, CalibrationNotice, error) {
	s.mu.Lock()
	order, ok := s.orders[orderID]
	if !ok {
		s.mu.Unlock()
		return model.CalibrationOrder{}, CalibrationNotice{}, errors.New("calibration order not found")
	}
	if order.Status != model.CalibrationPending {
		s.mu.Unlock()
		return order, CalibrationNotice{}, errors.New("calibration order has already been executed")
	}
	probeState, ok := s.registry.Get(order.ProbeID)
	if !ok {
		order, _ = order.MarkFailed("probe no longer exists", s.now())
		s.orders[orderID] = order
		s.mu.Unlock()
		return order, CalibrationNotice{}, errors.New("probe not found")
	}
	if probeState.CalibrationEpoch != order.ExpectedEpoch {
		// The probe generation moved after the request (e.g. a concurrent
		// calibration); executing now would desynchronise the pressure window.
		order, _ = order.MarkFailed("probe calibration epoch changed before execution", s.now())
		s.orders[orderID] = order
		s.mu.Unlock()
		return order, CalibrationNotice{}, errors.New("probe calibration epoch no longer matches the order")
	}

	oldEpoch := probeState.CalibrationEpoch
	now := s.now()
	updated, err := s.registry.Update(order.ProbeID, func(current model.Probe) (model.Probe, error) {
		if current.CalibrationEpoch != order.ExpectedEpoch {
			return model.Probe{}, errors.New("probe calibration epoch moved during calibration")
		}
		return current.Calibrate(now), nil
	})
	if err != nil {
		order, _ = order.MarkFailed(err.Error(), now)
		s.orders[orderID] = order
		s.mu.Unlock()
		return order, CalibrationNotice{}, err
	}

	if s.windows != nil {
		if resetErr := s.windows.Reset(probeState.VesselID, updated.CalibrationEpoch); resetErr != nil {
			// Roll the probe back so its epoch cannot sit ahead of the window.
			if _, rollbackErr := s.registry.Update(order.ProbeID, func(current model.Probe) (model.Probe, error) {
				current.CalibrationEpoch = oldEpoch
				current.UpdatedAt = now.UTC()
				return current, nil
			}); rollbackErr != nil {
				s.mu.Unlock()
				return model.CalibrationOrder{}, CalibrationNotice{}, rollbackErr
			}
			failed, _ := order.MarkFailed(resetErr.Error(), now)
			s.orders[orderID] = failed
			s.mu.Unlock()
			return failed, CalibrationNotice{}, resetErr
		}
	}

	succeeded, err := order.MarkSucceeded(updated.CalibrationEpoch, now)
	if err != nil {
		s.mu.Unlock()
		return model.CalibrationOrder{}, CalibrationNotice{}, err
	}
	s.orders[orderID] = succeeded
	notice := CalibrationNotice{
		ProbeID:   updated.ID,
		VesselID:  updated.VesselID,
		OldEpoch:  oldEpoch,
		NewEpoch:  updated.CalibrationEpoch,
		OrderID:   succeeded.ID,
		Operator:  succeeded.Operator,
		ChangedAt: now.UTC(),
	}
	listeners := append([]CalibrationListener(nil), s.listeners...)
	s.mu.Unlock()

	for _, listener := range listeners {
		listener(notice)
	}
	return succeeded, notice, nil
}

func (s *CalibrationService) Get(orderID uuid.UUID) (model.CalibrationOrder, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	order, ok := s.orders[orderID]
	return order, ok
}

func (s *CalibrationService) List(probeID string) []model.CalibrationOrder {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.byProbe[probeID]
	result := make([]model.CalibrationOrder, 0, len(ids))
	for _, id := range ids {
		if order, ok := s.orders[id]; ok {
			result = append(result, order)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RequestedAt.Before(result[j].RequestedAt) })
	return result
}

func (s *CalibrationService) pendingLocked(probeID string) uuid.UUID {
	for _, id := range s.byProbe[probeID] {
		if order, ok := s.orders[id]; ok && order.Status == model.CalibrationPending {
			return id
		}
	}
	return uuid.Nil
}
