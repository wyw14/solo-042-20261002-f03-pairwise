package pressure

import (
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

var ErrStaleEpoch = errors.New("pressure sample belongs to an obsolete calibration epoch")

type Result struct {
	VesselID  string    `json:"vessel_id"`
	Epoch     uint64    `json:"epoch"`
	Stable    bool      `json:"stable"`
	Samples   int       `json:"samples"`
	CheckedAt time.Time `json:"checked_at"`
}

type Monitor struct {
	mu      sync.RWMutex
	windows map[string]*Window
	now     func() time.Time
}

func NewMonitor(now func() time.Time) *Monitor {
	if now == nil {
		now = time.Now
	}
	return &Monitor{windows: make(map[string]*Window), now: now}
}

func (m *Monitor) AddVessel(vesselID string, capacity int, spread float64) error {
	window, err := NewWindow(vesselID, capacity, spread)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.windows[vesselID] = window
	m.mu.Unlock()
	return nil
}

func (m *Monitor) Reset(vesselID string, epoch uint64) error {
	m.mu.RLock()
	window := m.windows[vesselID]
	m.mu.RUnlock()
	if window == nil {
		return errors.New("pressure window not found")
	}
	window.Reset(epoch)
	return nil
}

func (m *Monitor) Add(sample model.Sample) error {
	m.mu.RLock()
	window := m.windows[sample.VesselID]
	m.mu.RUnlock()
	if window == nil {
		return errors.New("pressure window not found")
	}
	if err := window.Add(sample); err != nil {
		return err
	}
	return nil
}

// HasVessel reports whether a pressure window exists for the vessel.
func (m *Monitor) HasVessel(vesselID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.windows[vesselID]
	return ok
}

func (m *Monitor) Evaluate(vesselID string) (Result, error) {
	m.mu.RLock()
	window := m.windows[vesselID]
	m.mu.RUnlock()
	if window == nil {
		return Result{}, errors.New("pressure window not found")
	}
	return Result{
		VesselID:  vesselID,
		Epoch:     window.Epoch(),
		Stable:    window.Stable(),
		Samples:   len(window.Samples()),
		CheckedAt: m.now().UTC(),
	}, nil
}
