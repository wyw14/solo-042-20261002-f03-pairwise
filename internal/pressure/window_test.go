package pressure

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

func pressureSample(vesselID string, epoch uint64, value float64, at time.Time) model.Sample {
	return model.Sample{
		ProbeID:          "P-P07",
		VesselID:         vesselID,
		Kind:             model.ProbePressure,
		Value:            value,
		CalibrationEpoch: epoch,
		ObservedAt:       at,
	}
}

func TestWindowRejectsStaleEpochSamples(t *testing.T) {
	window, err := NewWindow("V-07", 3, 1.5)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(100, 0)
	for i, value := range []float64{10, 10.5, 11} {
		if err := window.Add(pressureSample("V-07", 1, value, base.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}

	// Reset represents a successful calibration to epoch 2.
	window.Reset(2)
	if err := window.Add(pressureSample("V-07", 1, 99, base.Add(10*time.Second))); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale epoch add error = %v, want ErrStaleEpoch", err)
	}
	for i, value := range []float64{10, 10.5, 11} {
		if err := window.Add(pressureSample("V-07", 2, value, base.Add(time.Duration(11+i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	if !window.Stable() {
		t.Fatal("window should be stable on epoch-2 samples; stale epoch-1 sample leaked in")
	}
	if epoch := window.Epoch(); epoch != 2 {
		t.Fatalf("window epoch = %d, want 2", epoch)
	}
}

func TestMonitorAddRejectsStaleEpoch(t *testing.T) {
	monitor := NewMonitor(func() time.Time { return time.Unix(100, 0) })
	if err := monitor.AddVessel("V-07", 3, 1.5); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(200, 0)
	if err := monitor.Add(pressureSample("V-07", 2, 10, at)); err != nil {
		t.Fatal(err)
	}
	if err := monitor.Add(pressureSample("V-07", 1, 10, at.Add(time.Second))); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("monitor stale add error = %v, want ErrStaleEpoch", err)
	}
	if !monitor.HasVessel("V-07") || monitor.HasVessel("V-99") {
		t.Fatal("HasVessel returned unexpected result")
	}
}
