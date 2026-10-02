package pressure

import (
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

func TestWindowRejectsStaleEpochSamples(t *testing.T) {
	window, err := NewWindow("V-07", 3, 1.5)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(100, 0)
	makeSample := func(epoch uint64, value float64) model.Sample {
		return model.Sample{
			ProbeID:          "P-P07",
			VesselID:         "V-07",
			Kind:             model.ProbePressure,
			Value:            value,
			CalibrationEpoch: epoch,
			ObservedAt:       at,
		}
	}

	if !window.Add(makeSample(2, 10)) {
		t.Fatal("current-epoch sample was rejected")
	}
	if window.Add(makeSample(1, 10)) {
		t.Fatal("pre-calibration sample was accepted into the window")
	}
	if window.Epoch() != 2 {
		t.Fatalf("window epoch rewound to %d", window.Epoch())
	}
	if samples := window.Samples(); len(samples) != 1 || samples[0].CalibrationEpoch != 2 {
		t.Fatalf("window samples contaminated by old epoch: %+v", samples)
	}

	// A newer generation still resets and starts a fresh window.
	if !window.Add(makeSample(3, 10)) {
		t.Fatal("newer-epoch sample was rejected")
	}
	if window.Epoch() != 3 || len(window.Samples()) != 1 {
		t.Fatalf("window did not reset for newer epoch: epoch=%d", window.Epoch())
	}
}
