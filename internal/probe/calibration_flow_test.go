package probe_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/pressure"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/telemetry"
	"github.com/wyw14/cry-98/internal/vessel"
)

type flowHarness struct {
	vessels    *vessel.Registry
	probes     *probe.Registry
	ingestor   *telemetry.Ingestor
	monitor    *pressure.Monitor
	calibs     *probe.CalibrationService
	probeState model.Probe
}

func newFlowHarness(t *testing.T, sink probe.EpochSink) *flowHarness {
	t.Helper()
	now := time.Unix(500, 0)
	clock := func() time.Time { return now }

	vessels := vessel.NewRegistry()
	vesselState, err := model.NewVessel("V-07", "Vessel 07", "north", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}

	probes := probe.NewRegistry(8)
	probeState, err := model.NewProbe("P-P07", model.ProbePressure, "V-07", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}

	ingestor := telemetry.NewIngestor(probes, vessel.NewStateService(vessels, clock), telemetry.NewSnapshotStore())
	monitor := pressure.NewMonitor(clock)
	if err := monitor.AddVessel("V-07", 3, 1.5); err != nil {
		t.Fatal(err)
	}
	calibs := probe.NewCalibrationService(probes, clock)
	if sink != nil {
		if err := calibs.AddEpochSink(sink); err != nil {
			t.Fatal(err)
		}
	}
	return &flowHarness{
		vessels:    vessels,
		probes:     probes,
		ingestor:   ingestor,
		monitor:    monitor,
		calibs:     calibs,
		probeState: probeState,
	}
}

func TestCalibrationFlowOldEpochSamplesRejectedEndToEnd(t *testing.T) {
	h := newFlowHarness(t, nil)
	if err := h.calibs.AddEpochSink(func(notice probe.CalibrationNotice) error {
		if notice.Kind != model.ProbePressure {
			t.Fatalf("sink notice kind = %s", notice.Kind)
		}
		return h.monitor.Reset(notice.VesselID, notice.NewEpoch)
	}); err != nil {
		t.Fatal(err)
	}

	oldSample := h.probeState.NewSample(99, time.Unix(509, 0))
	// Prime the window with epoch-1 samples.
	for i := 0; i < 3; i++ {
		sample := h.probeState.NewSample(10, time.Unix(501+int64(i), 0))
		if err := h.ingestor.Ingest(sample); err != nil {
			t.Fatal(err)
		}
		if err := h.monitor.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := h.monitor.Evaluate("V-07")
	if !before.Stable || before.Epoch != 1 {
		t.Fatalf("pre-calibration window = %+v", before)
	}

	record, err := h.calibs.Request("P-P07", "alice")
	if err != nil {
		t.Fatal(err)
	}
	committed, notice, err := h.calibs.Execute(context.Background(), record.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if committed.OldEpoch != 1 || committed.NewEpoch != 2 {
		t.Fatalf("calibration epochs = %d->%d", committed.OldEpoch, committed.NewEpoch)
	}
	if notice.OldEpoch != 1 || notice.NewEpoch != 2 || notice.Operator != "alice" {
		t.Fatalf("notice missing audit fields: %+v", notice)
	}
	result, err := h.monitor.Evaluate("V-07")
	if err != nil {
		t.Fatal(err)
	}
	if result.Epoch != 2 || result.Samples != 0 {
		t.Fatalf("window did not reset to new epoch: %+v", result)
	}

	// Old-generation samples must not touch the vessel state.
	if err := h.ingestor.Ingest(oldSample); err == nil {
		t.Fatal("old-epoch sample was accepted by the ingestor after calibration")
	}
	if err := h.monitor.Add(oldSample); !errors.Is(err, pressure.ErrStaleEpoch) {
		t.Fatalf("old-epoch pressure sample error = %v, want ErrStaleEpoch", err)
	}
	if _, dropped := h.ingestor.Stats(); dropped != 1 {
		t.Fatalf("dropped count = %d, want 1", dropped)
	}
	current, _ := h.vessels.Get("V-07")
	if current.PressureKPa == 99 {
		t.Fatal("stale sample overwrote vessel telemetry")
	}

	// New-generation samples flow through and can re-establish stability,
	// proving the old samples did not contaminate the new soak window.
	updated, _ := h.probes.Get("P-P07")
	for i := 0; i < 3; i++ {
		sample := updated.NewSample(20, time.Unix(510+int64(i), 0))
		if err := h.ingestor.Ingest(sample); err != nil {
			t.Fatal(err)
		}
		if err := h.monitor.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
	after, err := h.monitor.Evaluate("V-07")
	if err != nil {
		t.Fatal(err)
	}
	if !after.Stable || after.Epoch != 2 {
		t.Fatalf("post-calibration window = %+v", after)
	}
	current, _ = h.vessels.Get("V-07")
	if current.PressureKPa != 20 {
		t.Fatalf("vessel pressure = %v, want 20 from new epoch", current.PressureKPa)
	}
}

func TestCalibrationFlowFailingSinkKeepsEpochsConsistent(t *testing.T) {
	h := newFlowHarness(t, func(notice probe.CalibrationNotice) error {
		return errors.New("pressure window offline")
	})
	for i := 0; i < 3; i++ {
		sample := h.probeState.NewSample(10, time.Unix(501+int64(i), 0))
		if err := h.monitor.Add(sample); err != nil {
			t.Fatal(err)
		}
	}

	record, err := h.calibs.Request("P-P07", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.calibs.Execute(context.Background(), record.ID, ""); err == nil {
		t.Fatal("expected failed calibration")
	}
	state, _ := h.probes.Get("P-P07")
	if state.CalibrationEpoch != 1 {
		t.Fatalf("probe epoch = %d after sink failure, want 1", state.CalibrationEpoch)
	}
	result, err := h.monitor.Evaluate("V-07")
	if err != nil {
		t.Fatal(err)
	}
	if result.Epoch != 1 {
		t.Fatalf("pressure window epoch = %d after sink failure, want 1", result.Epoch)
	}

	// Current samples remain valid because epochs never diverged.
	sample := state.NewSample(10, time.Unix(510, 0))
	if err := h.ingestor.Ingest(sample); err != nil {
		t.Fatalf("valid sample rejected after rolled-back calibration: %v", err)
	}
	if err := h.monitor.Add(sample); err != nil {
		t.Fatalf("pressure window rejected matching epoch after rollback: %v", err)
	}
}
