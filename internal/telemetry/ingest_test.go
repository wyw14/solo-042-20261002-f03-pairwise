package telemetry

import (
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

func TestIngestRejectsObsoleteBindingGeneration(t *testing.T) {
	now := time.Now
	vessels := vessel.NewRegistry()
	vesselState, _ := model.NewVessel("V-01", "Vessel", "north", now())
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}
	probes := probe.NewRegistry(4)
	probeState, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", now())
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	ingestor := NewIngestor(probes, vessel.NewStateService(vessels, now), NewSnapshotStore())
	stale := probeState.NewSample(50, now())
	if _, err := probe.NewLifecycle(probes, now).Rebind("P-01", "V-02"); err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Ingest(stale); err == nil {
		t.Fatal("obsolete binding sample was accepted")
	}
}

func TestIngestRejectsObsoleteCalibrationEpoch(t *testing.T) {
	now := time.Now
	vessels := vessel.NewRegistry()
	vesselState, _ := model.NewVessel("V-01", "Vessel", "north", now())
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}
	probes := probe.NewRegistry(4)
	probeState, _ := model.NewProbe("P-01", model.ProbePressure, "V-01", now())
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	states := vessel.NewStateService(vessels, now)
	snapshots := NewSnapshotStore()
	snapshots.Replace(vessels.List(), now())
	ingestor := NewIngestor(probes, states, snapshots)

	preCalibration := probeState.NewSample(999, now())
	// Advance the probe calibration epoch through a real calibration order.
	service := probe.NewCalibrationService(probes, nil, now)
	order, err := service.Request("P-01", "operator")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Execute(order.ID); err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Ingest(preCalibration); err == nil {
		t.Fatal("pre-calibration sample was accepted")
	}
	updated, _ := vessels.Get("V-01")
	if updated.PressureKPa == 999 {
		t.Fatal("stale sample changed vessel state")
	}
}
