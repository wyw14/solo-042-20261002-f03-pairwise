package probe

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

type fakeResetter struct {
	epochs map[string]uint64
	fail   bool
}

func newFakeResetter() *fakeResetter {
	return &fakeResetter{epochs: make(map[string]uint64)}
}

func (f *fakeResetter) Reset(vesselID string, epoch uint64) error {
	if f.fail {
		return errors.New("pressure window unavailable")
	}
	f.epochs[vesselID] = epoch
	return nil
}

func newCalibrationFixture(t *testing.T, windows PressureWindowResetter) (*Registry, *CalibrationService, model.Probe) {
	t.Helper()
	now := func() time.Time { return time.Unix(100, 0) }
	registry := NewRegistry(4)
	probeState, err := model.NewProbe("P-P07", model.ProbePressure, "V-07", now())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(probeState); err != nil {
		t.Fatal(err)
	}
	return registry, NewCalibrationService(registry, windows, now), probeState
}

func TestCalibrationRequestExecuteRecordsOperatorAndEpochs(t *testing.T) {
	windows := newFakeResetter()
	registry, service, probeState := newCalibrationFixture(t, windows)

	order, err := service.Request("P-P07", "operator-42")
	if err != nil {
		t.Fatal(err)
	}
	if order.Status != model.CalibrationPending || order.Operator != "operator-42" {
		t.Fatalf("unexpected order: %+v", order)
	}
	if order.PreviousEpoch != 1 || order.ExpectedEpoch != 1 {
		t.Fatalf("epochs=%d/%d", order.PreviousEpoch, order.ExpectedEpoch)
	}

	done, notice, err := service.Execute(order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != model.CalibrationSucceeded || done.SucceededEpoch != 2 {
		t.Fatalf("unexpected executed order: %+v", done)
	}
	if notice.OldEpoch != 1 || notice.NewEpoch != 2 || notice.Operator != "operator-42" {
		t.Fatalf("unexpected notice: %+v", notice)
	}
	updated, _ := registry.Get("P-P07")
	if updated.CalibrationEpoch != 2 {
		t.Fatalf("probe epoch=%d", updated.CalibrationEpoch)
	}
	if windows.epochs["V-07"] != 2 {
		t.Fatalf("window epoch=%d, want 2", windows.epochs["V-07"])
	}
	_ = probeState
}

func TestCalibrationDuplicateRequestAndExecutionRejected(t *testing.T) {
	windows := newFakeResetter()
	registry, service, _ := newCalibrationFixture(t, windows)

	first, err := service.Request("P-P07", "operator-42")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Request("P-P07", "operator-42"); err == nil {
		t.Fatal("duplicate pending request was accepted")
	}

	if _, _, err := service.Execute(first.ID); err != nil {
		t.Fatal(err)
	}
	// Re-executing the same order must not advance the probe again.
	if _, _, err := service.Execute(first.ID); err == nil {
		t.Fatal("duplicate execution was accepted")
	}
	updated, _ := registry.Get("P-P07")
	if updated.CalibrationEpoch != 2 {
		t.Fatalf("probe epoch=%d, want 2", updated.CalibrationEpoch)
	}
	if windows.epochs["V-07"] != 2 {
		t.Fatalf("window epoch=%d, want 2", windows.epochs["V-07"])
	}
}

func TestCalibrationFailureRollsProbeEpochBack(t *testing.T) {
	windows := newFakeResetter()
	windows.fail = true
	registry, service, _ := newCalibrationFixture(t, windows)

	order, err := service.Request("P-P07", "operator-42")
	if err != nil {
		t.Fatal(err)
	}
	failed, _, err := service.Execute(order.ID)
	if err == nil {
		t.Fatal("expected execution failure")
	}
	if failed.Status != model.CalibrationFailed || failed.FailureReason == "" {
		t.Fatalf("order was not marked failed: %+v", failed)
	}
	updated, _ := registry.Get("P-P07")
	if updated.CalibrationEpoch != 1 {
		t.Fatalf("probe epoch=%d, want rollback to 1", updated.CalibrationEpoch)
	}
	if _, exists := windows.epochs["V-07"]; exists {
		t.Fatal("pressure window epoch must stay untouched on failure")
	}
}

func TestCalibrationGetAndList(t *testing.T) {
	windows := newFakeResetter()
	_, service, _ := newCalibrationFixture(t, windows)

	order, err := service.Request("P-P07", "operator-42")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := service.Get(order.ID); !ok {
		t.Fatal("order not found by id")
	}
	if items := service.List("P-P07"); len(items) != 1 || items[0].ID != order.ID {
		t.Fatalf("unexpected list: %+v", items)
	}
	if items := service.List("unknown"); len(items) != 0 {
		t.Fatalf("unexpected list for unknown probe: %+v", items)
	}
}

func TestCalibrationRequestUnknownProbe(t *testing.T) {
	registry := NewRegistry(4)
	service := NewCalibrationService(registry, newFakeResetter(), time.Now)
	if _, err := service.Request("P-XX", "operator-42"); err == nil {
		t.Fatal("request for unknown probe was accepted")
	}
}
