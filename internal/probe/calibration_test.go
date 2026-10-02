package probe

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

type testClock struct{ t time.Time }

func (c *testClock) now() time.Time { return c.t }

func newCalibrationFixture(t *testing.T) (*Registry, *CalibrationService, *testClock) {
	t.Helper()
	clock := &testClock{t: time.Unix(1000, 0)}
	registry := NewRegistry(4)
	probeState, err := model.NewProbe("P-P07", model.ProbePressure, "V-07", clock.now())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(probeState); err != nil {
		t.Fatal(err)
	}
	return registry, NewCalibrationService(registry, clock.now), clock
}

type recordingSink struct {
	mu       sync.Mutex
	failNext bool
	notices  []CalibrationNotice
}

func (s *recordingSink) apply(notice CalibrationNotice) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext {
		return errors.New("pressure window unavailable")
	}
	s.notices = append(s.notices, notice)
	return nil
}

func (s *recordingSink) calls() []CalibrationNotice {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]CalibrationNotice(nil), s.notices...)
}

func TestCalibrationRequestCapturesOperatorAndBaseEpoch(t *testing.T) {
	registry, service, _ := newCalibrationFixture(t)

	record, err := service.Request("P-P07", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != CalibrationPending || record.OldEpoch != 1 || record.NewEpoch != 0 {
		t.Fatalf("unexpected pending record: %+v", record)
	}
	if record.Operator != "alice" || record.ProbeID != "P-P07" || record.VesselID != "V-07" {
		t.Fatalf("record lost operator or identity: %+v", record)
	}

	if _, err := service.Request("P-P07", "bob"); !errors.Is(err, ErrCalibrationInFlight) {
		t.Fatalf("duplicate request error = %v, want ErrCalibrationInFlight", err)
	}
	if _, err := service.Request("P-P07", "alice"); !errors.Is(err, ErrCalibrationInFlight) {
		t.Fatalf("repeated identical submission should be rejected, got %v", err)
	}

	state, _ := registry.Get("P-P07")
	if state.CalibrationEpoch != 1 {
		t.Fatalf("request must not change probe epoch, got %d", state.CalibrationEpoch)
	}
}

func TestCalibrationExecuteCommitsAndIsIdempotent(t *testing.T) {
	registry, service, clock := newCalibrationFixture(t)
	sink := &recordingSink{}
	if err := service.AddEpochSink(sink.apply); err != nil {
		t.Fatal(err)
	}

	record, err := service.Request("P-P07", "alice")
	if err != nil {
		t.Fatal(err)
	}
	clock.t = clock.t.Add(time.Second)
	executed, notice, err := service.Execute(context.Background(), record.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if executed.Status != CalibrationSucceeded || executed.OldEpoch != 1 || executed.NewEpoch != 2 {
		t.Fatalf("unexpected executed record: %+v", executed)
	}
	if executed.ResolvedAt.IsZero() || !executed.ResolvedAt.After(executed.RequestedAt) {
		t.Fatal("resolved timestamp not recorded")
	}
	if notice.RequestID != record.ID || notice.OldEpoch != 1 || notice.NewEpoch != 2 ||
		notice.Kind != model.ProbePressure || notice.Operator != "alice" {
		t.Fatalf("unexpected notice: %+v", notice)
	}
	state, _ := registry.Get("P-P07")
	if state.CalibrationEpoch != 2 {
		t.Fatalf("probe epoch = %d, want 2", state.CalibrationEpoch)
	}

	// Duplicate execution of the same request must not bump anything again.
	repeated, _, err := service.Execute(context.Background(), record.ID, "alice")
	if err != nil {
		t.Fatalf("idempotent re-execute returned error: %v", err)
	}
	if repeated.NewEpoch != 2 || repeated.Status != CalibrationSucceeded {
		t.Fatalf("re-execute changed record: %+v", repeated)
	}
	state, _ = registry.Get("P-P07")
	if state.CalibrationEpoch != 2 {
		t.Fatalf("re-execute changed probe epoch to %d", state.CalibrationEpoch)
	}
	if calls := sink.calls(); len(calls) != 1 {
		t.Fatalf("sink invoked %d times, want exactly 1", len(calls))
	}

	// A fresh request after success is allowed and produces the next epoch.
	next, err := service.Request("P-P07", "bob")
	if err != nil {
		t.Fatal(err)
	}
	nextExec, _, err := service.Execute(context.Background(), next.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if nextExec.OldEpoch != 2 || nextExec.NewEpoch != 3 {
		t.Fatalf("second calibration epochs = %d->%d", nextExec.OldEpoch, nextExec.NewEpoch)
	}
}

func TestCalibrationSinkFailureRollsProbeBack(t *testing.T) {
	registry, service, _ := newCalibrationFixture(t)
	sink := &recordingSink{failNext: true}
	if err := service.AddEpochSink(sink.apply); err != nil {
		t.Fatal(err)
	}

	record, err := service.Request("P-P07", "alice")
	if err != nil {
		t.Fatal(err)
	}
	failed, _, err := service.Execute(context.Background(), record.ID, "")
	if err == nil {
		t.Fatal("expected sink failure to fail the calibration")
	}
	if failed.Status != CalibrationFailed || failed.Reason == "" || failed.NewEpoch != 0 {
		t.Fatalf("unexpected failed record: %+v", failed)
	}
	state, _ := registry.Get("P-P07")
	if state.CalibrationEpoch != 1 {
		t.Fatalf("probe epoch = %d after failure, want rollback to 1", state.CalibrationEpoch)
	}
	if calls := sink.calls(); len(calls) != 0 {
		t.Fatal("failed sink must not be treated as committed")
	}

	if _, _, err := service.Execute(context.Background(), record.ID, ""); !errors.Is(err, ErrCalibrationResolved) {
		t.Fatalf("re-executing failed request error = %v, want ErrCalibrationResolved", err)
	}
	state, _ = registry.Get("P-P07")
	if state.CalibrationEpoch != 1 {
		t.Fatalf("failed request retry changed probe epoch to %d", state.CalibrationEpoch)
	}

	// A new request can succeed after the failure; probe and sink end on epoch 2.
	sink.failNext = false
	retry, err := service.Request("P-P07", "alice")
	if err != nil {
		t.Fatal(err)
	}
	committed, notice, err := service.Execute(context.Background(), retry.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if committed.NewEpoch != 2 || notice.NewEpoch != 2 {
		t.Fatalf("retry calibration did not reach epoch 2: %+v", committed)
	}
	state, _ = registry.Get("P-P07")
	if state.CalibrationEpoch != 2 {
		t.Fatalf("probe epoch = %d, want 2", state.CalibrationEpoch)
	}
	if calls := sink.calls(); len(calls) != 1 || calls[0].NewEpoch != 2 {
		t.Fatalf("sink state after recovery: %+v", calls)
	}
}

func TestCalibrationExecuteRejectsRequestStaleAgainstProbe(t *testing.T) {
	registry, service, _ := newCalibrationFixture(t)
	record, err := service.Request("P-P07", "alice")
	if err != nil {
		t.Fatal(err)
	}
	// Probe epoch moved through some other path before execution.
	if _, err := registry.Update("P-P07", func(current model.Probe) (model.Probe, error) {
		return current.Calibrate(time.Now()), nil
	}); err != nil {
		t.Fatal(err)
	}
	failed, _, err := service.Execute(context.Background(), record.ID, "")
	if !errors.Is(err, ErrCalibrationStale) {
		t.Fatalf("error = %v, want ErrCalibrationStale", err)
	}
	if failed.Status != CalibrationFailed {
		t.Fatalf("stale request should be marked failed, got %s", failed.Status)
	}
	state, _ := registry.Get("P-P07")
	if state.CalibrationEpoch != 2 {
		t.Fatal("stale execute must not roll back or double-bump the probe")
	}
}

func TestCalibrationQueries(t *testing.T) {
	_, service, _ := newCalibrationFixture(t)
	if _, err := service.Get("missing"); !errors.Is(err, ErrCalibrationNotFound) {
		t.Fatalf("Get missing = %v, want ErrCalibrationNotFound", err)
	}
	first, err := service.Request("P-P07", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Execute(context.Background(), first.ID, ""); err != nil {
		t.Fatal(err)
	}
	second, err := service.Request("P-P07", "bob")
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := service.Get(first.ID)
	if err != nil || loaded.Operator != "alice" || loaded.Status != CalibrationSucceeded {
		t.Fatalf("Get returned %+v, %v", loaded, err)
	}
	byProbe := service.ListByProbe("P-P07")
	if len(byProbe) != 2 || byProbe[0].ID != first.ID || byProbe[1].ID != second.ID {
		t.Fatalf("per-probe history out of order: %+v", byProbe)
	}
	if all := service.List(); len(all) != 2 {
		t.Fatalf("global history length = %d", len(all))
	}
}
