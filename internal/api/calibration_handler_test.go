package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
)

func newCalibrationServer(t *testing.T, probeID string, kind model.ProbeKind, vesselID string) *Server {
	t.Helper()
	now := time.Unix(100, 0)
	probes := probe.NewRegistry(8)
	probeState, err := model.NewProbe(probeID, kind, vesselID, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	calibrations := probe.NewCalibrationService(probes, func() time.Time { return now })
	return NewServer(Dependencies{
		Probes:       probes,
		Calibrations: calibrations,
		Now:          func() time.Time { return now },
	})
}

func doJSON(t *testing.T, server *Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, reader)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	var decoded map[string]any
	if recorder.Body.Len() > 0 {
		if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("response is not json (%d): %s", recorder.Code, recorder.Body.String())
		}
	}
	return recorder.Code, decoded
}

func TestCalibrationEndToEndLifecycle(t *testing.T) {
	server := newCalibrationServer(t, "P-P07", model.ProbePressure, "V-07")

	status, body := doJSON(t, server, http.MethodPost, "/api/probes/P-P07/calibrations", `{"operator":"alice"}`)
	if status != http.StatusCreated {
		t.Fatalf("create status = %d, body = %v", status, body)
	}
	id, _ := body["id"].(string)
	if id == "" || body["operator"] != "alice" || body["status"] != "pending" {
		t.Fatalf("unexpected create response: %v", body)
	}
	if oldEpoch := body["old_epoch"]; oldEpoch != float64(1) {
		t.Fatalf("old_epoch = %v", oldEpoch)
	}

	// Duplicate application before execution is rejected.
	if status, dup := doJSON(t, server, http.MethodPost, "/api/probes/P-P07/calibrations", `{"operator":"bob"}`); status != http.StatusConflict {
		t.Fatalf("duplicate request status = %d, body = %v", status, dup)
	}

	// Execute with an empty body (operator captured at request time).
	if status, executed := doJSON(t, server, http.MethodPost, "/api/calibrations/"+id+"/execute", ""); status != http.StatusOK {
		t.Fatalf("execute status = %d, body = %v", status, executed)
	} else if executed["status"] != "succeeded" || executed["new_epoch"] != float64(2) {
		t.Fatalf("unexpected execute response: %v", executed)
	}

	// Repeated execution is idempotent: no new epoch.
	if status, repeated := doJSON(t, server, http.MethodPost, "/api/calibrations/"+id+"/execute", `{"operator":"alice"}`); status != http.StatusOK {
		t.Fatalf("repeat execute status = %d, body = %v", status, repeated)
	} else if repeated["new_epoch"] != float64(2) {
		t.Fatalf("repeat execute bumped epoch: %v", repeated)
	}

	status, body = doJSON(t, server, http.MethodGet, "/api/calibrations/"+id, "")
	if status != http.StatusOK || body["status"] != "succeeded" || body["operator"] != "alice" {
		t.Fatalf("query response = %d, %v", status, body)
	}

	status, body = doJSON(t, server, http.MethodGet, "/api/probes/P-P07/calibrations", "")
	if status != http.StatusOK {
		t.Fatalf("list status = %d, body = %v", status, body)
	}
	items, _ := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one calibration history item, got %v", body["items"])
	}
}

func TestCalibrationEndpointsValidateInputs(t *testing.T) {
	server := newCalibrationServer(t, "P-P07", model.ProbePressure, "V-07")

	if status, _ := doJSON(t, server, http.MethodPost, "/api/probes/missing/calibrations", `{"operator":"alice"}`); status != http.StatusNotFound {
		t.Fatalf("missing probe status = %d", status)
	}
	if status, body := doJSON(t, server, http.MethodPost, "/api/probes/P-P07/calibrations", `{"operator":"  "}`); status != http.StatusBadRequest {
		t.Fatalf("blank operator status = %d, body = %v", status, body)
	}
	if status, body := doJSON(t, server, http.MethodPost, "/api/calibrations/nope/execute", ""); status != http.StatusNotFound {
		t.Fatalf("missing calibration status = %d, body = %v", status, body)
	}
	if status, _ := doJSON(t, server, http.MethodGet, "/api/probes/missing/calibrations", ""); status != http.StatusNotFound {
		t.Fatalf("list missing probe status = %d", status)
	}
	// Malformed JSON is rejected.
	req := httptest.NewRequest(http.MethodPost, "/api/probes/P-P07/calibrations", strings.NewReader("{not-json"))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("malformed body status = %d", recorder.Code)
	}
}
