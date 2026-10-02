package api

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type calibrationRequest struct {
	Operator string `json:"operator"`
}

func (s *Server) requestProbeCalibration(writer http.ResponseWriter, request *http.Request) {
	probeID := chi.URLParam(request, "id")
	var input calibrationRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(input.Operator) == "" {
		writeError(writer, http.StatusBadRequest, "operator is required")
		return
	}
	if _, ok := s.deps.Probes.Get(probeID); !ok {
		writeError(writer, http.StatusNotFound, "probe not found")
		return
	}
	order, err := s.deps.Calibrations.Request(probeID, input.Operator)
	if err != nil {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, order)
}

func (s *Server) executeCalibration(writer http.ResponseWriter, request *http.Request) {
	orderID, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid calibration order id")
		return
	}
	order, _, err := s.deps.Calibrations.Execute(orderID)
	if err != nil {
		status := http.StatusConflict
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		writeError(writer, status, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, order)
}

func (s *Server) getCalibration(writer http.ResponseWriter, request *http.Request) {
	orderID, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid calibration order id")
		return
	}
	order, ok := s.deps.Calibrations.Get(orderID)
	if !ok {
		writeError(writer, http.StatusNotFound, "calibration order not found")
		return
	}
	writeJSON(writer, http.StatusOK, order)
}

func (s *Server) listProbeCalibrations(writer http.ResponseWriter, request *http.Request) {
	probeID := chi.URLParam(request, "id")
	if _, ok := s.deps.Probes.Get(probeID); !ok {
		writeError(writer, http.StatusNotFound, "probe not found")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.deps.Calibrations.List(probeID)})
}
