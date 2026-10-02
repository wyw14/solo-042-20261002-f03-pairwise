package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-98/internal/probe"
)

type calibrationRequest struct {
	Operator string `json:"operator"`
}

func (s *Server) createCalibration(writer http.ResponseWriter, request *http.Request) {
	probeID := chi.URLParam(request, "id")
	var input calibrationRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if _, ok := s.deps.Probes.Get(probeID); !ok {
		writeError(writer, http.StatusNotFound, "probe not found")
		return
	}
	record, err := s.deps.Calibrations.Request(probeID, input.Operator)
	if err != nil {
		if errors.Is(err, probe.ErrCalibrationInFlight) {
			writeError(writer, http.StatusConflict, err.Error())
			return
		}
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, record)
}

func (s *Server) executeCalibration(writer http.ResponseWriter, request *http.Request) {
	requestID := chi.URLParam(request, "calibrationID")
	var input calibrationRequest
	if request.Body != nil {
		if err := decodeJSON(request, &input); err != nil && !errors.Is(err, io.EOF) {
			writeError(writer, http.StatusBadRequest, err.Error())
			return
		}
	}
	record, _, err := s.deps.Calibrations.Execute(request.Context(), requestID, input.Operator)
	if err != nil {
		switch {
		case errors.Is(err, probe.ErrCalibrationNotFound):
			writeError(writer, http.StatusNotFound, err.Error())
		case errors.Is(err, probe.ErrCalibrationResolved),
			errors.Is(err, probe.ErrCalibrationStale),
			errors.Is(err, probe.ErrCalibrationInFlight):
			writeError(writer, http.StatusConflict, err.Error())
		default:
			writeError(writer, http.StatusBadGateway, err.Error())
		}
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

func (s *Server) listCalibrations(writer http.ResponseWriter, request *http.Request) {
	probeID := chi.URLParam(request, "id")
	if _, ok := s.deps.Probes.Get(probeID); !ok {
		writeError(writer, http.StatusNotFound, "probe not found")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"items": s.deps.Calibrations.ListByProbe(probeID),
	})
}

func (s *Server) getCalibration(writer http.ResponseWriter, request *http.Request) {
	requestID := chi.URLParam(request, "calibrationID")
	record, err := s.deps.Calibrations.Get(requestID)
	if err != nil {
		if errors.Is(err, probe.ErrCalibrationNotFound) {
			writeError(writer, http.StatusNotFound, err.Error())
			return
		}
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, record)
}
