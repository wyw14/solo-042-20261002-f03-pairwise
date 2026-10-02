package model

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

type CalibrationStatus string

const (
	CalibrationPending   CalibrationStatus = "pending"
	CalibrationSucceeded CalibrationStatus = "succeeded"
	CalibrationFailed    CalibrationStatus = "failed"
)

// CalibrationOrder records an operator request to calibrate a probe.
//
// PreviousEpoch is the probe generation in effect when the request was made,
// ExpectedEpoch is the generation that execution must observe before bumping
// the probe, and SucceededEpoch is the generation produced by a successful
// calibration. Old-generation samples are identified against SucceededEpoch.
type CalibrationOrder struct {
	ID             uuid.UUID         `json:"id"`
	ProbeID        string            `json:"probe_id"`
	Operator       string            `json:"operator"`
	Status         CalibrationStatus `json:"status"`
	PreviousEpoch  uint64            `json:"previous_epoch"`
	ExpectedEpoch  uint64            `json:"expected_epoch"`
	SucceededEpoch uint64            `json:"succeeded_epoch,omitempty"`
	FailureReason  string            `json:"failure_reason,omitempty"`
	RequestedAt    time.Time         `json:"requested_at"`
	ExecutedAt     time.Time         `json:"executed_at,omitempty"`
}

func NewCalibrationOrder(probeID, operator string, epoch uint64, now time.Time) (CalibrationOrder, error) {
	probeID = strings.TrimSpace(probeID)
	operator = strings.TrimSpace(operator)
	if probeID == "" {
		return CalibrationOrder{}, errors.New("probe id is required")
	}
	if operator == "" {
		return CalibrationOrder{}, errors.New("operator is required")
	}
	if epoch == 0 {
		return CalibrationOrder{}, errors.New("probe calibration epoch is invalid")
	}
	return CalibrationOrder{
		ID:            uuid.New(),
		ProbeID:       probeID,
		Operator:      operator,
		Status:        CalibrationPending,
		PreviousEpoch: epoch,
		ExpectedEpoch: epoch,
		RequestedAt:   now.UTC(),
	}, nil
}

func (o CalibrationOrder) MarkSucceeded(succeededEpoch uint64, now time.Time) (CalibrationOrder, error) {
	if o.Status != CalibrationPending {
		return CalibrationOrder{}, errors.New("calibration order is not pending")
	}
	if succeededEpoch <= o.ExpectedEpoch {
		return CalibrationOrder{}, errors.New("calibration epoch did not advance")
	}
	o.Status = CalibrationSucceeded
	o.SucceededEpoch = succeededEpoch
	o.ExecutedAt = now.UTC()
	return o, nil
}

func (o CalibrationOrder) MarkFailed(reason string, now time.Time) (CalibrationOrder, error) {
	if o.Status != CalibrationPending {
		return CalibrationOrder{}, errors.New("calibration order is not pending")
	}
	o.Status = CalibrationFailed
	o.FailureReason = strings.TrimSpace(reason)
	o.ExecutedAt = now.UTC()
	return o, nil
}

// Final reports whether the order can no longer be executed.
func (o CalibrationOrder) Final() bool {
	return o.Status == CalibrationSucceeded || o.Status == CalibrationFailed
}
