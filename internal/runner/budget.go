package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type budgetChargeRequest struct {
	StepID              string `json:"step_id"`
	StepIndex           int    `json:"step_index"`
	Tokens              *int   `json:"tokens"`
	CompletedAt         string `json:"completed_at"`
	RemainingWorkflowMS int64  `json:"remaining_workflow_ms"`
}

type budgetChargeResponse struct {
	Accepted bool   `json:"accepted"`
	Action   string `json:"action"`
	ResetAt  string `json:"reset_at"`
	Message  string `json:"message"`
}

/** Reports one completed agent step before the workflow may start another step. */
func reportBudgetCharge(ctx context.Context, request RunRequest, step WorkflowStep, index int, tokens *int, remaining time.Duration) (budgetChargeResponse, error) {
	payload, err := json.Marshal(budgetChargeRequest{
		StepID: step.ID, StepIndex: index, Tokens: tokens, CompletedAt: time.Now().UTC().Format(time.RFC3339Nano),
		RemainingWorkflowMS: max(remaining.Milliseconds(), 1),
	})
	if err != nil {
		return budgetChargeResponse{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, request.BudgetURL, bytes.NewReader(payload))
	if err != nil {
		return budgetChargeResponse{}, err
	}
	httpRequest.Header.Set("content-type", "application/json")
	httpRequest.Header.Set("authorization", "Bearer "+request.CallbackToken)
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(httpRequest)
	if err != nil {
		return budgetChargeResponse{}, fmt.Errorf("send token budget charge: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil || len(body) > 8192 {
		return budgetChargeResponse{}, fmt.Errorf("read token budget response")
	}
	var result budgetChargeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return budgetChargeResponse{}, fmt.Errorf("decode token budget response: %w", err)
	}
	if response.StatusCode != http.StatusOK || !result.Accepted || result.Action != "continue" && result.Action != "suspend" {
		return budgetChargeResponse{}, fmt.Errorf("token budget callback rejected charge with HTTP %d: %s", response.StatusCode, result.Message)
	}
	if result.Action == "suspend" {
		if resetAt, parseErr := time.Parse(time.RFC3339Nano, result.ResetAt); parseErr != nil || resetAt.IsZero() {
			return budgetChargeResponse{}, fmt.Errorf("token budget callback returned an invalid reset_at")
		}
	}
	return result, nil
}
