package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	acp "github.com/coder/acp-go-sdk"
)

var errInteractionPending = errors.New("user input is pending")

const maxUserInputQuestions = 5

type InteractionKind string

const (
	InteractionPermission   InteractionKind = "permission"
	InteractionQuestion     InteractionKind = "question"
	InteractionPlanApproval InteractionKind = "plan_approval"
	InteractionElicitation  InteractionKind = "elicitation"
)

type InteractionRequest struct {
	Kind      InteractionKind `json:"kind"`
	SessionID string          `json:"session_id,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

type InteractionAction string

const (
	InteractionApprove InteractionAction = "approve"
	InteractionAccept  InteractionAction = "accept"
	InteractionDecline InteractionAction = "decline"
	InteractionCancel  InteractionAction = "cancel"
)

type InteractionDecision struct {
	Action        InteractionAction
	OptionID      string
	Content       map[string]any
	Pending       bool
	InteractionID string
}

type callbackBroker struct {
	url              string
	token            string
	maxRequestBytes  int
	maxResponseBytes int
	liveWait         time.Duration
	pause            func()
	resume           func()
	stepID           string
	remainingStep    func() time.Duration
}

type callbackResponse struct {
	Accepted      bool                 `json:"accepted"`
	State         string               `json:"state"`
	InteractionID string               `json:"interaction_id"`
	Response      *InteractionResponse `json:"response"`
	Message       string               `json:"message"`
}

type InteractionResponse struct {
	Action  InteractionAction `json:"action"`
	Content map[string]any    `json:"content,omitempty"`
}

/** Resolve forwards only standard ACP form elicitations to the authenticated Worker callback. */
func (broker callbackBroker) Resolve(ctx context.Context, request InteractionRequest) (InteractionDecision, error) {
	if request.Kind != InteractionElicitation {
		return nonInteractiveBroker{}.Resolve(ctx, request)
	}
	if len(request.Payload) == 0 || len(request.Payload) > broker.maxRequestBytes {
		return InteractionDecision{}, fmt.Errorf("elicitation request exceeds configured limit")
	}
	if broker.pause != nil {
		broker.pause()
	}
	resumeTimeouts := true
	defer func() {
		if resumeTimeouts && broker.resume != nil {
			broker.resume()
		}
	}()
	remainingStep := broker.liveWait
	if broker.remainingStep != nil {
		remainingStep = broker.remainingStep()
	}
	callbackPayload, err := json.Marshal(map[string]any{"request": json.RawMessage(request.Payload), "step_id": broker.stepID, "remaining_step_ms": max(remainingStep.Milliseconds(), 1)})
	if err != nil {
		return InteractionDecision{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, broker.url, bytes.NewReader(callbackPayload))
	if err != nil {
		return InteractionDecision{}, err
	}
	httpRequest.Header.Set("content-type", "application/json")
	httpRequest.Header.Set("authorization", "Bearer "+broker.token)
	client := &http.Client{Timeout: broker.liveWait + 10*time.Second}
	response, err := client.Do(httpRequest)
	if err != nil {
		return InteractionDecision{}, fmt.Errorf("send elicitation callback: %w", err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, int64(broker.maxResponseBytes)+1)
	body, err := io.ReadAll(limited)
	if err != nil || len(body) > broker.maxResponseBytes {
		return InteractionDecision{}, fmt.Errorf("read elicitation callback response")
	}
	var result callbackResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return InteractionDecision{}, fmt.Errorf("decode elicitation callback response: %w", err)
	}
	if (response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted) || !result.Accepted {
		return InteractionDecision{}, fmt.Errorf("elicitation callback rejected request with HTTP %d: %s", response.StatusCode, result.Message)
	}
	if result.State == "pending" {
		resumeTimeouts = false
		return InteractionDecision{Action: InteractionCancel, Pending: true, InteractionID: result.InteractionID}, nil
	}
	if result.State != "resolved" || result.Response == nil {
		return InteractionDecision{}, fmt.Errorf("elicitation callback returned an invalid response")
	}
	if result.Response.Action == InteractionDecline || result.Response.Action == InteractionCancel {
		resumeTimeouts = false
	}
	return InteractionDecision{Action: result.Response.Action, Content: result.Response.Content, InteractionID: result.InteractionID}, nil
}

type InteractionBroker interface {
	Resolve(context.Context, InteractionRequest) (InteractionDecision, error)
}

// nonInteractiveBroker approves sandbox execution and cancels requests that
// require human-provided content when no external provider is configured.
type nonInteractiveBroker struct{}

func (nonInteractiveBroker) Resolve(_ context.Context, request InteractionRequest) (InteractionDecision, error) {
	switch request.Kind {
	case InteractionPermission, InteractionPlanApproval:
		return InteractionDecision{Action: InteractionApprove}, nil
	case InteractionQuestion, InteractionElicitation:
		return InteractionDecision{Action: InteractionCancel}, nil
	default:
		return InteractionDecision{}, fmt.Errorf("unsupported interaction kind %q", request.Kind)
	}
}

func selectAllowOption(options []acp.PermissionOption) (acp.PermissionOptionId, bool) {
	for _, kind := range []acp.PermissionOptionKind{acp.PermissionOptionKindAllowAlways, acp.PermissionOptionKindAllowOnce} {
		for _, option := range options {
			if option.Kind == kind {
				return option.OptionId, true
			}
		}
	}
	return "", false
}
