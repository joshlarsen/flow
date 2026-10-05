package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
)

type fixedInteractionBroker struct{ decision InteractionDecision }

func (broker fixedInteractionBroker) Resolve(context.Context, InteractionRequest) (InteractionDecision, error) {
	return broker.decision, nil
}

func TestCallbackBrokerReturnsLiveResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("authorization") != "Bearer scoped-token" {
			t.Fatal("missing callback token")
		}
		writeJSON(response, http.StatusOK, map[string]any{"accepted": true, "state": "resolved", "interaction_id": "id", "response": map[string]any{"action": "accept", "content": map[string]any{"name": "Ada"}}})
	}))
	defer server.Close()
	paused, resumed := false, false
	broker := callbackBroker{
		url: server.URL, token: "scoped-token", maxRequestBytes: 4096, maxResponseBytes: 4096, liveWait: time.Second,
		pause: func() { paused = true }, resume: func() { resumed = true },
	}
	decision, err := broker.Resolve(context.Background(), InteractionRequest{Kind: InteractionElicitation, Payload: json.RawMessage(`{"mode":"form"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != InteractionAccept || decision.Content["name"] != "Ada" || decision.Pending || !paused || !resumed {
		t.Fatalf("unexpected decision: %#v, paused=%v resumed=%v", decision, paused, resumed)
	}
}

func TestCallbackBrokerKeepsTimeoutsPausedForLiveTurnBoundaryResponses(t *testing.T) {
	for _, action := range []InteractionAction{InteractionDecline, InteractionCancel} {
		t.Run(string(action), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				writeJSON(response, http.StatusOK, map[string]any{
					"accepted": true, "state": "resolved", "interaction_id": "id", "response": map[string]any{"action": action},
				})
			}))
			defer server.Close()
			paused, resumed := false, false
			broker := callbackBroker{
				url: server.URL, token: "scoped-token", maxRequestBytes: 4096, maxResponseBytes: 4096, liveWait: time.Second,
				pause: func() { paused = true }, resume: func() { resumed = true },
			}
			decision, err := broker.Resolve(context.Background(), InteractionRequest{Kind: InteractionElicitation, Payload: json.RawMessage(`{"mode":"form"}`)})
			if err != nil {
				t.Fatal(err)
			}
			if decision.Action != action || decision.InteractionID != "id" || !paused || resumed {
				t.Fatalf("unexpected turn-boundary decision: %#v, paused=%v resumed=%v", decision, paused, resumed)
			}
		})
	}
}

func TestCallbackBrokerMarksDeferredResponsePendingAndLeavesTimeoutsPaused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, http.StatusAccepted, map[string]any{"accepted": true, "state": "pending", "interaction_id": "id"})
	}))
	defer server.Close()
	paused, resumed := false, false
	broker := callbackBroker{url: server.URL, token: "scoped-token", maxRequestBytes: 4096, maxResponseBytes: 4096, liveWait: time.Second, pause: func() { paused = true }, resume: func() { resumed = true }}
	decision, err := broker.Resolve(context.Background(), InteractionRequest{Kind: InteractionElicitation, Payload: json.RawMessage(`{"mode":"form"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Pending || decision.Action != InteractionCancel || !paused || resumed {
		t.Fatalf("unexpected pending decision: %#v, paused=%v resumed=%v", decision, paused, resumed)
	}
}

func TestPausableTimeoutExcludesPausedDuration(t *testing.T) {
	ctx, timeout := newPausableTimeout(context.Background(), 40*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	timeout.Pause()
	time.Sleep(50 * time.Millisecond)
	if ctx.Err() != nil {
		t.Fatal("timeout elapsed while paused")
	}
	timeout.Resume()
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout did not resume")
	}
}

func TestACPFormElicitationReturnsAcceptedContent(t *testing.T) {
	config := testConfig()
	config.Workflow.Steps[0].AllowUserInput = true
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	runner.broker = fixedInteractionBroker{decision: InteractionDecision{Action: InteractionAccept, Content: map[string]any{"name": "Ada"}}}
	client := &runnerACPClient{runner: runner, state: &acpState{}, broker: runner.broker}
	response, err := client.UnstableCreateElicitation(context.Background(), acp.NewUnstableCreateElicitationRequestForm(acp.UnstableElicitationSchema{Properties: map[string]any{"name": map[string]any{"type": "string"}}}))
	if err != nil {
		t.Fatal(err)
	}
	if response.Accept == nil || response.Accept.Content["name"] != "Ada" {
		t.Fatalf("unexpected response: %#v", response)
	}
}

func TestNativeACPFormIsCancelledWithoutStepOptIn(t *testing.T) {
	runner := NewRunner(testConfig(), "job-1", testConfig().Workflow.Steps[0], &bytes.Buffer{})
	runner.broker = fixedInteractionBroker{decision: InteractionDecision{Action: InteractionAccept, Content: map[string]any{"name": "Ada"}}}
	client := &runnerACPClient{runner: runner, state: &acpState{}, broker: runner.broker}
	response, err := client.UnstableCreateElicitation(context.Background(), acp.NewUnstableCreateElicitationRequestForm(acp.UnstableElicitationSchema{Properties: map[string]any{"name": map[string]any{"type": "string"}}}))
	if err != nil || response.Cancel == nil {
		t.Fatalf("expected cancellation, got %#v, %v", response, err)
	}
}

func TestTurnBoundaryUserInputCancelsPromptForCheckpointing(t *testing.T) {
	tests := []struct {
		name     string
		decision InteractionDecision
	}{
		{name: "pending", decision: InteractionDecision{Action: InteractionCancel, Pending: true, InteractionID: "interaction-1"}},
		{name: "decline", decision: InteractionDecision{Action: InteractionDecline, InteractionID: "interaction-1"}},
		{name: "cancel", decision: InteractionDecision{Action: InteractionCancel, InteractionID: "interaction-1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig()
			config.Workflow.Steps[0].AllowUserInput = true
			runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
			runner.broker = fixedInteractionBroker{decision: test.decision}
			ctx, cancel := context.WithCancelCause(context.Background())
			runner.setPromptCancel(cancel)
			decision, err := runner.resolveElicitation(context.Background(), json.RawMessage(`{"mode":"form"}`))
			if err != nil || decision.Action != test.decision.Action || runner.pendingInteraction() != "interaction-1" {
				t.Fatalf("turn-boundary interaction was not recorded: %#v, %v", decision, err)
			}
			if !errors.Is(context.Cause(ctx), errInteractionPending) {
				t.Fatalf("prompt was not cancelled for checkpointing: %v", context.Cause(ctx))
			}
		})
	}
}

func TestAcceptedUserInputDoesNotCancelPrompt(t *testing.T) {
	config := testConfig()
	config.Workflow.Steps[0].AllowUserInput = true
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	runner.broker = fixedInteractionBroker{decision: InteractionDecision{Action: InteractionAccept, InteractionID: "interaction-1", Content: map[string]any{"answer": "no"}}}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runner.setPromptCancel(cancel)
	decision, err := runner.resolveElicitation(context.Background(), json.RawMessage(`{"mode":"form"}`))
	if err != nil || decision.Action != InteractionAccept || runner.pendingInteraction() != "" || context.Cause(ctx) != nil {
		t.Fatalf("accepted interaction interrupted the active prompt: %#v, %v, cause=%v", decision, err, context.Cause(ctx))
	}
}
