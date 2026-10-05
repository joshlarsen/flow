package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
)

func testConfig() Config {
	return Config{
		Port: 8080, MaxPromptBytes: 65536, MaxPromptFiles: 100, MaxPromptBundleBytes: 1 << 20,
		MaxAssetBytes: 5 << 20, MaxAssetFiles: 500, MaxAssetBundleBytes: 25 << 20,
		WorkflowTimeoutMS: 2000, DefaultStepTimeoutMS: 2000, ShutdownGraceMS: 50,
		MaxResultBytes: 1 << 20, MaxLogBytes: 240000, MaxArtifactFiles: 50, MaxArtifactFileBytes: 5 << 20, MaxArtifactTotalBytes: 25 << 20,
		MaxMemoryBytes: 25 << 20, MemoryPersistenceMS: 120000,
		Providers: map[string]ProviderConfig{
			"xai": {Protocol: "xai", BaseURL: "https://api.x.ai/v1", StaticHeaders: map[string]string{}},
		},
		Models: map[string]map[string]string{"grok-4.6": {"xai": "grok-4.6"}},
		Harnesses: map[string]HarnessConfig{
			"grok": {Type: "grok"},
		},
		Workflow:      &WorkflowConfig{Name: "default", BundleDigest: strings.Repeat("a", 64), Steps: []WorkflowStep{{ID: "run", Prompt: "foo.md", Harness: "grok", Provider: "xai", Model: "grok-4.6", ModelID: "grok-4.6", TimeoutMS: 2000}}},
		Interactions:  InteractionsConfig{Provider: "none", CheckpointMaxFiles: 10000, CheckpointMaxFileBytes: 128 << 20, CheckpointMaxTotalBytes: 256 << 20},
		CredentialEnv: []string{"GH_TOKEN", "LINEAR_API_KEY"},
		RuntimeEnv:    []string{"GO_WANT_ACP_HELPER", "ACP_HELPER_SCENARIO", "GO_WANT_COMMAND_HELPER", "COMMAND_HELPER_SCENARIO"},
	}
}

func configuredRunner(t *testing.T, config Config, scenario string, output *bytes.Buffer) *Runner {
	t.Helper()
	t.Setenv("GROK_PATH", fakeACPAgent(t, scenario))
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], output)
	runner.workspace = filepath.Join(t.TempDir(), "workspace")
	runner.proxyToken = "scoped-token"
	return runner
}

func configuredCodexRunner(t *testing.T, scenario string, output *bytes.Buffer) *Runner {
	t.Helper()
	config := testConfig()
	config.Providers["openai"] = ProviderConfig{Protocol: "openai-responses", BaseURL: "https://api.openai.com/v1", StaticHeaders: map[string]string{}}
	config.Models["codex-model"] = map[string]string{"openai": "gpt-5.6-terra"}
	config.Harnesses["codex"] = HarnessConfig{Type: "codex"}
	step := WorkflowStep{ID: "run", Prompt: "foo.md", Harness: "codex", Provider: "openai", Model: "codex-model", ModelID: "gpt-5.6-terra", TimeoutMS: 2000}
	config.Workflow.Steps = []WorkflowStep{step}
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("CODEX_ACP_PATH", fakeACPAgent(t, scenario))
	t.Setenv("CODEX_HOME", filepath.Join(t.TempDir(), "codex"))
	runner := NewRunner(config, "job-1", step, output)
	runner.workspace = filepath.Join(t.TempDir(), "workspace")
	return runner
}

func fakeACPAgent(t *testing.T, scenario string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-acp-agent")
	script := "#!/bin/sh\nexec " + strconv.Quote(os.Args[0]) + " -test.run=^TestACPHelperProcess$\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GO_WANT_ACP_HELPER", "1")
	t.Setenv("ACP_HELPER_SCENARIO", scenario)
	return path
}

func TestACPHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_ACP_HELPER") != "1" {
		return
	}
	scenario := os.Getenv("ACP_HELPER_SCENARIO")
	if scenario == "startup_stderr" {
		_, _ = os.Stderr.WriteString("final startup diagnostic")
		os.Exit(7)
	}
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			continue
		}
		switch request.Method {
		case "initialize":
			result := map[string]any{
				"protocolVersion": 1, "agentCapabilities": map[string]any{}, "authMethods": []any{},
			}
			if scenario == "grok_runtime_metadata" {
				result["_meta"] = map[string]any{"modelState": map[string]any{
					"currentModelId": "grok-4.6",
					"availableModels": []any{map[string]any{
						"modelId": "grok-4.6", "name": "grok-4.6",
						"_meta": map[string]any{"agentType": "grok-build-plan", "totalContextTokens": 500000},
					}},
				}}
			}
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
			if scenario == "auth_status_notification" {
				_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": codexAuthStatusUpdateMethod, "params": map[string]any{
					"authStatus": map[string]any{"kind": "gateway", "label": "Custom model gateway", "detail": "cloudflare_proxy"},
				}})
			}
		case "session/new":
			result := map[string]any{"sessionId": "session-1"}
			if scenario == "grok_lifecycle_notifications" {
				for _, method := range []string{
					grokMCPInitProgressMethod,
					grokMCPServersUpdatedMethod,
					grokMCPServerStatusMethod,
					grokMCPInitializedMethod,
					grokSessionsChangedMethod,
					grokQueueChangedMethod,
					grokSessionNotificationMethod,
					grokSessionPromptCompleteMethod,
				} {
					_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": method, "params": map[string]any{"ignored": true}})
				}
			}
			if strings.HasPrefix(scenario, "acp_reasoning") && scenario != "acp_reasoning_missing" {
				options := []any{map[string]any{"value": "low", "name": "Low"}, map[string]any{"value": "high", "name": "High"}}
				if scenario == "acp_reasoning_unsupported" {
					options = options[:1]
				}
				configID := "reasoning_effort"
				if scenario == "acp_reasoning_claude" {
					configID = "effort"
				}
				result["configOptions"] = []any{map[string]any{
					"id": configID, "name": "Reasoning effort", "type": "select", "category": "thought_level", "currentValue": "low", "options": options,
				}}
			}
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
		case "session/resume":
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{}})
		case "session/set_config_option":
			var params struct {
				ConfigID string `json:"configId"`
				Value    string `json:"value"`
			}
			_ = json.Unmarshal(request.Params, &params)
			if scenario == "acp_reasoning_rejected" {
				_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32602, "message": "unsupported effort"}})
				continue
			}
			effective := params.Value
			if scenario == "acp_reasoning_clamped" {
				effective = "low"
			}
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{
				"configOptions": []any{map[string]any{
					"id": params.ConfigID, "name": "Reasoning effort", "type": "select", "category": "thought_level", "currentValue": effective,
					"options": []any{map[string]any{"value": "low", "name": "Low"}, map[string]any{"value": "high", "name": "High"}},
				}},
			}})
		case "session/prompt":
			if scenario == "timeout" {
				time.Sleep(5 * time.Second)
				continue
			}
			if scenario == "prompt_error" {
				_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32603, "message": "prompt failed"}})
				continue
			}
			if scenario == "deferred_interaction" {
				_ = encoder.Encode(map[string]any{
					"jsonrpc": "2.0", "id": "elicitation-1", "method": "elicitation/create",
					"params": map[string]any{
						"message": "Continue?", "mode": "form",
						"requestedSchema": map[string]any{"properties": map[string]any{"answer": map[string]any{"type": "string"}}},
					},
				})
				continue
			}
			if scenario == "grok_deferred_interaction" {
				_ = encoder.Encode(map[string]any{
					"jsonrpc": "2.0", "id": "grok-elicitation-1", "method": "_x.ai/ask_user_question",
					"params": map[string]any{
						"sessionId": "session-1", "toolCallId": "tool-1", "mode": "default",
						"questions": []any{map[string]any{
							"question": "Favorite animal?", "multiSelect": false,
							"options": []any{map[string]any{"label": "Cat"}, map[string]any{"label": "Dog"}},
						}},
					},
				})
				continue
			}
			if scenario == "tool_failure" {
				_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
					"sessionId": "session-1", "update": map[string]any{"sessionUpdate": "tool_call", "toolCallId": "tool-1", "title": "example", "status": "pending"},
				}})
				_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
					"sessionId": "session-1", "update": map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "tool-1", "status": "failed"},
				}})
			}
			if scenario == "codex_retry_recovered" || scenario == "codex_terminal_error" || scenario == "codex_terminal_error_without_diagnostic" {
				if scenario != "codex_terminal_error_without_diagnostic" {
					_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
						"sessionId": "session-1", "update": map[string]any{"sessionUpdate": "session_info_update", "_meta": map[string]any{
							"codex": map[string]any{"error": map[string]any{"message": "Reconnecting... 1/5", "additionalDetails": "unexpected status 401 for scoped-token", "willRetry": true}},
						}},
					}})
				}
				if scenario != "codex_retry_recovered" {
					_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
						"sessionId": "session-1", "update": map[string]any{"sessionUpdate": "session_info_update", "_meta": map[string]any{
							"codex": map[string]any{"threadStatus": map[string]any{"type": "systemError"}},
						}},
					}})
				}
			}
			message := "done"
			if scenario == "oversized" {
				message = strings.Repeat("x", 1025)
			}
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
				"sessionId": "session-1", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": message}},
			}})
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
				"sessionId": "session-1", "update": map[string]any{"sessionUpdate": "usage_update", "used": 12, "size": 128000},
			}})
			stopReason := "end_turn"
			if scenario == "partial" {
				stopReason = "max_tokens"
			}
			result := map[string]any{"stopReason": stopReason}
			if scenario == "missing_token_usage" {
				// Deliberately omit normalized model token usage.
			} else if scenario == "meta_usage" {
				result["_meta"] = map[string]any{"usage": map[string]any{
					"inputTokens": 100, "outputTokens": 10, "totalTokens": 110,
					"cachedReadTokens": 20, "cachedWriteTokens": 10, "reasoningTokens": 4,
				}}
			} else {
				result["usage"] = map[string]any{
					"inputTokens": 70, "outputTokens": 10, "totalTokens": 110,
					"cachedReadTokens": 20, "cachedWriteTokens": 10, "thoughtTokens": 4,
				}
			}
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
		case "session/close":
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{}})
			os.Exit(0)
		}
	}
	os.Exit(0)
}

func fakeCommand(t *testing.T, scenario string) []string {
	t.Helper()
	t.Setenv("GO_WANT_COMMAND_HELPER", "1")
	t.Setenv("COMMAND_HELPER_SCENARIO", scenario)
	return []string{os.Args[0], "-test.run=^TestCommandHelperProcess$"}
}

func TestCommandHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_COMMAND_HELPER") != "1" {
		return
	}
	switch os.Getenv("COMMAND_HELPER_SCENARIO") {
	case "success":
		_, _ = os.Stdout.WriteString("command output\n")
		_, _ = os.Stderr.WriteString("command warning\n")
	case "fast_output":
		_, _ = os.Stdout.WriteString("verified")
		_, _ = os.Stderr.WriteString("final warning")
	case "environment":
		_, _ = os.Stdout.WriteString(os.Getenv("GH_TOKEN") + "|" + os.Getenv("LINEAR_API_KEY") + "|" + os.Getenv("AWS_SECRET_ACCESS_KEY"))
	case "failure":
		_, _ = os.Stderr.WriteString("preflight rejected\n")
		os.Exit(7)
	case "timeout":
		time.Sleep(5 * time.Second)
	case "oversized":
		_, _ = os.Stdout.WriteString(strings.Repeat("x", 2048))
	case "endless_output":
		for {
			_, _ = os.Stdout.WriteString(strings.Repeat("x", 2048))
		}
	}
	os.Exit(0)
}

func TestRunnerCapturesSuccessfulACPResult(t *testing.T) {
	var output bytes.Buffer
	runner := configuredRunner(t, testConfig(), "success", &output)
	completion := runner.Execute(context.Background(), "hello")
	if completion.Status != "succeeded" || completion.Message != "done" || completion.SessionID == nil || *completion.SessionID != "session-1" {
		t.Fatalf("unexpected completion: %+v, error: %+v, logs: %s", completion, completion.Error, output.String())
	}
	if completion.Usage == nil || completion.Usage.Used == nil || *completion.Usage.Used != 12 {
		t.Fatalf("usage not captured: %+v", completion.Usage)
	}
	if completion.Usage.TotalTokens == nil || *completion.Usage.TotalTokens != 110 || completion.Usage.InputTokens == nil || *completion.Usage.InputTokens != 70 {
		t.Fatalf("prompt token usage not captured: %+v", completion.Usage)
	}
	if completion.Usage.TotalInputTokens == nil || *completion.Usage.TotalInputTokens != 100 {
		t.Fatalf("total input usage not derived: %+v", completion.Usage)
	}
	if !strings.Contains(output.String(), `"event_type":"session/update"`) || !strings.Contains(output.String(), `"harness":"grok"`) || !strings.Contains(output.String(), `"step_id":"run"`) {
		t.Fatalf("expected canonical ACP logs: %s", output.String())
	}
}

func TestRunnerExportsDeferredUserInputAsInformationalTelemetry(t *testing.T) {
	config := testConfig()
	config.Interactions.Provider = "callback"
	config.Workflow.Steps[0].AllowUserInput = true
	var output bytes.Buffer
	runner := configuredRunner(t, config, "deferred_interaction", &output)
	runner.broker = fixedInteractionBroker{decision: InteractionDecision{Action: InteractionCancel, Pending: true, InteractionID: "interaction-1"}}

	completion := runner.Execute(context.Background(), "ask")
	if completion.Status != "waiting_for_input" || runner.pendingInteraction() != "interaction-1" {
		t.Fatalf("unexpected deferred completion: %+v, logs: %s", completion, output.String())
	}
	runner.emitStepSuspended(completion)

	seenElicitation, seenPrompt, seenStep := false, false, false
	scanner := bufio.NewScanner(strings.NewReader(output.String()))
	for scanner.Scan() {
		var event struct {
			Level     string `json:"level"`
			EventType string `json:"event_type"`
			Event     struct {
				Phase  string `json:"phase"`
				Result struct {
					Action string `json:"action"`
					Status string `json:"status"`
				} `json:"result"`
				Status string `json:"status"`
			} `json:"event"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		if event.Level == "error" {
			t.Fatalf("deferred interaction emitted an error event: %s", scanner.Text())
		}
		switch {
		case event.EventType == "elicitation/create" && event.Event.Phase == "response":
			seenElicitation = event.Level == "info" && event.Event.Result.Action == "cancel" && event.Event.Result.Status == "waiting_for_input"
		case event.EventType == "session/prompt" && event.Event.Phase == "response":
			seenPrompt = event.Level == "info" && event.Event.Result.Status == "waiting_for_input"
		case event.EventType == "step.suspended":
			seenStep = event.Level == "info" && event.Event.Status == "waiting_for_input"
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !seenElicitation || !seenPrompt || !seenStep {
		t.Fatalf("missing normalized waiting telemetry: elicitation=%t prompt=%t step=%t logs=%s", seenElicitation, seenPrompt, seenStep, output.String())
	}
}

func TestRunnerNormalizesGrokDeferredQuestionTelemetry(t *testing.T) {
	config := testConfig()
	config.Interactions.Provider = "callback"
	config.Workflow.Steps[0].AllowUserInput = true
	var output bytes.Buffer
	runner := configuredRunner(t, config, "grok_deferred_interaction", &output)
	runner.broker = fixedInteractionBroker{decision: InteractionDecision{Action: InteractionCancel, Pending: true, InteractionID: "interaction-1"}}

	completion := runner.Execute(context.Background(), "ask")
	if completion.Status != "waiting_for_input" {
		t.Fatalf("unexpected deferred completion: %+v, logs: %s", completion, output.String())
	}
	logs := output.String()
	if strings.Contains(logs, `"event_type":"_x.ai/ask_user_question"`) || strings.Contains(logs, `"event_type":"x.ai/ask_user_question"`) {
		t.Fatalf("Grok-native question escaped into telemetry: %s", logs)
	}
	if !strings.Contains(logs, `"event_type":"elicitation/create"`) || !strings.Contains(logs, `"status":"waiting_for_input"`) {
		t.Fatalf("canonical deferred elicitation telemetry missing: %s", logs)
	}
}

func TestRunnerCheckpointsLiveTurnBoundaryResponses(t *testing.T) {
	for _, action := range []InteractionAction{InteractionDecline, InteractionCancel} {
		t.Run(string(action), func(t *testing.T) {
			config := testConfig()
			config.Interactions.Provider = "callback"
			config.Workflow.Steps[0].AllowUserInput = true
			var output bytes.Buffer
			runner := configuredRunner(t, config, "deferred_interaction", &output)
			runner.broker = fixedInteractionBroker{decision: InteractionDecision{Action: action, InteractionID: "interaction-1"}}

			completion := runner.Execute(context.Background(), "ask")
			if completion.Status != "waiting_for_input" || runner.pendingInteraction() != "interaction-1" {
				t.Fatalf("live %s did not checkpoint the prompt: %+v, logs: %s", action, completion, output.String())
			}
			if strings.Contains(output.String(), `"level":"error"`) {
				t.Fatalf("live %s emitted an error event: %s", action, output.String())
			}
		})
	}
}

func TestResumedTurnAllowsFurtherInputOnlyAfterAcceptedContent(t *testing.T) {
	for _, test := range []struct {
		action              InteractionAction
		wantElicitationTool bool
	}{
		{action: InteractionAccept, wantElicitationTool: true},
		{action: InteractionDecline, wantElicitationTool: false},
		{action: InteractionCancel, wantElicitationTool: false},
	} {
		t.Run(string(test.action), func(t *testing.T) {
			config := testConfig()
			config.Interactions.Provider = "callback"
			config.Workflow.Steps[0].AllowUserInput = true
			config.Workflow.Steps[0].RequiredMetrics = []RequiredMetric{{Namespace: "haiku", Key: "num_lines", Description: "number of lines"}}
			workspace := filepath.Join(t.TempDir(), "workspace")
			if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(workspace, "prompts", "foo.md"), []byte("hello"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("RUNNER_WORKSPACE", workspace)
			t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
			t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
			t.Setenv("GROK_PATH", fakeACPAgent(t, "success"))
			var output bytes.Buffer
			originalStartedAt := "2026-09-05T10:00:00Z"
			result := executeWorkflow(context.Background(), config, RunRequest{
				JobID: "job-1", DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano), InteractionURL: "https://runner.test/interactions",
				Resume: &ResumeRequest{
					StepIndex: 0, RemainingStepMS: 1000, SessionID: "session-1", Response: &InteractionResponse{Action: test.action},
					Steps: []StepCompletion{{ID: config.Workflow.Steps[0].ID, Status: "running", StartedAt: originalStartedAt, EmittedMetrics: []string{"haiku.num_lines"}}},
				},
			}, &output, nil, func(WorkflowCompletion) {})
			if result.Status != "succeeded" {
				t.Fatalf("resumed workflow failed: %+v, logs: %s", result, output.String())
			}
			if result.Steps[0].StartedAt != originalStartedAt || !strings.Contains(output.String(), `"event_type":"step.resumed"`) || strings.Contains(output.String(), `"event_type":"step.started"`) {
				t.Fatalf("resume did not preserve the logical step lifecycle: result=%+v logs=%s", result.Steps[0], output.String())
			}

			foundInitialize, hasElicitationTool, promptIncludesMetrics, resumeIncludesMetrics := false, false, false, false
			scanner := bufio.NewScanner(strings.NewReader(output.String()))
			for scanner.Scan() {
				var event struct {
					EventType string          `json:"event_type"`
					Event     json.RawMessage `json:"event"`
				}
				if json.Unmarshal(scanner.Bytes(), &event) != nil {
					continue
				}
				if event.EventType == "session/prompt" {
					promptIncludesMetrics = promptIncludesMetrics || strings.Contains(string(event.Event), "mcp.metrics.emit") && strings.Contains(string(event.Event), "haiku.num_lines")
					continue
				}
				if event.EventType == "session/resume" {
					var payload struct {
						Phase  string `json:"phase"`
						Params struct {
							MCPServers []struct {
								Name string `json:"name"`
							} `json:"mcpServers"`
						} `json:"params"`
					}
					if json.Unmarshal(event.Event, &payload) == nil && payload.Phase == "request" && len(payload.Params.MCPServers) == 1 && payload.Params.MCPServers[0].Name == "metrics" {
						resumeIncludesMetrics = true
					}
					continue
				}
				if event.EventType != "initialize" {
					continue
				}
				var payload struct {
					Phase  string `json:"phase"`
					Params struct {
						ClientCapabilities map[string]json.RawMessage `json:"clientCapabilities"`
					} `json:"params"`
				}
				if json.Unmarshal(event.Event, &payload) == nil && payload.Phase == "request" {
					foundInitialize = true
					_, hasElicitationTool = payload.Params.ClientCapabilities["elicitation"]
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
			if !foundInitialize || hasElicitationTool != test.wantElicitationTool {
				t.Fatalf("unexpected resumed elicitation capability: found=%t capability=%t logs=%s", foundInitialize, hasElicitationTool, output.String())
			}
			if !promptIncludesMetrics {
				t.Fatalf("resumed prompt omitted required metric instructions: %s", output.String())
			}
			if !resumeIncludesMetrics {
				t.Fatalf("resumed session omitted the required metrics MCP server: %s", output.String())
			}
		})
	}
}

func TestRunnerKeepsActualPromptFailuresAsErrors(t *testing.T) {
	var output bytes.Buffer
	completion := configuredRunner(t, testConfig(), "prompt_error", &output).Execute(context.Background(), "hello")
	if completion.Status != "failed" || completion.Error == nil || completion.Error.Code != "acp_prompt_failed" {
		t.Fatalf("unexpected prompt failure: %+v, logs: %s", completion, output.String())
	}
	if !strings.Contains(output.String(), `"level":"error"`) || !strings.Contains(output.String(), `"event_type":"session/prompt"`) {
		t.Fatalf("actual prompt failure was not exported as an error: %s", output.String())
	}
}

func TestRunnerFailsCodexTerminalSystemErrors(t *testing.T) {
	var output bytes.Buffer
	runner := configuredCodexRunner(t, "codex_terminal_error", &output)
	completion := runner.Execute(context.Background(), "hello")
	if completion.Status != "failed" || completion.Error == nil || completion.Error.Code != "acp_prompt_failed" || completion.Error.Retryable {
		t.Fatalf("unexpected terminal failure: %+v, logs: %s", completion, output.String())
	}
	if completion.Error.Message != "unexpected status 401 for [REDACTED]" {
		t.Fatalf("unexpected redacted diagnostic: %q", completion.Error.Message)
	}
	if strings.Count(output.String(), `"level":"error"`) != 2 {
		t.Fatalf("retry and terminal updates were not exported as errors: %s", output.String())
	}
}

func TestRunnerAllowsCodexRetryRecovery(t *testing.T) {
	var output bytes.Buffer
	completion := configuredCodexRunner(t, "codex_retry_recovered", &output).Execute(context.Background(), "hello")
	if completion.Status != "succeeded" || completion.Error != nil {
		t.Fatalf("recoverable retry failed the turn: %+v, logs: %s", completion, output.String())
	}
	if strings.Count(output.String(), `"level":"error"`) != 1 {
		t.Fatalf("retry was not exported as an error: %s", output.String())
	}
}

func TestRunnerUsesFallbackForCodexTerminalSystemError(t *testing.T) {
	var output bytes.Buffer
	completion := configuredCodexRunner(t, "codex_terminal_error_without_diagnostic", &output).Execute(context.Background(), "hello")
	if completion.Status != "failed" || completion.Error == nil || completion.Error.Message != "Harness reported a terminal system error" {
		t.Fatalf("unexpected terminal fallback: %+v, logs: %s", completion, output.String())
	}
}

func TestWorkflowStopsAfterCodexTerminalSystemError(t *testing.T) {
	config := testConfig()
	config.Providers["openai"] = ProviderConfig{Protocol: "openai-responses", BaseURL: "https://api.openai.com/v1", StaticHeaders: map[string]string{}}
	config.Models["codex-model"] = map[string]string{"openai": "gpt-5.6-terra"}
	config.Harnesses["codex"] = HarnessConfig{Type: "codex"}
	config.Workflow.Steps = []WorkflowStep{
		{ID: "generate", Prompt: "foo.md", Harness: "codex", Provider: "openai", Model: "codex-model", ModelID: "gpt-5.6-terra", TimeoutMS: 2000},
		{ID: "should-not-run", Command: []string{"sh", "-c", "exit 99"}, TimeoutMS: 2000},
	}
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "prompts", "foo.md"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUNNER_WORKSPACE", workspace)
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("CODEX_ACP_PATH", fakeACPAgent(t, "codex_terminal_error"))
	t.Setenv("CODEX_HOME", filepath.Join(t.TempDir(), "codex"))
	var output bytes.Buffer
	result := executeWorkflow(context.Background(), config, RunRequest{JobID: "job-1", DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}, &output, nil, func(WorkflowCompletion) {})
	if result.Status != "failed" || result.Error == nil || result.Error.Code != "acp_prompt_failed" {
		t.Fatalf("unexpected workflow failure: %+v, logs: %s", result, output.String())
	}
	if result.Steps[0].Status != "failed" || result.Steps[1].Status != "pending" {
		t.Fatalf("workflow did not stop after the failed Codex step: %+v", result.Steps)
	}
	if !strings.Contains(output.String(), `"event_type":"step.finished"`) || !strings.Contains(output.String(), `"level":"error"`) {
		t.Fatalf("failed step lifecycle was not emitted as an error: %s", output.String())
	}
}

func TestRunnerHandlesCodexAuthStatusNotification(t *testing.T) {
	var output bytes.Buffer
	runner := configuredRunner(t, testConfig(), "auth_status_notification", &output)
	completion := runner.Execute(context.Background(), "hello")
	if completion.Status != "succeeded" {
		t.Fatalf("auth status notification failed the run: %+v, error: %+v, logs: %s", completion, completion.Error, output.String())
	}

	count := 0
	scanner := bufio.NewScanner(strings.NewReader(output.String()))
	for scanner.Scan() {
		var event struct {
			Level     string         `json:"level"`
			EventType string         `json:"event_type"`
			Event     map[string]any `json:"event"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil || event.EventType != codexAuthStatusUpdateMethod {
			continue
		}
		count++
		params, _ := event.Event["params"].(map[string]any)
		authStatus, _ := params["authStatus"].(map[string]any)
		if event.Level != "info" || event.Event["phase"] != "notification" || event.Event["direction"] != "agent_to_client" || authStatus["kind"] != "gateway" {
			t.Fatalf("unexpected auth status event: %#v", event)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 || strings.Contains(output.String(), "Method not found") {
		t.Fatalf("expected one successful auth status notification, got %d: %s", count, output.String())
	}
}

func TestRunnerSilentlyIgnoresGrokLifecycleNotifications(t *testing.T) {
	var output bytes.Buffer
	completion := configuredRunner(t, testConfig(), "grok_lifecycle_notifications", &output).Execute(context.Background(), "hello")
	if completion.Status != "succeeded" {
		t.Fatalf("Grok lifecycle notifications failed the run: %+v, logs: %s", completion, output.String())
	}

	for _, method := range []string{
		grokMCPInitProgressMethod,
		grokMCPServersUpdatedMethod,
		grokMCPServerStatusMethod,
		grokMCPInitializedMethod,
		grokSessionsChangedMethod,
		grokQueueChangedMethod,
		grokSessionNotificationMethod,
		grokSessionPromptCompleteMethod,
	} {
		if strings.Contains(output.String(), `"event_type":"`+method+`"`) {
			t.Fatalf("ignored Grok lifecycle method %q was logged: %s", method, output.String())
		}
	}
	if strings.Contains(output.String(), `"ignored":true`) || strings.Contains(output.String(), "Method not found") {
		t.Fatalf("ignored Grok lifecycle payloads leaked into logs: %s", output.String())
	}
}

func TestRunnerConfiguresAndVerifiesACPReasoningEffort(t *testing.T) {
	config := testConfig()
	config.Providers["openai"] = ProviderConfig{Protocol: "openai-responses", BaseURL: "https://api.openai.com/v1", StaticHeaders: map[string]string{}}
	config.Models["codex-model"] = map[string]string{"openai": "gpt-5.6-terra"}
	config.Harnesses["codex"] = HarnessConfig{Type: "codex"}
	effort := "high"
	step := WorkflowStep{ID: "run", Prompt: "foo.md", Harness: "codex", Provider: "openai", Model: "codex-model", ModelID: "gpt-5.6-terra", ReasoningEffort: &effort, TimeoutMS: 2000}
	config.Workflow.Steps = []WorkflowStep{step}
	var output bytes.Buffer
	t.Setenv("CODEX_ACP_PATH", fakeACPAgent(t, "acp_reasoning_success"))
	t.Setenv("CODEX_HOME", filepath.Join(t.TempDir(), "codex"))
	runner := NewRunner(config, "job-1", step, &output)
	runner.workspace = filepath.Join(t.TempDir(), "workspace")
	runner.proxyToken = "scoped-token"
	completion := runner.Execute(context.Background(), "hello")
	if completion.Status != "succeeded" {
		t.Fatalf("unexpected completion: %+v, logs: %s", completion, output.String())
	}
	configurationIndex := strings.Index(output.String(), `"event_type":"session/set_config_option"`)
	promptIndex := strings.Index(output.String(), `"event_type":"session/prompt"`)
	if configurationIndex < 0 || promptIndex < 0 || configurationIndex > promptIndex || !strings.Contains(output.String(), `"value":"high"`) {
		t.Fatalf("reasoning effort was not configured before prompting: %s", output.String())
	}
}

func TestRunnerRejectsUnsupportedOrClampedReasoningEffort(t *testing.T) {
	for _, scenario := range []string{"acp_reasoning_missing", "acp_reasoning_unsupported", "acp_reasoning_rejected", "acp_reasoning_clamped"} {
		t.Run(scenario, func(t *testing.T) {
			config := testConfig()
			config.Providers["openai"] = ProviderConfig{Protocol: "openai-responses", BaseURL: "https://api.openai.com/v1", StaticHeaders: map[string]string{}}
			config.Models["codex-model"] = map[string]string{"openai": "gpt-5.6-terra"}
			config.Harnesses["codex"] = HarnessConfig{Type: "codex"}
			effort := "high"
			step := WorkflowStep{ID: "run", Prompt: "foo.md", Harness: "codex", Provider: "openai", Model: "codex-model", ModelID: "gpt-5.6-terra", ReasoningEffort: &effort, TimeoutMS: 2000}
			config.Workflow.Steps = []WorkflowStep{step}
			var output bytes.Buffer
			t.Setenv("CODEX_ACP_PATH", fakeACPAgent(t, scenario))
			t.Setenv("CODEX_HOME", filepath.Join(t.TempDir(), "codex"))
			runner := NewRunner(config, "job-1", step, &output)
			runner.workspace = filepath.Join(t.TempDir(), "workspace")
			runner.proxyToken = "scoped-token"
			completion := runner.Execute(context.Background(), "hello")
			if completion.Error == nil || completion.Error.Code != "acp_configuration_failed" || strings.Contains(output.String(), `"event_type":"session/prompt"`) {
				t.Fatalf("reasoning failure did not stop before prompt: %+v, logs: %s", completion, output.String())
			}
		})
	}
}

func TestGrokReasoningEffortUsesLaunchFlagWithoutACPOption(t *testing.T) {
	config := testConfig()
	effort := "medium"
	config.Workflow.Steps[0].ReasoningEffort = &effort
	var output bytes.Buffer
	runner := configuredRunner(t, config, "grok_runtime_metadata", &output)
	command, err := (grokDriver{}).Command(runner)
	if err != nil || !strings.Contains(strings.Join(command.Args, " "), "--reasoning-effort medium") {
		t.Fatalf("Grok command did not receive reasoning effort: %v, %v", command.Args, err)
	}
	completion := runner.Execute(context.Background(), "hello")
	if completion.Status != "succeeded" {
		t.Fatalf("unexpected Grok completion: %+v, logs: %s", completion, output.String())
	}
	if !strings.Contains(output.String(), `"event_type":"session/prompt"`) || strings.Contains(output.String(), `"event_type":"session/set_config_option"`) {
		t.Fatalf("Grok reasoning should use only the launch flag: %s", output.String())
	}
}

func TestClaudeReasoningEffortRequiresACPOption(t *testing.T) {
	config := testConfig()
	config.Providers["anthropic"] = ProviderConfig{Protocol: "anthropic", BaseURL: "https://api.anthropic.com/v1", StaticHeaders: map[string]string{}}
	config.Models["claude-model"] = map[string]string{"anthropic": "claude-sonnet-5"}
	config.Harnesses["claude-code"] = HarnessConfig{Type: "claude-code"}
	effort := "medium"
	step := WorkflowStep{ID: "run", Prompt: "foo.md", Harness: "claude-code", Provider: "anthropic", Model: "claude-model", ModelID: "claude-sonnet-5", ReasoningEffort: &effort, TimeoutMS: 2000}
	config.Workflow.Steps = []WorkflowStep{step}
	var output bytes.Buffer
	t.Setenv("CLAUDE_ACP_PATH", fakeACPAgent(t, "acp_reasoning_missing"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(t.TempDir(), "claude"))
	runner := NewRunner(config, "job-1", step, &output)
	runner.workspace = filepath.Join(t.TempDir(), "workspace")
	runner.proxyToken = "scoped-token"
	completion := runner.Execute(context.Background(), "hello")
	if completion.Status != "failed" || completion.Error == nil || completion.Error.Code != "acp_configuration_failed" {
		t.Fatalf("Claude reasoning configuration unexpectedly succeeded: %+v, logs: %s", completion, output.String())
	}
	if strings.Contains(output.String(), `"event_type":"session/prompt"`) || !strings.Contains(completion.Error.Message, "did not advertise a reasoning configuration option") {
		t.Fatalf("Claude missing effort option did not fail before prompting: %s", output.String())
	}
}

func TestClaudeReasoningEffortStillVerifiesAdvertisedACPOption(t *testing.T) {
	config := testConfig()
	config.Providers["anthropic"] = ProviderConfig{Protocol: "anthropic", BaseURL: "https://api.anthropic.com/v1", StaticHeaders: map[string]string{}}
	config.Models["claude-model"] = map[string]string{"anthropic": "claude-sonnet-5"}
	config.Harnesses["claude-code"] = HarnessConfig{Type: "claude-code"}
	effort := "high"
	step := WorkflowStep{ID: "run", Prompt: "foo.md", Harness: "claude-code", Provider: "anthropic", Model: "claude-model", ModelID: "claude-sonnet-5", ReasoningEffort: &effort, TimeoutMS: 2000}
	config.Workflow.Steps = []WorkflowStep{step}
	var output bytes.Buffer
	t.Setenv("CLAUDE_ACP_PATH", fakeACPAgent(t, "acp_reasoning_claude"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(t.TempDir(), "claude"))
	runner := NewRunner(config, "job-1", step, &output)
	runner.workspace = filepath.Join(t.TempDir(), "workspace")
	runner.proxyToken = "scoped-token"
	completion := runner.Execute(context.Background(), "hello")
	if completion.Status != "succeeded" {
		t.Fatalf("unexpected Claude completion: %+v, logs: %s", completion, output.String())
	}
	if !strings.Contains(output.String(), `"event_type":"session/set_config_option"`) || !strings.Contains(output.String(), `"configId":"effort"`) || !strings.Contains(output.String(), `"value":"high"`) {
		t.Fatalf("Claude ACP effort was not configured and verified: %s", output.String())
	}
}

func TestClaudeRejectsUnsupportedNativeReasoningEffort(t *testing.T) {
	for _, effort := range []string{"none", "minimal"} {
		t.Run(effort, func(t *testing.T) {
			model := "claude-sonnet-5"
			runner := &Runner{harness: ResolvedHarness{
				Type: "claude-code", Model: &model, ReasoningEffort: &effort,
				Provider: ProviderConfig{Protocol: "anthropic", BaseURL: "https://api.anthropic.com/v1"},
			}, proxyToken: "scoped-token", logger: NewEventLogger(&bytes.Buffer{}, "job", "claude-anthropic", 240000)}
			if _, err := (claudeCodeDriver{}).Command(runner); err == nil || !strings.Contains(err.Error(), `does not support reasoning effort "`+effort+`"`) {
				t.Fatalf("unsupported Claude effort was not rejected clearly: %v", err)
			}
		})
	}
}

func TestClaudeCustomModelOptionDetection(t *testing.T) {
	tests := map[string]bool{
		"claude-opus-4-7":          false,
		"claude-opus-4-8":          true,
		"claude-opus-4-8-20260901": true,
		"claude-sonnet-5":          true,
		"claude-opus-5":            true,
		"claude-haiku-5":           true,
		"claude-fable-5-1":         true,
		"claude-sonnet-4-6":        false,
		"claude-haiku-4-5":         false,
		"claude-3-5-sonnet":        false,
		"custom-claude-5":          false,
	}
	for model, expected := range tests {
		t.Run(model, func(t *testing.T) {
			if actual := claudeCodeNeedsCustomModelOption(model); actual != expected {
				t.Fatalf("claudeCodeNeedsCustomModelOption(%q) = %t, want %t", model, actual, expected)
			}
		})
	}
}

func TestClaudeModernModelsPreserveExactModelOption(t *testing.T) {
	for _, model := range []string{"claude-opus-4-8", "claude-sonnet-5", "claude-opus-5", "claude-haiku-5", "claude-fable-5-1"} {
		t.Run(model, func(t *testing.T) {
			effort := "xhigh"
			runner := &Runner{harness: ResolvedHarness{
				Type: "claude-code", Model: &model, ReasoningEffort: &effort,
				Provider: ProviderConfig{Protocol: "anthropic", BaseURL: "https://api.anthropic.com/v1"},
			}, config: testConfig(), proxyToken: "scoped-token", logger: NewEventLogger(&bytes.Buffer{}, "job", "claude-anthropic", 240000)}
			command, err := (claudeCodeDriver{}).Command(runner)
			if err != nil {
				t.Fatal(err)
			}
			for _, expected := range []string{
				"ANTHROPIC_MODEL=" + model,
				"ANTHROPIC_CUSTOM_MODEL_OPTION=" + model,
				"CLAUDE_CODE_EXECUTABLE=/usr/local/bin/claude",
				"CLAUDE_CODE_EFFORT_LEVEL=xhigh",
			} {
				if !containsEnvironment(command.Env, expected) {
					t.Fatalf("Claude command environment missing %q: %v", expected, command.Env)
				}
			}
			for _, removed := range []string{
				"ANTHROPIC_CUSTOM_MODEL_OPTION_SUPPORTED_CAPABILITIES=",
				"CLAUDE_CODE_ALLOW_MODEL_CAPABILITY_OVERRIDES=",
			} {
				if environmentHasPrefix(command.Env, removed) {
					t.Fatalf("Claude command environment retained obsolete override %q: %v", removed, command.Env)
				}
			}
		})
	}
}

func TestGrokFeatureOverlayDisablesAuxiliaryCallsAndFollowsQuestionOptIn(t *testing.T) {
	config := testConfig()
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	runner.proxyToken = "scoped-token"
	command, err := (grokDriver{}).Command(runner)
	environment := strings.Join(command.Env, "\n")
	if err != nil || !strings.Contains(environment, `"ask_user_question":false`) {
		t.Fatalf("Grok question feature was not disabled: %v, %v", command.Env, err)
	}
	for _, feature := range []string{`"session_recap":false`, `"title_refresh":false`} {
		if !strings.Contains(environment, feature) {
			t.Fatalf("Grok auxiliary feature was not disabled with %s: %v", feature, command.Env)
		}
	}
	config.Workflow.Steps[0].AllowUserInput = true
	runner = NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	runner.proxyToken = "scoped-token"
	command, err = (grokDriver{}).Command(runner)
	if err != nil || !strings.Contains(strings.Join(command.Env, "\n"), `"ask_user_question":true`) {
		t.Fatalf("Grok question feature was not enabled: %v, %v", command.Env, err)
	}
}

func TestPiReasoningEffortMapsNoneToOff(t *testing.T) {
	if got := reasoningEffortWireValue("pi", "none"); got != "off" {
		t.Fatalf("Pi reasoning value = %q, want off", got)
	}
	if got := reasoningEffortWireValue("codex", "none"); got != "none" {
		t.Fatalf("Codex reasoning value = %q, want none", got)
	}
}

func TestRunnerDrainsACPProtocolOnFastExit(t *testing.T) {
	for iteration := range 25 {
		var output bytes.Buffer
		completion := configuredRunner(t, testConfig(), "success", &output).Execute(context.Background(), "hello")
		if completion.Status != "succeeded" || completion.Message != "done" {
			t.Fatalf("iteration %d lost ACP output: %+v, logs: %s", iteration, completion, output.String())
		}
	}
}

func TestRunnerDrainsFinalACPStderr(t *testing.T) {
	for iteration := range 25 {
		var output bytes.Buffer
		completion := configuredRunner(t, testConfig(), "startup_stderr", &output).Execute(context.Background(), "hello")
		if completion.Error == nil || completion.Error.Code != "acp_initialize_failed" || !strings.Contains(completion.Error.Message, "final startup diagnostic") || completion.ExitCode != 7 {
			t.Fatalf("iteration %d lost final stderr: %+v, logs: %s", iteration, completion, output.String())
		}
	}
}

func TestRunnerNormalizesPromptUsageFromMetadata(t *testing.T) {
	var output bytes.Buffer
	completion := configuredRunner(t, testConfig(), "meta_usage", &output).Execute(context.Background(), "hello")
	if completion.Status != "succeeded" || completion.Usage == nil {
		t.Fatalf("unexpected completion: %+v", completion)
	}
	usage := completion.Usage
	if usage.TotalTokens == nil || *usage.TotalTokens != 110 || usage.InputTokens == nil || *usage.InputTokens != 70 || usage.OutputTokens == nil || *usage.OutputTokens != 10 {
		t.Fatalf("metadata token usage not normalized: %+v", usage)
	}
	if usage.TotalInputTokens == nil || *usage.TotalInputTokens != 100 {
		t.Fatalf("metadata total input usage not derived: %+v", usage)
	}
	if usage.CachedReadTokens == nil || *usage.CachedReadTokens != 20 || usage.CachedWriteTokens == nil || *usage.CachedWriteTokens != 10 || usage.ThoughtTokens == nil || *usage.ThoughtTokens != 4 {
		t.Fatalf("metadata token details not normalized: %+v", usage)
	}
}

func TestTotalInputUsageTreatsMissingCacheCountersAsZero(t *testing.T) {
	usage := &ACPUsage{InputTokens: intPointer(37)}
	setTotalInputTokens(usage)
	if usage.TotalInputTokens == nil || *usage.TotalInputTokens != 37 {
		t.Fatalf("unexpected total input usage: %+v", usage)
	}

	usage = &ACPUsage{CachedReadTokens: intPointer(20)}
	setTotalInputTokens(usage)
	if usage.TotalInputTokens != nil {
		t.Fatalf("derived total input without input tokens: %+v", usage)
	}
}

func TestRunnerMapsLimitedStopToPartial(t *testing.T) {
	var output bytes.Buffer
	completion := configuredRunner(t, testConfig(), "partial", &output).Execute(context.Background(), "hello")
	if completion.Status != "partial" || completion.StopReason == nil || *completion.StopReason != "max_tokens" {
		t.Fatalf("unexpected completion: %+v", completion)
	}
}

func TestSessionUpdateLevelsFailedToolCallsAsErrors(t *testing.T) {
	failed := acp.ToolCallStatusFailed
	completed := acp.ToolCallStatusCompleted
	tests := []struct {
		name   string
		update acp.SessionUpdate
		want   string
	}{
		{name: "message", update: acp.UpdateAgentMessageText("done"), want: "info"},
		{name: "failed tool start", update: acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{Status: failed}}, want: "error"},
		{name: "failed tool update", update: acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{Status: &failed}}, want: "error"},
		{name: "completed tool update", update: acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{Status: &completed}}, want: "info"},
		{name: "statusless tool update", update: acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{}}, want: "info"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sessionUpdateLevel(test.update, harnessSessionUpdateAssessment{}); got != test.want {
				t.Fatalf("sessionUpdateLevel() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRunnerTimesOutACPProcess(t *testing.T) {
	var output bytes.Buffer
	config := testConfig()
	config.WorkflowTimeoutMS = 500
	config.DefaultStepTimeoutMS = 500
	config.Workflow.Steps[0].TimeoutMS = 500
	config.ShutdownGraceMS = 20
	started := time.Now()
	completion := configuredRunner(t, config, "timeout", &output).Execute(context.Background(), "hello")
	if completion.Status != "timed_out" || completion.Error == nil || completion.Error.Code != "timeout" {
		t.Fatalf("unexpected completion: %+v", completion)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("timeout took too long: %s", time.Since(started))
	}
}

func TestRunnerRejectsOversizedFinalMessage(t *testing.T) {
	var output bytes.Buffer
	config := testConfig()
	config.MaxResultBytes = 1024
	completion := configuredRunner(t, config, "oversized", &output).Execute(context.Background(), "hello")
	if completion.Error == nil || completion.Error.Code != "output_too_large" {
		t.Fatalf("unexpected completion: %+v", completion)
	}
}

func TestWorkflowRunsStepsSeriallyAndAggregatesTokens(t *testing.T) {
	config := testConfig()
	config.Workflow.Steps = []WorkflowStep{
		{ID: "prepare", Command: []string{"sh", "-c", "printf ready > marker.txt"}, TimeoutMS: 2000},
		config.Workflow.Steps[0],
		{ID: "review", Prompt: "review.md", Harness: "grok", Provider: "xai", Model: "grok-4.6", ModelID: "grok-4.6", TimeoutMS: 2000},
		{ID: "verify", Command: []string{"sh", "-c", "test -f marker.txt && printf verified"}, TimeoutMS: 2000},
	}
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"foo.md", "review.md"} {
		if err := os.WriteFile(filepath.Join(workspace, "prompts", name), []byte("hello"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("RUNNER_WORKSPACE", workspace)
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
	t.Setenv("GROK_PATH", fakeACPAgent(t, "success"))
	result := executeWorkflow(context.Background(), config, RunRequest{JobID: "job-1", DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}, &bytes.Buffer{}, nil, func(WorkflowCompletion) {})
	if result.Status != "succeeded" || len(result.Steps) != 4 || result.Steps[3].Stdout != "verified" {
		t.Fatalf("unexpected workflow result: %+v", result)
	}
	if result.Usage == nil || result.Usage.TotalTokens == nil || *result.Usage.TotalTokens != 220 {
		t.Fatalf("usage was not aggregated: %+v", result.Usage)
	}
	if result.Usage.TotalInputTokens == nil || *result.Usage.TotalInputTokens != 200 {
		t.Fatalf("total input usage was not aggregated: %+v", result.Usage)
	}
}

func TestWorkflowOnlyAttachesMetricsToRequiredSteps(t *testing.T) {
	for _, test := range []struct {
		name              string
		requiredMetrics   []RequiredMetric
		wantMetricServers int
		wantErrorCode     string
		resume            bool
	}{
		{name: "not required"},
		{name: "not required when resumed", resume: true},
		{
			name:              "required",
			requiredMetrics:   []RequiredMetric{{Namespace: "haiku", Key: "num_lines", Description: "number of lines"}},
			wantMetricServers: 1,
			wantErrorCode:     "required_metrics_missing",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig()
			config.Workflow.Steps[0].RequiredMetrics = test.requiredMetrics
			workspace := filepath.Join(t.TempDir(), "workspace")
			if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(workspace, "prompts", "foo.md"), []byte("hello"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("RUNNER_WORKSPACE", workspace)
			t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
			t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
			t.Setenv("GROK_PATH", fakeACPAgent(t, "success"))
			var output bytes.Buffer
			request := RunRequest{JobID: "job-1", DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}
			sessionEvent := "session/new"
			if test.resume {
				sessionEvent = "session/resume"
				request.Resume = &ResumeRequest{
					StepIndex: 0, SessionID: "session-1",
					Steps: []StepCompletion{{ID: config.Workflow.Steps[0].ID, Status: "running", StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}},
				}
			}
			result := executeWorkflow(context.Background(), config, request, &output, nil, func(WorkflowCompletion) {})
			if test.wantErrorCode == "" {
				if result.Status != "succeeded" {
					t.Fatalf("non-metric step failed: %+v, logs: %s", result, output.String())
				}
			} else if result.Error == nil || result.Error.Code != test.wantErrorCode {
				t.Fatalf("unexpected metric-step result: %+v, logs: %s", result, output.String())
			}

			foundRequest := false
			scanner := bufio.NewScanner(strings.NewReader(output.String()))
			for scanner.Scan() {
				var event struct {
					EventType string `json:"event_type"`
					Event     struct {
						Phase  string `json:"phase"`
						Params struct {
							MCPServers []struct {
								Name string `json:"name"`
							} `json:"mcpServers"`
						} `json:"params"`
					} `json:"event"`
				}
				if json.Unmarshal(scanner.Bytes(), &event) != nil || event.EventType != sessionEvent || event.Event.Phase != "request" {
					continue
				}
				foundRequest = true
				if len(event.Event.Params.MCPServers) != test.wantMetricServers {
					t.Fatalf("%s metric servers = %#v, want %d: %s", sessionEvent, event.Event.Params.MCPServers, test.wantMetricServers, output.String())
				}
				if test.wantMetricServers == 1 && event.Event.Params.MCPServers[0].Name != "metrics" {
					t.Fatalf("unexpected metric server: %#v", event.Event.Params.MCPServers[0])
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
			if !foundRequest {
				t.Fatalf("%s request was not logged: %s", sessionEvent, output.String())
			}
		})
	}
}

func TestWorkflowSuspendsBetweenStepsWhenTokenBudgetIsExhausted(t *testing.T) {
	config := testConfig()
	config.Workflow.TokenBudget = &TokenBudget{Limit: 100, Period: "day"}
	config.Workflow.Steps = []WorkflowStep{
		config.Workflow.Steps[0],
		{ID: "review", Prompt: "review.md", Harness: "grok", Provider: "xai", Model: "grok-4.6", ModelID: "grok-4.6", TimeoutMS: 2000},
	}
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"foo.md", "review.md"} {
		if err := os.WriteFile(filepath.Join(workspace, "prompts", name), []byte("hello"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("RUNNER_WORKSPACE", workspace)
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
	t.Setenv("GROK_PATH", fakeACPAgent(t, "success"))
	resetAt := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	charges := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		charges++
		var charge budgetChargeRequest
		if request.Header.Get("authorization") != "Bearer callback-token" || json.NewDecoder(request.Body).Decode(&charge) != nil {
			t.Error("invalid budget charge callback")
		}
		if charge.StepID != "run" || charge.StepIndex != 0 || charge.Tokens == nil || *charge.Tokens != 110 {
			t.Errorf("unexpected charge: %+v", charge)
		}
		_ = json.NewEncoder(response).Encode(budgetChargeResponse{Accepted: true, Action: "suspend", ResetAt: resetAt})
	}))
	defer server.Close()
	result := executeWorkflow(context.Background(), config, RunRequest{
		JobID: "job-1", CallbackToken: "callback-token", BudgetURL: server.URL,
		DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
	}, &bytes.Buffer{}, nil, func(WorkflowCompletion) {})
	if result.Status != "budget_suspended" || result.BudgetResetAt != resetAt || charges != 1 {
		t.Fatalf("unexpected budget suspension: %+v; charges=%d", result, charges)
	}
	if result.Steps[0].Status != "succeeded" || result.Steps[1].Status != "pending" || result.Usage == nil || result.Usage.TotalTokens == nil || *result.Usage.TotalTokens != 110 {
		t.Fatalf("completed output was not preserved at the step boundary: %+v", result)
	}
}

func TestBudgetedWorkflowFailsClosedWhenUsageIsMissing(t *testing.T) {
	config := testConfig()
	config.Workflow.TokenBudget = &TokenBudget{Limit: 100, Period: "day"}
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "prompts", "foo.md"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUNNER_WORKSPACE", workspace)
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
	t.Setenv("GROK_PATH", fakeACPAgent(t, "missing_token_usage"))
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var charge budgetChargeRequest
		if json.NewDecoder(request.Body).Decode(&charge) != nil || charge.Tokens != nil {
			t.Errorf("expected an indeterminate charge: %+v", charge)
		}
		_ = json.NewEncoder(response).Encode(budgetChargeResponse{Accepted: true, Action: "suspend", ResetAt: time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)})
	}))
	defer server.Close()
	result := executeWorkflow(context.Background(), config, RunRequest{
		JobID: "job-1", CallbackToken: "callback-token", BudgetURL: server.URL,
		DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
	}, &bytes.Buffer{}, nil, func(WorkflowCompletion) {})
	if result.Status != "failed" || result.Error == nil || result.Error.Code != "token_usage_unavailable" || result.Steps[0].Message != "done" {
		t.Fatalf("missing usage did not fail closed while retaining output: %+v", result)
	}
}

func TestWorkflowExportsStableRunMetadataAndMonotonicSequences(t *testing.T) {
	config := testConfig()
	config.Workflow.Steps = []WorkflowStep{
		{ID: "prepare", Command: fakeCommand(t, "success"), TimeoutMS: 2000},
		{ID: "draft", Prompt: "foo.md", Harness: "grok", Provider: "xai", Model: "grok-4.6", ModelID: "grok-4.6", TimeoutMS: 2000},
		{ID: "review", Prompt: "review.md", Harness: "grok", Provider: "xai", Model: "grok-4.6", ModelID: "grok-4.6", TimeoutMS: 2000},
	}
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"foo.md", "review.md"} {
		if err := os.WriteFile(filepath.Join(workspace, "prompts", name), []byte("hello"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("RUNNER_WORKSPACE", workspace)
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
	t.Setenv("GROK_PATH", fakeACPAgent(t, "tool_failure"))

	type callbackBatch struct {
		Run struct {
			Harness    string `json:"harness"`
			TraceID    string `json:"trace_id"`
			RootSpanID string `json:"root_span_id"`
		} `json:"run"`
		Events []flowEvent `json:"events"`
		Spans  []flowSpan  `json:"spans"`
	}
	var batchesMu sync.Mutex
	batches := []callbackBatch{}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("authorization") != "Bearer callback-token" {
			t.Error("missing callback authorization")
		}
		var batch callbackBatch
		if err := json.NewDecoder(request.Body).Decode(&batch); err != nil {
			t.Error(err)
		}
		batchesMu.Lock()
		batches = append(batches, batch)
		batchesMu.Unlock()
		response.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	result := executeWorkflow(context.Background(), config, RunRequest{
		JobID: "job-1", CallbackToken: "callback-token", CallbackURL: server.URL,
		RunHarness: "workflow", RunCreatedAt: "2026-08-31T00:00:00Z", RunStartedAt: "2026-08-31T00:00:01Z",
		TraceID: testTraceID, RunSpanID: testRootSpanID,
		DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
	}, &bytes.Buffer{}, nil, func(WorkflowCompletion) {})
	if result.Status != "succeeded" {
		t.Fatalf("unexpected workflow result: %+v", result)
	}

	batchesMu.Lock()
	defer batchesMu.Unlock()
	lastSequence := lastWorkerStartupEventSequence
	stepEvents := map[string]int{"prepare": 0, "draft": 0, "review": 0}
	commandEvents := map[string]int{}
	commandOutput := map[string]string{}
	commandArgvFound := false
	promptRequestFound := false
	failedToolEventFound := false
	pendingToolEventFound := false
	spanNames := map[string]int{}
	for _, batch := range batches {
		if batch.Run.Harness != "workflow" || batch.Run.TraceID != testTraceID || batch.Run.RootSpanID != testRootSpanID {
			t.Fatalf("callback changed run harness: %#v", batch.Run)
		}
		for _, span := range batch.Spans {
			spanNames[span.Name]++
			if span.TraceID != testTraceID || span.SpanID == "" || span.ParentSpanID == nil {
				t.Fatalf("span lost trace hierarchy: %+v", span)
			}
		}
		for _, event := range batch.Events {
			if event.ProtocolVersion != 4 || event.TraceID != testTraceID || event.SpanID == "" {
				t.Fatalf("event did not use correlated protocol v4 telemetry: %+v", event)
			}
			if event.Sequence <= lastSequence {
				t.Fatalf("event sequence did not increase across steps: %d after %d", event.Sequence, lastSequence)
			}
			lastSequence = event.Sequence
			for stepID := range stepEvents {
				if event.StepID != nil && *event.StepID == stepID {
					if event.StepIndex == nil || event.StepKind == nil {
						t.Fatalf("event has partial step coordinates: %+v", event)
					}
					stepEvents[stepID]++
					if stepID == "prepare" && event.Source == "command" {
						commandEvents[event.Type]++
						if event.Type == "command.started" {
							commandArgvFound = strings.Contains(string(event.Data), "-test.run=^TestCommandHelperProcess$")
						}
						if event.Type == "command.stdout" || event.Type == "command.stderr" {
							var payload struct {
								Message string `json:"message"`
							}
							if err := json.Unmarshal(event.Data, &payload); err != nil {
								t.Fatal(err)
							}
							commandOutput[event.Type] = payload.Message
						}
					}
					if stepID == "draft" && event.Type == "session/prompt" {
						var payload map[string]any
						if err := json.Unmarshal(event.Data, &payload); err != nil {
							t.Fatal(err)
						}
						promptRequestFound = promptRequestFound || (payload["direction"] == "client_to_agent" && payload["phase"] == "request" && strings.Contains(string(event.Data), "hello"))
					}
					if stepID == "draft" && event.Type == "session/update" {
						var payload struct {
							Params struct {
								Update struct {
									SessionUpdate string `json:"session_update"`
									Status        string `json:"status"`
								} `json:"update"`
							} `json:"params"`
						}
						if err := json.Unmarshal(event.Data, &payload); err != nil {
							t.Fatal(err)
						}
						if payload.Params.Update.SessionUpdate == "tool_call" && payload.Params.Update.Status == "pending" {
							pendingToolEventFound = event.Level == "info"
						}
						if payload.Params.Update.SessionUpdate == "tool_call_update" && payload.Params.Update.Status == "failed" {
							failedToolEventFound = event.Level == "error"
						}
					}
				}
			}
		}
	}
	if stepEvents["prepare"] == 0 || stepEvents["draft"] == 0 || stepEvents["review"] == 0 {
		t.Fatalf("missing exported events from workflow steps: %+v", stepEvents)
	}
	for _, eventType := range []string{"command.started", "command.stdout", "command.stderr", "command.finished"} {
		if commandEvents[eventType] != 1 {
			t.Fatalf("expected one exported %s event, got %+v", eventType, commandEvents)
		}
	}
	if commandOutput["command.stdout"] != "command output\n" || commandOutput["command.stderr"] != "command warning\n" {
		t.Fatalf("exported command output was incomplete: %+v", commandOutput)
	}
	if !commandArgvFound {
		t.Fatal("full command argv was not exported")
	}
	if !promptRequestFound {
		t.Fatal("full outbound prompt request was not exported")
	}
	if !pendingToolEventFound || !failedToolEventFound {
		t.Fatalf("tool call levels were not preserved in callback batches: pending=%t failed=%t", pendingToolEventFound, failedToolEventFound)
	}
	if spanNames["workflow.step"] != 3 || spanNames["workflow.command"] != 1 || spanNames["acp.rpc"] == 0 || spanNames["agent.processing"] == 0 || spanNames["gen_ai.tool.call"] == 0 {
		t.Fatalf("missing completed workflow spans: %+v", spanNames)
	}
}

func TestCommandCapturesOutputAndRedactsScopedAliases(t *testing.T) {
	config := testConfig()
	step := WorkflowStep{ID: "check", Command: fakeCommand(t, "environment"), TimeoutMS: 2000}
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("RUNNER_WORKSPACE", t.TempDir())
	t.Setenv("GH_TOKEN", "real-github-token")
	t.Setenv("LINEAR_API_KEY", "real-linear-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "real-aws-secret")
	var logs bytes.Buffer
	runner := newCommandRunner(config, "job-1", step, &logs)
	completion := runner.executeCommand(context.Background(), step, nil)
	if completion.Status != "succeeded" || completion.Stdout != "[REDACTED]|[REDACTED]|" || completion.Stderr != "" {
		t.Fatalf("unexpected command completion: %+v", completion)
	}
	if strings.Contains(logs.String(), "scoped-token") || strings.Contains(logs.String(), "real-github-token") {
		t.Fatalf("credential leaked into command logs: %s", logs.String())
	}
}

func TestCommandCapturesSeparateStreamsAndStableJSON(t *testing.T) {
	config := testConfig()
	step := WorkflowStep{ID: "check", Command: fakeCommand(t, "success"), TimeoutMS: 2000}
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("RUNNER_WORKSPACE", t.TempDir())
	completion := newCommandRunner(config, "job-1", step, &bytes.Buffer{}).executeCommand(context.Background(), step, nil)
	result := stepResult(step, completion, time.Now(), time.Now())
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, expected := range []string{`"command"`, `"stdout":"command output\n"`, `"stderr":"command warning\n"`} {
		if !strings.Contains(text, expected) {
			t.Fatalf("command result missing %s: %s", expected, text)
		}
	}
	if strings.Contains(text, `"prompt"`) || strings.Contains(text, `"session_id"`) || strings.Contains(text, `"reasoning_effort"`) {
		t.Fatalf("command result exposed agent fields: %s", text)
	}
}

func TestAgentResultAndStartEventExposeReasoningEffort(t *testing.T) {
	config := testConfig()
	effort := "medium"
	step := config.Workflow.Steps[0]
	step.ReasoningEffort = &effort
	var output bytes.Buffer
	runner := NewRunner(config, "job-1", step, &output)
	startedAt := time.Date(2026, time.September, 18, 12, 34, 56, 789123456, time.UTC)
	runner.emitStepStarted(step, startedAt)
	result := stepResult(step, Completion{Status: "succeeded"}, startedAt, time.Now())
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for label, value := range map[string]string{"result": string(encoded), "event": output.String()} {
		if !strings.Contains(value, `"reasoning_effort":"medium"`) {
			t.Fatalf("%s omitted reasoning effort: %s", label, value)
		}
		if !strings.Contains(value, `"started_at":"2026-09-18T12:34:56.789123456Z"`) {
			t.Fatalf("%s omitted the logical step start time: %s", label, value)
		}
	}
}

func TestCommandDrainsFastOutputBeforeWaitReturns(t *testing.T) {
	config := testConfig()
	t.Setenv("RUNNER_WORKSPACE", t.TempDir())
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	for iteration := range 50 {
		step := WorkflowStep{ID: "check", Command: fakeCommand(t, "fast_output"), TimeoutMS: 2000}
		completion := newCommandRunner(config, "job-1", step, &bytes.Buffer{}).executeCommand(context.Background(), step, nil)
		if completion.Status != "succeeded" || completion.Stdout != "verified" || completion.Stderr != "final warning" {
			t.Fatalf("iteration %d lost command output: %+v, error: %+v", iteration, completion, completion.Error)
		}
	}
}

func TestCommandFailureStopsWorkflow(t *testing.T) {
	config := testConfig()
	config.Workflow.Steps = []WorkflowStep{
		{ID: "check", Command: fakeCommand(t, "failure"), TimeoutMS: 2000},
		config.Workflow.Steps[0],
	}
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "prompts", "foo.md"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUNNER_WORKSPACE", workspace)
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	result := executeWorkflow(context.Background(), config, RunRequest{JobID: "job-1", DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}, &bytes.Buffer{}, nil, func(WorkflowCompletion) {})
	if result.Status != "failed" || result.Error == nil || result.Error.Code != "command_failed" || result.Steps[0].ExitCode != 7 || result.Steps[1].Status != "pending" {
		t.Fatalf("unexpected workflow result: %+v", result)
	}
}

func TestWorkflowTimeoutAppliesAcrossSteps(t *testing.T) {
	config := testConfig()
	config.WorkflowTimeoutMS = 500
	config.DefaultStepTimeoutMS = 500
	config.Workflow.Steps = []WorkflowStep{
		{ID: "prepare", Command: []string{"sh", "-c", "sleep 0.25"}, TimeoutMS: 500},
		{ID: "wait", Command: fakeCommand(t, "timeout"), TimeoutMS: 500},
		testConfig().Workflow.Steps[0],
	}
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUNNER_WORKSPACE", workspace)
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	started := time.Now()
	result := executeWorkflow(context.Background(), config, RunRequest{JobID: "job-1", DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}, &bytes.Buffer{}, nil, func(WorkflowCompletion) {})
	if result.Status != "timed_out" || result.Error == nil || result.Error.Code != "timeout" || result.Steps[0].Status != "succeeded" || result.Steps[1].Status != "timed_out" || result.Steps[2].Status != "pending" {
		t.Fatalf("unexpected aggregate timeout result: %+v", result)
	}
	if elapsed := time.Since(started); elapsed > 900*time.Millisecond {
		t.Fatalf("aggregate timeout took too long: %s", elapsed)
	}
}

func TestWorkflowHonorsLLMStepTimeoutOverride(t *testing.T) {
	config := testConfig()
	config.WorkflowTimeoutMS = 2000
	config.DefaultStepTimeoutMS = 2000
	config.Workflow.Steps[0].TimeoutMS = 100
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "prompts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "prompts", "foo.md"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUNNER_WORKSPACE", workspace)
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
	t.Setenv("GROK_PATH", fakeACPAgent(t, "timeout"))
	started := time.Now()
	result := executeWorkflow(context.Background(), config, RunRequest{JobID: "job-1", DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}, &bytes.Buffer{}, nil, func(WorkflowCompletion) {})
	if result.Status != "timed_out" || result.Error == nil || result.Error.Code != "timeout" {
		t.Fatalf("unexpected step timeout result: %+v", result)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("step timeout override took too long: %s", elapsed)
	}
}

func TestCommandTimeoutAndOutputLimit(t *testing.T) {
	config := testConfig()
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("RUNNER_WORKSPACE", t.TempDir())
	step := WorkflowStep{ID: "check", Command: fakeCommand(t, "timeout"), TimeoutMS: 100}
	runner := newCommandRunner(config, "job-1", step, &bytes.Buffer{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	completion := runner.executeCommand(ctx, step, nil)
	if completion.Status != "timed_out" || completion.Error == nil || completion.Error.Code != "timeout" {
		t.Fatalf("unexpected timeout: %+v", completion)
	}

	config.MaxResultBytes = 1024
	step.Command = fakeCommand(t, "oversized")
	runner = newCommandRunner(config, "job-1", step, &bytes.Buffer{})
	completion = runner.executeCommand(context.Background(), step, nil)
	if completion.Status != "failed" || completion.Error == nil || completion.Error.Code != "output_too_large" || len(completion.Stdout) != 1024 {
		t.Fatalf("unexpected oversized output result: %+v", completion)
	}

	step.Command = fakeCommand(t, "endless_output")
	runner = newCommandRunner(config, "job-1", step, &bytes.Buffer{})
	started := time.Now()
	completion = runner.executeCommand(context.Background(), step, nil)
	if completion.Error == nil || completion.Error.Code != "output_too_large" || time.Since(started) > time.Second {
		t.Fatalf("unbounded output was not stopped promptly: %+v after %s", completion, time.Since(started))
	}
}

func TestCommandCancellationAndStartFailure(t *testing.T) {
	config := testConfig()
	t.Setenv("RUNNER_EGRESS_TOKEN", "scoped-token")
	t.Setenv("RUNNER_WORKSPACE", t.TempDir())
	step := WorkflowStep{ID: "check", Command: fakeCommand(t, "timeout"), TimeoutMS: 2000}
	runner := newCommandRunner(config, "job-1", step, &bytes.Buffer{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	completion := runner.executeCommand(ctx, step, nil)
	if completion.Status != "cancelled" || completion.Error == nil || completion.Error.Code != "cancelled" {
		t.Fatalf("unexpected cancellation: %+v", completion)
	}

	step.Command = []string{filepath.Join(t.TempDir(), "missing-command")}
	runner = newCommandRunner(config, "job-1", step, &bytes.Buffer{})
	completion = runner.executeCommand(context.Background(), step, nil)
	if completion.Status != "failed" || completion.Error == nil || completion.Error.Code != "command_start_failed" {
		t.Fatalf("unexpected start failure: %+v", completion)
	}
}

func TestHarnessesAlwaysBypassPermissions(t *testing.T) {
	config := testConfig()
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	runner.proxyToken = "scoped-token"
	grok, _ := newHarnessDriver("grok")
	command, err := grok.Command(runner)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(command.Args, " "), "--always-approve") {
		t.Fatalf("Grok command does not bypass permissions: %v", command.Args)
	}

	config.Providers["openai"] = ProviderConfig{Protocol: "openai-responses", BaseURL: "https://api.openai.com/v1", StaticHeaders: map[string]string{}}
	config.Models["codex-model"] = map[string]string{"openai": "gpt-5.6-terra"}
	config.Harnesses["codex"] = HarnessConfig{Type: "codex"}
	codexStep := WorkflowStep{ID: "codex", Prompt: "foo.md", Harness: "codex", Provider: "openai", Model: "codex-model", ModelID: "gpt-5.6-terra", TimeoutMS: 1000}
	runner = NewRunner(config, "job-1", codexStep, &bytes.Buffer{})
	runner.proxyToken = "scoped-token"
	codex, _ := newHarnessDriver("codex")
	command, err = codex.Command(runner)
	if err != nil {
		t.Fatal(err)
	}
	if !containsEnvironment(command.Env, "INITIAL_AGENT_MODE=agent-full-access") {
		t.Fatalf("Codex command does not bypass permissions: %v", command.Env)
	}
}

func TestChildEnvironmentExposesOnlyScopedGenericCredentials(t *testing.T) {
	t.Setenv("GH_TOKEN", "real-github-token")
	t.Setenv("LINEAR_API_KEY", "real-linear-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "real-aws-secret")
	t.Setenv("THIRD_PARTY_URL", "https://service.example")
	t.Setenv("UNDECLARED_VALUE", "must-not-leak")
	config := testConfig()
	config.RuntimeEnv = append(config.RuntimeEnv, "THIRD_PARTY_URL")
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	runner.proxyToken = "scoped-token"
	driver, _ := newHarnessDriver("grok")
	command, err := driver.Command(runner)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"GH_TOKEN=scoped-token", "LINEAR_API_KEY=scoped-token", "XAI_API_KEY=scoped-token"} {
		if !containsEnvironment(command.Env, expected) {
			t.Fatalf("missing scoped environment %q", expected)
		}
	}
	if containsEnvironment(command.Env, "GH_TOKEN=real-github-token") || containsEnvironment(command.Env, "LINEAR_API_KEY=real-linear-key") || containsEnvironment(command.Env, "AWS_SECRET_ACCESS_KEY=real-aws-secret") {
		t.Fatalf("real credentials leaked into child environment: %v", command.Env)
	}
	if !containsEnvironment(command.Env, "THIRD_PARTY_URL=https://service.example") || containsEnvironment(command.Env, "UNDECLARED_VALUE=must-not-leak") {
		t.Fatalf("runtime environment allowlist was not enforced: %v", command.Env)
	}
	configuration := strings.Join(command.Env, "\n")
	if !strings.Contains(configuration, `"ignore_default_excludes":true`) || strings.Contains(configuration, `"*TOKEN*"`) {
		t.Fatalf("Grok shell policy does not expose configured scoped credentials: %s", configuration)
	}
}

func TestChildEnvironmentExposesMemoryPathOnlyWhenEnabled(t *testing.T) {
	config := testConfig()
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	runner.workspace = "/workspace"
	if containsEnvironment(runner.childEnvironment(nil), "AGENT_MEMORY_DB=/workspace/memory.sqlite3") {
		t.Fatal("memory path was exposed while workflow memory was disabled")
	}
	config.Workflow.MemoryEnabled = true
	runner = NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	runner.workspace = "/workspace"
	if !containsEnvironment(runner.childEnvironment(nil), "AGENT_MEMORY_DB=/workspace/memory.sqlite3") {
		t.Fatal("memory path was not exposed while workflow memory was enabled")
	}
}

func TestChildEnvironmentExposesMetricEndpointOnlyWhenAttached(t *testing.T) {
	config := testConfig()
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	if environmentHasPrefix(runner.childEnvironment(nil), metricEndpointEnvironment+"=") {
		t.Fatal("metric endpoint was exposed without an attached metric runtime")
	}
	runner.metrics = &metricRuntime{URL: "http://127.0.0.1:1234/metrics"}
	if !containsEnvironment(runner.childEnvironment(nil), metricEndpointEnvironment+"=http://127.0.0.1:1234/metrics") {
		t.Fatal("attached metric endpoint was not exposed")
	}
}

func TestPermissionBrokerSelectsAllowAlways(t *testing.T) {
	client := &runnerACPClient{
		runner: func() *Runner {
			config := testConfig()
			return NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
		}(),
		state: &acpState{}, broker: nonInteractiveBroker{},
	}
	response, err := client.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		SessionId: "session-1",
		Options: []acp.PermissionOption{
			{Kind: acp.PermissionOptionKindRejectOnce, OptionId: "reject", Name: "Reject"},
			{Kind: acp.PermissionOptionKindAllowAlways, OptionId: "allow", Name: "Always allow"},
		},
	})
	if err != nil || response.Outcome.Selected == nil || response.Outcome.Selected.OptionId != "allow" {
		t.Fatalf("expected allow-always response, got %+v, %v", response, err)
	}
}

func TestGrokInteractionExtensionsAcceptNamespacedAliases(t *testing.T) {
	client := &runnerACPClient{
		runner: func() *Runner {
			config := testConfig()
			return NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
		}(),
		state: &acpState{}, broker: nonInteractiveBroker{},
	}
	for _, method := range []string{"x.ai/exit_plan_mode", "_x.ai/exit_plan_mode"} {
		response, err := client.HandleExtensionMethod(context.Background(), method, json.RawMessage(`{}`))
		if err != nil || response.(map[string]any)["outcome"] != "approved" {
			t.Fatalf("expected %s to be approved, got %#v, %v", method, response, err)
		}
	}
	for _, method := range []string{"x.ai/ask_user_question", "_x.ai/ask_user_question"} {
		response, err := client.HandleExtensionMethod(context.Background(), method, json.RawMessage(`{"mode":"default","questions":[{"question":"Continue?","options":[{"label":"Yes"},{"label":"No"}]}]}`))
		if err != nil || response.(map[string]any)["outcome"] != "cancelled" {
			t.Fatalf("expected %s to be cancelled, got %#v, %v", method, response, err)
		}
	}
	for _, method := range []string{"x.ai/mcp/elicit", "_x.ai/mcp/elicit"} {
		response, err := client.HandleExtensionMethod(context.Background(), method, json.RawMessage(`{}`))
		if err != nil || response.(map[string]any)["action"] != "cancel" {
			t.Fatalf("expected %s to be cancelled, got %#v, %v", method, response, err)
		}
	}
}

func TestUnknownExtensionMethodRemainsRejected(t *testing.T) {
	client := &runnerACPClient{
		runner: func() *Runner {
			config := testConfig()
			return NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
		}(),
		state: &acpState{}, broker: nonInteractiveBroker{},
	}
	response, err := client.HandleExtensionMethod(context.Background(), "_x.ai/unknown", json.RawMessage(`{}`))
	requestError, ok := err.(*acp.RequestError)
	if response != nil || !ok || requestError.Code != -32601 {
		t.Fatalf("expected method-not-found, got %#v, %v", response, err)
	}
}

func TestCodexConfigurationUsesACPAndFullAccess(t *testing.T) {
	model := "gpt-5.6-terra"
	config := renderCodexConfig(ResolvedHarness{
		Type: "codex", ProviderName: "openai", Model: &model,
		Provider: ProviderConfig{Protocol: "openai-responses", BaseURL: "https://api.openai.com/v1"},
	}, true)
	for _, expected := range []string{`model = "gpt-5.6-terra"`, `approval_policy = "never"`, `sandbox_mode = "danger-full-access"`, `env_key = "CODEX_API_KEY"`, `default_mode_request_user_input = true`} {
		if !strings.Contains(config, expected) {
			t.Fatalf("Codex config missing %q:\n%s", expected, config)
		}
	}
	if disabled := renderCodexConfig(ResolvedHarness{Provider: ProviderConfig{BaseURL: "https://api.openai.com/v1"}}, false); !strings.Contains(disabled, `default_mode_request_user_input = false`) {
		t.Fatalf("Codex user input feature was not disabled:\n%s", disabled)
	}
}

func TestOpenCodeConfigurationSelectsProtocolSDK(t *testing.T) {
	tests := []struct {
		protocol string
		model    string
		expected []string
	}{
		{"openai-responses", "gpt-5.6-terra", []string{`"openai"`, `"@ai-sdk/openai"`, `"openai/selected"`}},
		{"anthropic", "claude-sonnet-4-6", []string{`"anthropic"`, `"@ai-sdk/anthropic"`, `"authToken"`, `"baseURL":"https://example.com/v1"`}},
		{"openai-compatible", "@cf/zai-org/glm-5.3", []string{`"runner"`, `"@ai-sdk/openai-compatible"`, `"cf-aig-gateway-id"`}},
	}
	for _, test := range tests {
		t.Run(test.protocol, func(t *testing.T) {
			config, err := renderOpenCodeConfig(ResolvedHarness{
				Type: "opencode", ProviderName: "provider", Model: &test.model,
				Provider: ProviderConfig{Protocol: test.protocol, BaseURL: "https://example.com/v1", StaticHeaders: map[string]string{"cf-aig-gateway-id": "default"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, expected := range append(test.expected, `"permission":{"*":"allow","question":"deny"}`, `"reasoning":true`) {
				if !strings.Contains(config, expected) {
					t.Fatalf("OpenCode config missing %q: %s", expected, config)
				}
			}
		})
	}
}

func TestPiConfigurationSelectsProtocolTransport(t *testing.T) {
	tests := []struct {
		protocol    string
		baseURL     string
		expectedAPI string
		expectedURL string
	}{
		{"xai", "https://api.x.ai/v1", "openai-responses", "https://api.x.ai/v1"},
		{"openai-responses", "https://example.openai.azure.com/openai/v1/", "openai-responses", "https://example.openai.azure.com/openai/v1"},
		{"anthropic", "https://api.anthropic.com/v1", "anthropic-messages", "https://api.anthropic.com"},
		{"openai-compatible", "https://api.cloudflare.com/client/v4/accounts/example/ai/v1", "openai-completions", "https://api.cloudflare.com/client/v4/accounts/example/ai/v1"},
	}
	model := "provider-model"
	for _, test := range tests {
		t.Run(test.protocol, func(t *testing.T) {
			models, settings, err := renderPiConfig(ResolvedHarness{
				Type: "pi", ProviderName: "provider", Model: &model,
				Provider: ProviderConfig{
					Protocol: test.protocol, BaseURL: test.baseURL,
					StaticHeaders: map[string]string{"cf-aig-gateway-id": "default"},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			configuration := string(models)
			for _, expected := range []string{
				`"runner"`, `"api":"` + test.expectedAPI + `"`, `"apiKey":"$RUNNER_PROVIDER_TOKEN"`,
				`"authHeader":true`, `"cf-aig-gateway-id":"default"`, `"id":"provider-model"`,
				`"baseUrl":"` + test.expectedURL + `"`, `"reasoning":true`,
			} {
				if !strings.Contains(configuration, expected) {
					t.Fatalf("Pi model config missing %q: %s", expected, configuration)
				}
			}
			if !strings.Contains(string(settings), `"defaultProvider":"runner"`) || !strings.Contains(string(settings), `"defaultModel":"provider-model"`) {
				t.Fatalf("unexpected Pi settings: %s", settings)
			}
		})
	}
}

func TestPiDriverWritesIsolatedConfigAndScopedEnvironment(t *testing.T) {
	model := "claude-sonnet-4-6"
	home := filepath.Join(t.TempDir(), "pi")
	t.Setenv("PI_CODING_AGENT_DIR", home)
	t.Setenv("RUNNER_PROVIDER_TOKEN", "real-token-must-be-filtered")
	runner := &Runner{
		harness: ResolvedHarness{
			Type: "pi", ProviderName: "anthropic", Model: &model,
			Provider: ProviderConfig{Protocol: "anthropic", BaseURL: "https://api.anthropic.com/v1"},
		},
		proxyToken: "scoped-token", caBundle: "/tmp/container-proxy-ca.pem",
		logger: NewEventLogger(&bytes.Buffer{}, "job", "pi-anthropic", 240000),
	}
	driver := piDriver{}
	if err := driver.Prepare(runner); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"models.json", "settings.json"} {
		info, err := os.Stat(filepath.Join(home, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("Pi %s permissions are %o", name, info.Mode().Perm())
		}
	}
	command, err := driver.Command(runner)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"RUNNER_PROVIDER_TOKEN=scoped-token", "PI_CODING_AGENT_DIR=" + home,
		"PI_OFFLINE=1", "PI_SKIP_VERSION_CHECK=1", "PI_TELEMETRY=0", "NODE_USE_ENV_PROXY=1", "NODE_EXTRA_CA_CERTS=/tmp/container-proxy-ca.pem",
	} {
		if !containsEnvironment(command.Env, expected) {
			t.Fatalf("Pi command environment missing %q: %v", expected, command.Env)
		}
	}
	if containsEnvironment(command.Env, "RUNNER_PROVIDER_TOKEN=real-token-must-be-filtered") {
		t.Fatalf("real provider token leaked into Pi environment: %v", command.Env)
	}
}

func TestClaudeAndOpenCodeAlwaysBypassPermissions(t *testing.T) {
	model := "claude-sonnet-4-6"
	effort := "medium"
	runner := &Runner{
		harness: ResolvedHarness{
			Type: "claude-code", ProviderName: "anthropic", Model: &model, ReasoningEffort: &effort,
			Provider: ProviderConfig{Protocol: "anthropic", BaseURL: "https://api.anthropic.com/v1"},
		},
		workspace: t.TempDir(), proxyToken: "scoped-token", caBundle: "/tmp/container-proxy-ca.pem",
		logger: NewEventLogger(&bytes.Buffer{}, "job", "claude-anthropic", 240000),
	}
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(t.TempDir(), "claude"))
	t.Setenv("CLAUDE_CODE_EXECUTABLE", "/tmp/claude-code-cli.js")
	if err := (claudeCodeDriver{}).Prepare(runner); err != nil {
		t.Fatal(err)
	}
	settings, err := os.ReadFile(filepath.Join(os.Getenv("CLAUDE_CONFIG_DIR"), "settings.json"))
	if err != nil || !strings.Contains(string(settings), `"defaultMode":"bypassPermissions"`) {
		t.Fatalf("Claude settings do not bypass permissions: %s, %v", settings, err)
	}
	command, err := (claudeCodeDriver{}).Command(runner)
	if err != nil {
		t.Fatal(err)
	}
	if !containsEnvironment(command.Env, "ANTHROPIC_BASE_URL=https://api.anthropic.com") {
		t.Fatalf("Claude command did not normalize the shared Anthropic API root: %v", command.Env)
	}
	for _, expected := range []string{
		"CLAUDE_CODE_EXECUTABLE=/tmp/claude-code-cli.js",
		"CLAUDE_CODE_EFFORT_LEVEL=medium",
		"NODE_USE_ENV_PROXY=1",
		"NODE_EXTRA_CA_CERTS=/tmp/container-proxy-ca.pem",
	} {
		if !containsEnvironment(command.Env, expected) {
			t.Fatalf("Claude command environment missing %q: %v", expected, command.Env)
		}
	}
	if containsEnvironment(command.Env, "ANTHROPIC_CUSTOM_MODEL_OPTION="+model) {
		t.Fatalf("natively supported Claude model unexpectedly enabled a custom model option: %v", command.Env)
	}

	runner.harness.Type = "opencode"
	configuration, err := renderOpenCodeConfig(runner.harness)
	if err != nil || !strings.Contains(configuration, `"permission":{"*":"allow","question":"deny"}`) {
		t.Fatalf("OpenCode config does not deny unsupported questions: %s, %v", configuration, err)
	}
}

func containsEnvironment(environment []string, expected string) bool {
	for _, item := range environment {
		if item == expected {
			return true
		}
	}
	return false
}

func environmentHasPrefix(environment []string, prefix string) bool {
	for _, item := range environment {
		if strings.HasPrefix(item, prefix) {
			return true
		}
	}
	return false
}
