package main

import (
	"encoding/json"
	"testing"
)

func tracedTestEvent(sequence int, eventType, source, level, occurredAt string, data any) flowEvent {
	payload, _ := json.Marshal(data)
	stepID, stepIndex, stepKind := "run", 0, "harness"
	return flowEvent{
		EventID: "job-1:run:1:0", Sequence: sequence, Type: eventType, Source: source, Level: level,
		StepID: &stepID, StepIndex: &stepIndex, StepKind: &stepKind, Data: payload, OccurredAt: occurredAt,
	}
}

func TestFlowTraceBuilderBuildsPromptToolAndStepHierarchy(t *testing.T) {
	builder := newFlowTraceBuilder(testTraceID, testRootSpanID)
	step := tracedTestEvent(1, "step.started", "runner", "info", "2026-09-05T10:00:00Z", map[string]any{"harness": "pi", "reasoning_effort": "medium"})
	if spans := builder.decorate(&step); len(spans) != 0 || step.SpanID == testRootSpanID {
		t.Fatalf("step did not open its own span: event=%+v spans=%+v", step, spans)
	}
	stepSpanID := step.SpanID
	if builder.step.attributes["agent_runner.reasoning_effort"] != "medium" {
		t.Fatalf("step span omitted reasoning effort: %+v", builder.step.attributes)
	}

	prompt := tracedTestEvent(2, "session/prompt", "acp", "info", "2026-09-05T10:00:01Z", map[string]any{
		"method": "session/prompt", "phase": "request", "direction": "client_to_agent", "params": map[string]any{},
	})
	builder.decorate(&prompt)
	promptSpanID := prompt.SpanID
	if promptSpanID == stepSpanID {
		t.Fatal("prompt request did not open an RPC span")
	}
	processingSpanID := builder.processing.id
	message := tracedTestEvent(3, "session/update", "acp", "info", "2026-09-05T10:00:01.500Z", map[string]any{
		"method": "session/update", "phase": "notification", "direction": "agent_to_client",
		"params": map[string]any{"update": map[string]any{"session_update": "agent_message_chunk"}},
	})
	if spans := builder.decorate(&message); len(spans) != 0 || message.SpanID != processingSpanID {
		t.Fatalf("message update was not correlated with processing: event=%+v spans=%+v", message, spans)
	}

	tool := tracedTestEvent(4, "session/update", "acp", "info", "2026-09-05T10:00:02Z", map[string]any{
		"method": "session/update", "phase": "notification", "direction": "agent_to_client",
		"params": map[string]any{"update": map[string]any{
			"session_update": "tool_call", "tool_call_id": "tool-1", "status": "pending", "title": "read", "kind": "read",
		}},
	})
	processingSpans := builder.decorate(&tool)
	if len(processingSpans) != 1 || processingSpans[0].SpanID != processingSpanID || processingSpans[0].Name != "agent.processing" || pointerValueString(processingSpans[0].ParentSpanID) != promptSpanID || processingSpans[0].StartedAt != "2026-09-05T10:00:01Z" || processingSpans[0].FinishedAt != tool.OccurredAt {
		t.Fatalf("unexpected initial processing span: %+v", processingSpans)
	}
	toolSpanID := tool.SpanID
	progress := tracedTestEvent(5, "session/update", "acp", "info", "2026-09-05T10:00:03Z", map[string]any{
		"method": "session/update", "phase": "notification", "direction": "agent_to_client",
		"params": map[string]any{"update": map[string]any{
			"session_update": "tool_call_update", "tool_call_id": "tool-1", "status": "in_progress",
		}},
	})
	if spans := builder.decorate(&progress); len(spans) != 0 || progress.SpanID != toolSpanID {
		t.Fatalf("in-progress update created a span or lost correlation: event=%+v spans=%+v", progress, spans)
	}
	failed := tracedTestEvent(6, "session/update", "acp", "error", "2026-09-05T10:00:04Z", map[string]any{
		"method": "session/update", "phase": "notification", "direction": "agent_to_client",
		"params": map[string]any{"update": map[string]any{
			"session_update": "tool_call_update", "tool_call_id": "tool-1", "status": "failed",
		}},
	})
	toolSpans := builder.decorate(&failed)
	if len(toolSpans) != 1 || toolSpans[0].SpanID != toolSpanID || pointerValueString(toolSpans[0].ParentSpanID) != promptSpanID || toolSpans[0].Status.Code != "ERROR" {
		t.Fatalf("unexpected tool span: %+v", toolSpans)
	}
	finalProcessingSpanID := builder.processing.id

	promptResponse := tracedTestEvent(7, "session/prompt", "acp", "info", "2026-09-05T10:00:05Z", map[string]any{
		"method": "session/prompt", "phase": "response", "direction": "agent_to_client", "result": map[string]any{},
	})
	promptSpans := builder.decorate(&promptResponse)
	if len(promptSpans) != 2 || promptSpans[0].SpanID != finalProcessingSpanID || promptSpans[0].Name != "agent.processing" || promptSpans[0].StartedAt != failed.OccurredAt || promptSpans[0].FinishedAt != promptResponse.OccurredAt {
		t.Fatalf("unexpected final processing span: %+v", promptSpans)
	}
	if promptSpans[1].SpanID != promptSpanID || pointerValueString(promptSpans[1].ParentSpanID) != stepSpanID || promptSpans[1].Kind != "CLIENT" {
		t.Fatalf("unexpected prompt span: %+v", promptSpans)
	}

	finished := tracedTestEvent(8, "step.finished", "runner", "info", "2026-09-05T10:00:06Z", map[string]any{"status": "succeeded"})
	stepSpans := builder.decorate(&finished)
	if len(stepSpans) != 1 || stepSpans[0].SpanID != stepSpanID || pointerValueString(stepSpans[0].ParentSpanID) != testRootSpanID || stepSpans[0].Status.Code != "OK" {
		t.Fatalf("unexpected step span: %+v", stepSpans)
	}
}

func TestFlowTraceBuilderAddsMetricAssignmentsToMetricToolSpan(t *testing.T) {
	builder := newFlowTraceBuilder(testTraceID, testRootSpanID)
	started := tracedTestEvent(1, "session/update", "acp", "info", "2026-09-05T10:00:02Z", map[string]any{
		"method": "session/update", "phase": "notification", "direction": "agent_to_client",
		"params": map[string]any{"update": map[string]any{
			"session_update": "tool_call", "tool_call_id": "tool-metric", "status": "in_progress",
			"title": "mcp.metrics.emit",
		}},
	})
	builder.decorate(&started)

	finished := tracedTestEvent(2, "session/update", "acp", "info", "2026-09-05T10:00:03Z", map[string]any{
		"method": "session/update", "phase": "notification", "direction": "agent_to_client",
		"params": map[string]any{"update": map[string]any{
			"session_update": "tool_call_update", "tool_call_id": "tool-metric", "status": "completed",
			"raw_input": map[string]any{
				"server": "metrics", "tool": "emit",
				"arguments": map[string]any{"metrics": []any{"haiku.num_lines=3", "customers.pro=345"}},
			},
		}},
	})
	spans := builder.decorate(&finished)
	if len(spans) != 1 {
		t.Fatalf("expected one completed tool span, got %+v", spans)
	}
	if got := spans[0].Attributes["agent_runner.metric.assignments"]; got != "haiku.num_lines=3, customers.pro=345" {
		t.Fatalf("unexpected metric assignments attribute: %v", got)
	}
}

func TestMetricToolAssignmentsSupportsHarnessPayloadShapes(t *testing.T) {
	for _, test := range []struct {
		name   string
		source map[string]any
		want   string
	}{
		{
			name: "codex wrapper",
			source: map[string]any{"raw_input": map[string]any{
				"server": "metrics", "tool": "emit",
				"arguments": map[string]any{"metrics": []any{"haiku.num_lines=3"}},
			}},
			want: "haiku.num_lines=3",
		},
		{
			name: "pi direct arguments",
			source: map[string]any{
				"_meta":     map[string]any{"tool_name": "mcp__metrics__emit"},
				"raw_input": map[string]any{"metrics": []any{"haiku.num_lines=3"}},
			},
			want: "haiku.num_lines=3",
		},
		{
			name: "claude direct arguments",
			source: map[string]any{
				"_meta":     map[string]any{"claude_code": map[string]any{"tool_name": "mcp__metrics__emit"}},
				"raw_input": map[string]any{"metrics": []any{"haiku.num_lines=3"}},
			},
			want: "haiku.num_lines=3",
		},
		{
			name: "opencode direct arguments",
			source: map[string]any{
				"title": "metrics_emit", "raw_input": map[string]any{"metrics": []any{"haiku.num_lines=3"}},
			},
			want: "haiku.num_lines=3",
		},
		{
			name: "grok nested arguments",
			source: map[string]any{
				"title": "metrics__emit", "raw_input": map[string]any{
					"variant": "UseTool", "tool_name": "metrics__emit",
					"tool_input": map[string]any{"metrics": []any{"haiku.num_lines=3"}},
				},
			},
			want: "haiku.num_lines=3",
		},
		{
			name: "unrelated direct arguments",
			source: map[string]any{
				"title": "other", "raw_input": map[string]any{"metrics": []any{"haiku.num_lines=3"}},
			},
		},
		{
			name: "unrelated grok tool",
			source: map[string]any{
				"title": "use_tool", "raw_input": map[string]any{
					"tool_name":  "other__emit",
					"tool_input": map[string]any{"metrics": []any{"haiku.num_lines=3"}},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := metricToolAssignments(test.source); got != test.want {
				t.Fatalf("metric assignments = %q, want %q", got, test.want)
			}
		})
	}
}

func TestFlowTraceBuilderOpensPermissionFirstToolAndForceClosesIt(t *testing.T) {
	builder := newFlowTraceBuilder(testTraceID, testRootSpanID)
	step := tracedTestEvent(1, "step.started", "runner", "info", "2026-09-05T10:00:00Z", map[string]any{})
	builder.decorate(&step)
	prompt := tracedTestEvent(2, "session/prompt", "acp", "info", "2026-09-05T10:00:01Z", map[string]any{"method": "session/prompt", "phase": "request", "direction": "client_to_agent"})
	builder.decorate(&prompt)
	permission := tracedTestEvent(3, "session/request_permission", "acp", "info", "2026-09-05T10:00:02Z", map[string]any{
		"method": "session/request_permission", "phase": "request", "direction": "agent_to_client",
		"params": map[string]any{"tool_call": map[string]any{"tool_call_id": "tool-2", "title": "bash", "kind": "execute"}},
	})
	processingSpans := builder.decorate(&permission)
	if len(processingSpans) != 1 || processingSpans[0].Name != "agent.processing" || pointerValueString(processingSpans[0].ParentSpanID) != prompt.SpanID {
		t.Fatalf("permission-first tool did not pause processing: %+v", processingSpans)
	}
	permissionSpanID := permission.SpanID
	toolSpanID := builder.tools["tool-2"].id
	if active := builder.rpcs["session/request_permission"]; len(active) != 1 || active[0].parentID != toolSpanID {
		t.Fatalf("permission RPC was not parented to its tool: %+v", active)
	}
	response := tracedTestEvent(4, "session/request_permission", "acp", "info", "2026-09-05T10:00:03Z", map[string]any{"method": "session/request_permission", "phase": "response", "direction": "client_to_agent"})
	permissionSpans := builder.decorate(&response)
	if len(permissionSpans) != 1 || permissionSpans[0].SpanID != permissionSpanID || permissionSpans[0].Kind != "SERVER" {
		t.Fatalf("unexpected permission RPC span: %+v", permissionSpans)
	}
	finished := tracedTestEvent(5, "step.finished", "runner", "error", "2026-09-05T10:00:04Z", map[string]any{"status": "failed"})
	spans := builder.decorate(&finished)
	if len(spans) != 3 {
		t.Fatalf("expected tool, prompt, and step closures, got %+v", spans)
	}
	if spans[0].Name != "gen_ai.tool.call" || spans[0].Status.Code != "ERROR" || spans[0].Attributes["agent_runner.span.end_reason"] != "step_finished" {
		t.Fatalf("incomplete tool was not force-closed safely: %+v", spans[0])
	}
}

func TestFlowTraceBuilderKeepsProcessingPausedUntilAllToolsFinish(t *testing.T) {
	builder := newFlowTraceBuilder(testTraceID, testRootSpanID)
	prompt := tracedTestEvent(1, "session/prompt", "acp", "info", "2026-09-05T10:00:01Z", map[string]any{
		"method": "session/prompt", "phase": "request", "direction": "client_to_agent",
	})
	builder.decorate(&prompt)

	toolEvent := func(sequence int, occurredAt, toolID, status string) flowEvent {
		return tracedTestEvent(sequence, "session/update", "acp", "info", occurredAt, map[string]any{
			"method": "session/update", "phase": "notification", "direction": "agent_to_client",
			"params": map[string]any{"update": map[string]any{
				"session_update": "tool_call_update", "tool_call_id": toolID, "status": status,
			}},
		})
	}
	first := toolEvent(2, "2026-09-05T10:00:02Z", "tool-1", "in_progress")
	if spans := builder.decorate(&first); len(spans) != 1 || spans[0].Name != "agent.processing" {
		t.Fatalf("first tool did not pause processing: %+v", spans)
	}
	second := toolEvent(3, "2026-09-05T10:00:03Z", "tool-2", "in_progress")
	if spans := builder.decorate(&second); len(spans) != 0 {
		t.Fatalf("overlapping tool emitted another processing span: %+v", spans)
	}
	firstFinished := toolEvent(4, "2026-09-05T10:00:04Z", "tool-1", "completed")
	if spans := builder.decorate(&firstFinished); len(spans) != 1 || builder.processing != nil {
		t.Fatalf("processing resumed before all tools finished: spans=%+v processing=%+v", spans, builder.processing)
	}
	secondFinished := toolEvent(5, "2026-09-05T10:00:05Z", "tool-2", "completed")
	if spans := builder.decorate(&secondFinished); len(spans) != 1 || builder.processing == nil || builder.processing.startedAt != secondFinished.OccurredAt {
		t.Fatalf("processing did not resume after the final tool: spans=%+v processing=%+v", spans, builder.processing)
	}
}

func TestFlowTraceBuilderClosesProcessingWithPromptAndForcedStatuses(t *testing.T) {
	t.Run("successful prompt without tools", func(t *testing.T) {
		builder := newFlowTraceBuilder(testTraceID, testRootSpanID)
		prompt := tracedTestEvent(1, "session/prompt", "acp", "info", "2026-09-05T10:00:01Z", map[string]any{
			"method": "session/prompt", "phase": "request", "direction": "client_to_agent",
		})
		builder.decorate(&prompt)
		response := tracedTestEvent(2, "session/prompt", "acp", "info", "2026-09-05T10:00:05Z", map[string]any{
			"method": "session/prompt", "phase": "response", "direction": "agent_to_client",
		})
		spans := builder.decorate(&response)
		if len(spans) != 2 || spans[0].Name != "agent.processing" || spans[0].Status.Code != "OK" || spans[0].StartedAt != prompt.OccurredAt || spans[0].FinishedAt != response.OccurredAt {
			t.Fatalf("prompt did not produce a full processing interval: %+v", spans)
		}
	})

	t.Run("deferred prompt is a neutral suspension", func(t *testing.T) {
		builder := newFlowTraceBuilder(testTraceID, testRootSpanID)
		step := tracedTestEvent(1, "step.started", "runner", "info", "2026-09-05T10:00:00Z", map[string]any{})
		builder.decorate(&step)
		prompt := tracedTestEvent(2, "session/prompt", "acp", "info", "2026-09-05T10:00:01Z", map[string]any{
			"method": "session/prompt", "phase": "request", "direction": "client_to_agent",
		})
		builder.decorate(&prompt)
		elicitation := tracedTestEvent(3, "elicitation/create", "acp", "info", "2026-09-05T10:00:02Z", map[string]any{
			"method": "elicitation/create", "phase": "request", "direction": "agent_to_client",
		})
		builder.decorate(&elicitation)
		deferred := tracedTestEvent(4, "elicitation/create", "acp", "info", "2026-09-05T10:00:03Z", map[string]any{
			"method": "elicitation/create", "phase": "response", "direction": "client_to_agent",
			"result": map[string]any{"action": "cancel", "status": "waiting_for_input"},
		})
		elicitationSpans := builder.decorate(&deferred)
		if len(elicitationSpans) != 1 || elicitationSpans[0].Status.Code != "OK" {
			t.Fatalf("deferred elicitation RPC did not complete successfully: %+v", elicitationSpans)
		}
		response := tracedTestEvent(5, "session/prompt", "acp", "info", "2026-09-05T10:00:04Z", map[string]any{
			"method": "session/prompt", "phase": "response", "direction": "agent_to_client",
			"result": map[string]any{"status": "waiting_for_input"},
		})
		promptSpans := builder.decorate(&response)
		if len(promptSpans) != 2 {
			t.Fatalf("expected processing and prompt spans, got %+v", promptSpans)
		}
		for _, span := range promptSpans {
			if span.Status.Code != "UNSET" || span.Attributes["agent_runner.span.end_reason"] != "waiting_for_input" {
				t.Fatalf("deferred prompt span was not neutral: %+v", span)
			}
		}
		finished := tracedTestEvent(6, "step.suspended", "runner", "info", "2026-09-05T10:00:05Z", map[string]any{"status": "waiting_for_input"})
		stepSpans := builder.decorate(&finished)
		if len(stepSpans) != 1 || stepSpans[0].Status.Code != "UNSET" || stepSpans[0].Attributes["agent_runner.span.end_reason"] != "waiting_for_input" {
			t.Fatalf("deferred step span was not neutral: %+v", stepSpans)
		}
	})

	t.Run("resumed step keeps one logical span identity and start", func(t *testing.T) {
		startedAt := "2026-09-05T10:00:00Z"
		first := newFlowTraceBuilder(testTraceID, testRootSpanID)
		started := tracedTestEvent(1, "step.started", "runner", "info", "2026-09-05T10:00:00.001Z", map[string]any{
			"harness": "grok", "model": "grok-4.1", "started_at": startedAt,
		})
		first.decorate(&started)
		suspended := tracedTestEvent(2, "step.suspended", "runner", "info", "2026-09-05T10:00:05Z", map[string]any{"status": "waiting_for_input"})
		provisional := first.decorate(&suspended)
		if len(provisional) != 1 || provisional[0].SpanID != started.SpanID {
			t.Fatalf("unexpected suspended span: %+v", provisional)
		}

		second := newFlowTraceBuilder(testTraceID, testRootSpanID)
		resumed := tracedTestEvent(3, "step.resumed", "runner", "info", "2026-09-05T10:01:00Z", map[string]any{
			"harness": "grok", "model": "grok-4.1", "started_at": startedAt,
		})
		second.decorate(&resumed)
		finished := tracedTestEvent(4, "step.finished", "runner", "info", "2026-09-05T10:01:05Z", map[string]any{"status": "succeeded"})
		terminal := second.decorate(&finished)
		if len(terminal) != 1 || resumed.SpanID != started.SpanID || terminal[0].SpanID != started.SpanID || terminal[0].StartedAt != startedAt {
			t.Fatalf("resume created a second logical step span: started=%+v resumed=%+v terminal=%+v", started, resumed, terminal)
		}
		if _, waiting := terminal[0].Attributes["agent_runner.span.end_reason"]; waiting {
			t.Fatalf("terminal revision retained the suspension marker: %+v", terminal[0])
		}
	})

	t.Run("failed prompt remains an error", func(t *testing.T) {
		builder := newFlowTraceBuilder(testTraceID, testRootSpanID)
		prompt := tracedTestEvent(1, "session/prompt", "acp", "info", "2026-09-05T10:00:01Z", map[string]any{
			"method": "session/prompt", "phase": "request", "direction": "client_to_agent",
		})
		builder.decorate(&prompt)
		response := tracedTestEvent(2, "session/prompt", "acp", "error", "2026-09-05T10:00:04Z", map[string]any{
			"method": "session/prompt", "phase": "response", "direction": "agent_to_client",
			"result": map[string]any{"error": "prompt failed"},
		})
		spans := builder.decorate(&response)
		if len(spans) != 2 || spans[0].Status.Code != "ERROR" || spans[1].Status.Code != "ERROR" {
			t.Fatalf("actual prompt failure was not traced as an error: %+v", spans)
		}
	})

	t.Run("failed step force-closes processing", func(t *testing.T) {
		builder := newFlowTraceBuilder(testTraceID, testRootSpanID)
		step := tracedTestEvent(1, "step.started", "runner", "info", "2026-09-05T10:00:00Z", map[string]any{})
		builder.decorate(&step)
		prompt := tracedTestEvent(2, "session/prompt", "acp", "info", "2026-09-05T10:00:01Z", map[string]any{
			"method": "session/prompt", "phase": "request", "direction": "client_to_agent",
		})
		builder.decorate(&prompt)
		finished := tracedTestEvent(3, "step.finished", "runner", "error", "2026-09-05T10:00:04Z", map[string]any{"status": "failed"})
		spans := builder.decorate(&finished)
		if len(spans) != 3 || spans[0].Name != "agent.processing" || spans[0].Status.Code != "ERROR" || spans[0].Attributes["agent_runner.span.end_reason"] != "step_finished" {
			t.Fatalf("processing was not force-closed with the failed step: %+v", spans)
		}
	})
}

func pointerValueString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
