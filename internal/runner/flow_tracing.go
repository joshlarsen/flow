package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type activeFlowSpan struct {
	id         string
	parentID   string
	name       string
	kind       string
	startedAt  string
	attributes map[string]any
}

type flowTraceBuilder struct {
	mu         sync.Mutex
	traceID    string
	rootSpanID string
	step       *activeFlowSpan
	command    *activeFlowSpan
	prompt     *activeFlowSpan
	processing *activeFlowSpan
	rpcs       map[string][]*activeFlowSpan
	tools      map[string]*activeFlowSpan
}

func newFlowTraceBuilder(traceID, rootSpanID string) *flowTraceBuilder {
	return &flowTraceBuilder{traceID: traceID, rootSpanID: rootSpanID, rpcs: map[string][]*activeFlowSpan{}, tools: map[string]*activeFlowSpan{}}
}

func newFlowSpanID() string {
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		panic(fmt.Sprintf("generate Flow span ID: %v", err))
	}
	if allZero(value) {
		value[len(value)-1] = 1
	}
	return hex.EncodeToString(value)
}

func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}

func validTraceID(value string) bool {
	return validFlowID(value, 16)
}

func validSpanID(value string) bool {
	return validFlowID(value, 8)
}

func validFlowID(value string, byteLength int) bool {
	if len(value) != byteLength*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && !allZero(decoded)
}

func (builder *flowTraceBuilder) start(name, kind, parentID, startedAt string, attributes map[string]any) *activeFlowSpan {
	return &activeFlowSpan{id: newFlowSpanID(), parentID: parentID, name: name, kind: kind, startedAt: startedAt, attributes: attributes}
}

/** Opens the stable logical span shared by every attempt of one workflow step. */
func (builder *flowTraceBuilder) startStep(event *flowEvent, startedAt string) *activeFlowSpan {
	span := builder.start("workflow.step", "INTERNAL", builder.rootSpanID, startedAt, stepSpanAttributes(event))
	if event.StepID != nil && event.StepIndex != nil {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", builder.rootSpanID, *event.StepIndex, *event.StepID)))
		if allZero(digest[:8]) {
			digest[7] = 1
		}
		span.id = hex.EncodeToString(digest[:8])
	}
	return span
}

func (builder *flowTraceBuilder) finish(span *activeFlowSpan, finishedAt, status, reason string) flowSpan {
	attributes := make(map[string]any, len(span.attributes)+1)
	for key, value := range span.attributes {
		attributes[key] = value
	}
	if reason != "" {
		attributes["agent_runner.span.end_reason"] = reason
	}
	var parent *string
	if span.parentID != "" {
		value := span.parentID
		parent = &value
	}
	return flowSpan{
		TraceID: builder.traceID, SpanID: span.id, ParentSpanID: parent,
		Name: span.name, Kind: span.kind, StartedAt: span.startedAt, FinishedAt: finishedAt,
		Status: flowSpanStatus{Code: status}, Attributes: attributes,
	}
}

/** Decorates one normalized event and returns any spans completed by that event. */
func (builder *flowTraceBuilder) decorate(event *flowEvent) []flowSpan {
	builder.mu.Lock()
	defer builder.mu.Unlock()
	event.TraceID = builder.traceID
	event.SpanID = builder.rootSpanID
	switch event.Type {
	case "step.started":
		completed := builder.closeChildren(event.OccurredAt, "UNSET", "next_step_started")
		builder.step = builder.startStep(event, stepLifecycleStartedAt(event))
		event.SpanID = builder.step.id
		return completed
	case "step.resumed":
		completed := builder.closeChildren(event.OccurredAt, "UNSET", "step_resumed")
		builder.step = builder.startStep(event, stepLifecycleStartedAt(event))
		event.SpanID = builder.step.id
		return completed
	case "step.suspended":
		if builder.step == nil {
			builder.step = builder.startStep(event, event.OccurredAt)
			builder.step.attributes["agent_runner.span.recovered"] = true
		}
		event.SpanID = builder.step.id
		completed := builder.closeChildren(event.OccurredAt, "UNSET", "waiting_for_input")
		completed = append(completed, builder.finish(builder.step, event.OccurredAt, "UNSET", "waiting_for_input"))
		builder.step = nil
		return completed
	case "step.finished":
		if builder.step == nil {
			builder.step = builder.startStep(event, event.OccurredAt)
			builder.step.attributes["agent_runner.span.recovered"] = true
		}
		event.SpanID = builder.step.id
		status := eventTerminalStatus(event)
		forcedStatus := "UNSET"
		if status == "ERROR" {
			forcedStatus = "ERROR"
		}
		childReason, stepReason := "step_finished", ""
		if eventCompletionStatus(event) == "waiting_for_input" {
			childReason, stepReason = "waiting_for_input", "waiting_for_input"
		}
		completed := builder.closeChildren(event.OccurredAt, forcedStatus, childReason)
		completed = append(completed, builder.finish(builder.step, event.OccurredAt, status, stepReason))
		builder.step = nil
		return completed
	case "command.started":
		builder.command = builder.start("workflow.command", "INTERNAL", builder.currentStepID(), event.OccurredAt, map[string]any{})
		event.SpanID = builder.command.id
		return nil
	case "command.stdout", "command.stderr":
		if builder.command != nil {
			event.SpanID = builder.command.id
		} else {
			event.SpanID = builder.currentStepID()
		}
		return nil
	case "command.finished":
		if builder.command == nil {
			builder.command = builder.start("workflow.command", "INTERNAL", builder.currentStepID(), event.OccurredAt, map[string]any{"agent_runner.span.recovered": true})
		}
		event.SpanID = builder.command.id
		span := builder.finish(builder.command, event.OccurredAt, eventTerminalStatus(event), "")
		builder.command = nil
		return []flowSpan{span}
	}
	if event.Source == "acp" {
		return builder.decorateACP(event)
	}
	event.SpanID = builder.currentStepID()
	return nil
}

/** Reads the logical start time carried by a step lifecycle marker. */
func stepLifecycleStartedAt(event *flowEvent) string {
	var payload struct {
		StartedAt string `json:"started_at"`
	}
	if json.Unmarshal(event.Data, &payload) == nil {
		if _, err := time.Parse(time.RFC3339Nano, payload.StartedAt); err == nil {
			return payload.StartedAt
		}
	}
	return event.OccurredAt
}

/** Correlates ACP request/response pairs and attaches notifications to their active operation. */
func (builder *flowTraceBuilder) decorateACP(event *flowEvent) []flowSpan {
	var payload map[string]any
	if json.Unmarshal(event.Data, &payload) != nil {
		event.SpanID = builder.currentActivityID()
		return nil
	}
	method, _ := payload["method"].(string)
	phase, _ := payload["phase"].(string)
	direction, _ := payload["direction"].(string)
	params, _ := payload["params"].(map[string]any)
	if method == "session/update" && phase == "notification" {
		return builder.decorateSessionUpdate(event, params)
	}
	if phase == "request" {
		completed := []flowSpan{}
		parentID := builder.currentActivityID()
		if method == "session/request_permission" {
			if tool := nestedMap(params, "tool_call"); tool != nil {
				if toolID := boundedString(tool["tool_call_id"]); toolID != "" {
					var toolSpan *activeFlowSpan
					toolSpan, completed = builder.ensureTool(toolID, tool, event.OccurredAt)
					parentID = toolSpan.id
				}
			}
		}
		kind := "CLIENT"
		if direction == "agent_to_client" {
			kind = "SERVER"
		}
		span := builder.start("acp.rpc", kind, parentID, event.OccurredAt, map[string]any{
			"rpc.system": "acp", "rpc.method": boundedString(method), "agent_runner.acp.direction": boundedString(direction),
		})
		event.SpanID = span.id
		if method == "session/prompt" {
			builder.prompt = span
			builder.startProcessing(event.OccurredAt)
		} else {
			builder.rpcs[method] = append(builder.rpcs[method], span)
		}
		return completed
	}
	if phase == "response" {
		var span *activeFlowSpan
		if method == "session/prompt" {
			span = builder.prompt
		} else {
			active := builder.rpcs[method]
			if len(active) > 0 {
				span = active[len(active)-1]
				builder.rpcs[method] = active[:len(active)-1]
			}
		}
		if span == nil {
			span = builder.start("acp.rpc", rpcKindForResponse(direction), builder.currentActivityID(), event.OccurredAt, map[string]any{
				"rpc.system": "acp", "rpc.method": boundedString(method), "agent_runner.span.recovered": true,
			})
		}
		event.SpanID = span.id
		status := "OK"
		if event.Level == "error" {
			status = "ERROR"
		}
		if method == "session/prompt" {
			status = eventTerminalStatus(event)
		}
		completed := []flowSpan{}
		if method == "session/prompt" {
			forced := "UNSET"
			if status == "ERROR" {
				forced = "ERROR"
			}
			processingReason, toolReason := "", "prompt_finished"
			if eventCompletionStatus(event) == "waiting_for_input" {
				processingReason, toolReason = "waiting_for_input", "waiting_for_input"
			}
			completed = append(completed, builder.finishProcessing(event.OccurredAt, status, processingReason)...)
			completed = append(completed, builder.closeTools(event.OccurredAt, forced, toolReason)...)
			builder.prompt = nil
		}
		spanReason := ""
		if method == "session/prompt" && eventCompletionStatus(event) == "waiting_for_input" {
			spanReason = "waiting_for_input"
		}
		return append(completed, builder.finish(span, event.OccurredAt, status, spanReason))
	}
	event.SpanID = builder.currentActivityID()
	return nil
}

/** Converts tool lifecycle updates into one span while retaining progress updates as logs. */
func (builder *flowTraceBuilder) decorateSessionUpdate(event *flowEvent, params map[string]any) []flowSpan {
	update := nestedMap(params, "update")
	if update == nil {
		event.SpanID = builder.currentActivityID()
		return nil
	}
	kind := boundedString(update["session_update"])
	if kind != "tool_call" && kind != "tool_call_update" {
		event.SpanID = builder.currentActivityID()
		return nil
	}
	toolID := boundedString(update["tool_call_id"])
	if toolID == "" {
		event.SpanID = builder.currentActivityID()
		return nil
	}
	span, completed := builder.ensureTool(toolID, update, event.OccurredAt)
	if assignments := metricToolAssignments(update); assignments != "" {
		span.attributes["agent_runner.metric.assignments"] = assignments
	}
	event.SpanID = span.id
	status := boundedString(update["status"])
	if status != "completed" && status != "failed" {
		return completed
	}
	delete(builder.tools, toolID)
	code := "OK"
	if status == "failed" {
		code = "ERROR"
	}
	span.attributes["agent_runner.tool.status"] = status
	completed = append(completed, builder.finish(span, event.OccurredAt, code, ""))
	if len(builder.tools) == 0 {
		builder.startProcessing(event.OccurredAt)
	}
	return completed
}

/** Returns an active tool span and pauses prompt processing when the first tool starts. */
func (builder *flowTraceBuilder) ensureTool(toolID string, source map[string]any, startedAt string) (*activeFlowSpan, []flowSpan) {
	if span := builder.tools[toolID]; span != nil {
		return span, nil
	}
	completed := builder.finishProcessing(startedAt, "OK", "")
	attributes := map[string]any{"gen_ai.tool.call.id": toolID}
	if name := toolName(source); name != "" {
		attributes["gen_ai.tool.name"] = name
	}
	if kind := boundedString(source["kind"]); kind != "" {
		attributes["agent_runner.tool.kind"] = kind
	}
	span := builder.start("gen_ai.tool.call", "INTERNAL", builder.currentActivityID(), startedAt, attributes)
	builder.tools[toolID] = span
	return span, completed
}

/** Starts a prompt processing interval when no tool calls are active. */
func (builder *flowTraceBuilder) startProcessing(startedAt string) {
	if builder.prompt == nil || builder.processing != nil || len(builder.tools) != 0 {
		return
	}
	builder.processing = builder.start("agent.processing", "INTERNAL", builder.prompt.id, startedAt, map[string]any{})
}

/** Finishes the active prompt processing interval, if one exists. */
func (builder *flowTraceBuilder) finishProcessing(finishedAt, status, reason string) []flowSpan {
	if builder.processing == nil {
		return nil
	}
	span := builder.finish(builder.processing, finishedAt, status, reason)
	builder.processing = nil
	return []flowSpan{span}
}

func toolName(source map[string]any) string {
	if metadata := nestedMap(source, "_meta"); metadata != nil {
		if value := boundedString(metadata["tool_name"]); value != "" {
			return value
		}
	}
	return boundedString(source["title"])
}

/** Returns the programmatic MCP tool name exposed by supported harness adapters. */
func metricToolName(source map[string]any) string {
	if metadata := nestedMap(source, "_meta"); metadata != nil {
		if value := boundedString(metadata["tool_name"]); value != "" {
			return value
		}
		if claudeCode := nestedMap(metadata, "claude_code"); claudeCode != nil {
			if value := boundedString(claudeCode["tool_name"]); value != "" {
				return value
			}
		}
	}
	return boundedString(source["title"])
}

/** Returns the validated metric assignments carried by a metrics MCP tool update. */
func metricToolAssignments(source map[string]any) string {
	rawInput := nestedMap(source, "raw_input")
	if rawInput == nil {
		return ""
	}
	arguments := rawInput
	if boundedString(rawInput["server"]) == "metrics" && boundedString(rawInput["tool"]) == "emit" {
		arguments = nestedMap(rawInput, "arguments")
	} else if boundedString(rawInput["tool_name"]) == "metrics__emit" {
		arguments = nestedMap(rawInput, "tool_input")
	} else if name := metricToolName(source); name != "mcp__metrics__emit" && name != "mcp.metrics.emit" && name != "metrics_emit" {
		return ""
	}
	if arguments == nil {
		return ""
	}
	metrics, ok := arguments["metrics"].([]any)
	if !ok {
		return ""
	}
	assignments := make([]string, 0, len(metrics))
	length := 0
	for _, raw := range metrics {
		assignment, ok := raw.(string)
		if !ok {
			continue
		}
		namespace, key, value, err := parseMetricAssignment(assignment)
		if err != nil {
			continue
		}
		canonical := namespace + "." + key + "=" + strconv.FormatFloat(value, 'g', -1, 64)
		nextLength := length + len(canonical)
		if len(assignments) > 0 {
			nextLength += 2
		}
		if nextLength > 512 {
			break
		}
		assignments = append(assignments, canonical)
		length = nextLength
	}
	return strings.Join(assignments, ", ")
}

func nestedMap(source map[string]any, key string) map[string]any {
	value, _ := source[key].(map[string]any)
	return value
}

func boundedString(value any) string {
	text, _ := value.(string)
	text = strings.TrimSpace(text)
	if len(text) > 256 {
		return text[:256]
	}
	return text
}

func rpcKindForResponse(direction string) string {
	if direction == "client_to_agent" {
		return "SERVER"
	}
	return "CLIENT"
}

func stepSpanAttributes(event *flowEvent) map[string]any {
	attributes := map[string]any{}
	if event.StepID != nil {
		attributes["agent_runner.step.id"] = *event.StepID
	}
	if event.StepIndex != nil {
		attributes["agent_runner.step.index"] = *event.StepIndex
	}
	if event.StepKind != nil {
		attributes["agent_runner.step.kind"] = *event.StepKind
	}
	var payload map[string]any
	if json.Unmarshal(event.Data, &payload) == nil {
		for source, target := range map[string]string{
			"harness": "agent_runner.harness", "provider": "agent_runner.provider", "model": "gen_ai.request.model", "reasoning_effort": "agent_runner.reasoning_effort",
		} {
			if value := boundedString(payload[source]); value != "" {
				attributes[target] = value
			}
		}
	}
	return attributes
}

func eventTerminalStatus(event *flowEvent) string {
	status := eventCompletionStatus(event)
	switch status {
	case "succeeded", "completed":
		return "OK"
	case "failed", "timed_out":
		return "ERROR"
	case "waiting_for_input", "partial", "cancelled", "interrupted":
		return "UNSET"
	}
	if event.Level == "error" {
		return "ERROR"
	}
	return "OK"
}

/** Returns the normalized completion state carried by a runner or ACP response event. */
func eventCompletionStatus(event *flowEvent) string {
	var payload map[string]any
	if json.Unmarshal(event.Data, &payload) == nil {
		if status := boundedString(payload["status"]); status != "" {
			return status
		}
		if result := nestedMap(payload, "result"); result != nil {
			return boundedString(result["status"])
		}
	}
	return ""
}

func (builder *flowTraceBuilder) currentStepID() string {
	if builder.step != nil {
		return builder.step.id
	}
	return builder.rootSpanID
}

/** Returns the active workflow-step span, or the root span between steps. */
func (builder *flowTraceBuilder) metricSpanID() string {
	builder.mu.Lock()
	defer builder.mu.Unlock()
	return builder.currentStepID()
}

func (builder *flowTraceBuilder) currentActivityID() string {
	if builder.processing != nil {
		return builder.processing.id
	}
	if builder.prompt != nil {
		return builder.prompt.id
	}
	return builder.currentStepID()
}

func (builder *flowTraceBuilder) closeTools(finishedAt, status, reason string) []flowSpan {
	completed := make([]flowSpan, 0, len(builder.tools))
	for toolID, span := range builder.tools {
		completed = append(completed, builder.finish(span, finishedAt, status, reason))
		delete(builder.tools, toolID)
	}
	return completed
}

/** Closes every active child before its enclosing step or exporter scope ends. */
func (builder *flowTraceBuilder) closeChildren(finishedAt, status, reason string) []flowSpan {
	completed := []flowSpan{}
	for method, active := range builder.rpcs {
		for index := len(active) - 1; index >= 0; index-- {
			completed = append(completed, builder.finish(active[index], finishedAt, status, reason))
		}
		delete(builder.rpcs, method)
	}
	completed = append(completed, builder.closeTools(finishedAt, status, reason)...)
	completed = append(completed, builder.finishProcessing(finishedAt, status, reason)...)
	if builder.command != nil {
		completed = append(completed, builder.finish(builder.command, finishedAt, status, reason))
		builder.command = nil
	}
	if builder.prompt != nil {
		completed = append(completed, builder.finish(builder.prompt, finishedAt, status, reason))
		builder.prompt = nil
	}
	return completed
}

func (builder *flowTraceBuilder) closeIncomplete(finishedAt, status, reason string) []flowSpan {
	builder.mu.Lock()
	defer builder.mu.Unlock()
	completed := builder.closeChildren(finishedAt, status, reason)
	if builder.step != nil {
		completed = append(completed, builder.finish(builder.step, finishedAt, status, reason))
		builder.step = nil
	}
	return completed
}
