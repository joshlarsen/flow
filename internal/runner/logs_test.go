package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const testTraceID = "0123456789abcdef0123456789abcdef"
const testRootSpanID = "0123456789abcdef"

func TestEventLoggerChunksOversizedPayloads(t *testing.T) {
	var output bytes.Buffer
	logger := NewEventLogger(&output, "job-1", "grok", 2048)
	payload, _ := json.Marshal(map[string]string{"text": strings.Repeat("large-value-", 1000)})
	logger.Emit("acp", "info", "session/update", payload, 7)

	scanner := bufio.NewScanner(&output)
	lines := 0
	for scanner.Scan() {
		lines++
		if len(scanner.Bytes()) > 2048 {
			t.Fatalf("log line exceeds limit: %d", len(scanner.Bytes()))
		}
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("invalid JSON log: %v", err)
		}
		if event["encoding"] != "base64" {
			t.Fatalf("expected chunked base64 event: %#v", event)
		}
	}
	if lines < 2 {
		t.Fatalf("expected multiple chunks, got %d", lines)
	}
}

func TestFlowExporterBatchesEventsToWorkerCallback(t *testing.T) {
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("authorization") != "Bearer callback-token" {
			t.Fatal("missing callback authorization")
		}
		body, _ := io.ReadAll(request.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		received <- payload
		response.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	exporter := newFlowExporter(server.URL, "callback-token", "job-1", "grok", "2026-08-31T00:00:00Z", "2026-08-31T00:00:01Z", testTraceID, testRootSpanID, func([]byte) {}, 240000)
	exporter.Enqueue(flowEvent{EventID: "job-1:1:0", Sequence: 1, Type: "session/update", Source: "acp", Level: "info", ProtocolVersion: 2, Data: json.RawMessage(`{"message":"hello"}`), OccurredAt: "2026-08-31T00:00:02Z"})
	exporter.Close()

	payload := <-received
	if payload["schema_version"] != float64(4) {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	run := payload["run"].(map[string]any)
	if run["source_run_id"] != "job-1" || run["status"] != "running" || run["trace_id"] != testTraceID || run["root_span_id"] != testRootSpanID {
		t.Fatalf("unexpected run: %#v", run)
	}
	if spans, ok := payload["spans"].([]any); !ok || len(spans) != 0 {
		t.Fatalf("unexpected initial spans: %#v", payload["spans"])
	}
}

func TestFlowExporterCoalescesACPTextAndLogsMetrics(t *testing.T) {
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		received <- payload
		response.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	var output bytes.Buffer
	logger := NewEventLogger(&output, "job-1", "grok", 240000)
	logger.exporter = newFlowExporterWithInterval(server.URL, "callback-token", "job-1", "grok", "2026-08-31T00:00:00Z", "2026-08-31T00:00:01Z", testTraceID, testRootSpanID, logger.write, time.Hour, 240000)
	for index, text := range []string{"Hello", " ", "world"} {
		payload, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0", "method": "session/update", "vendor": "preserved",
			"params": map[string]any{
				"sessionId": "session-1", "vendorParam": true,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk", "messageId": "message-1",
					"_meta": map[string]any{"vendor": "preserved"}, "vendorUpdate": 7,
					"content": map[string]any{
						"type": "text", "text": text,
						"annotations": map[string]any{"audience": "user"},
					},
				},
			},
		})
		logger.Emit("acp", "info", "session/update", payload, index+1)
	}
	logger.exporter.Close()

	payload := <-received
	events := payload["events"].([]any)
	first := events[0].(map[string]any)
	if first["protocol_version"] != float64(4) || first["trace_id"] != testTraceID || first["span_id"] != testRootSpanID || first["step_id"] != nil || first["step_index"] != nil || first["step_kind"] != nil {
		t.Fatalf("unexpected v4 event coordinates: %#v", first)
	}
	if len(events) != 1 {
		t.Fatalf("expected one coalesced event, got %d", len(events))
	}
	event := events[0].(map[string]any)
	if event["event_id"] != "job-1::1-3:0" || event["sequence"] != float64(1) {
		t.Fatalf("unexpected coalesced envelope: %#v", event)
	}
	data := event["data"].(map[string]any)
	if data["vendor"] != "preserved" {
		t.Fatalf("top-level ACP extension was lost: %#v", data)
	}
	update := data["params"].(map[string]any)["update"].(map[string]any)
	content := update["content"].(map[string]any)
	if content["text"] != "Hello world" {
		t.Fatalf("unexpected coalesced text: %#v", content)
	}
	metadata := update["_meta"].(map[string]any)["flow_coalescing"].(map[string]any)
	if metadata["chunk_count"] != float64(3) || metadata["last_sequence"] != float64(3) {
		t.Fatalf("unexpected coalescing metadata: %#v", metadata)
	}
	if update["vendor_update"] != float64(7) || update["_meta"].(map[string]any)["vendor"] != "preserved" || content["annotations"].(map[string]any)["audience"] != "user" {
		t.Fatalf("nested ACP extensions were lost: %#v", update)
	}
	logs := output.String()
	if strings.Count(logs, `"event_type":"session/update"`) != 3 {
		t.Fatalf("raw ACP events were not preserved: %s", logs)
	}
	if !strings.Contains(logs, `"trace_id":"`+testTraceID+`"`) || !strings.Contains(logs, `"span_id":"`+testRootSpanID+`"`) {
		t.Fatalf("structured logs were not correlated with the Flow trace: %s", logs)
	}
	if !strings.Contains(logs, `"sessionId":"session-1"`) || !strings.Contains(logs, `"sessionUpdate":"agent_message_chunk"`) {
		t.Fatalf("local ACP casing was not preserved: %s", logs)
	}
	if !strings.Contains(logs, `"event_type":"flow_coalesce_flush"`) || !strings.Contains(logs, `"input_events":3`) {
		t.Fatalf("missing flush metrics: %s", logs)
	}
	if !strings.Contains(logs, `"event_type":"flow_coalesce_summary"`) || !strings.Contains(logs, `"raw_stream_events":3`) {
		t.Fatalf("missing summary metrics: %s", logs)
	}
}

func TestEventLoggerRedactsScopedTokenFromNestedPayloads(t *testing.T) {
	var output bytes.Buffer
	logger := NewEventLogger(&output, "job-1", "grok", 240000).WithRedactionSecret("scoped-token")
	payload, _ := json.Marshal(map[string]any{"message": "before scoped-token after", "nested": []any{"scoped-token"}})
	logger.Emit("acp", "info", "session/update", payload, 2)

	if strings.Contains(output.String(), "scoped-token") || !strings.Contains(output.String(), "[REDACTED]") {
		t.Fatalf("scoped credential was not redacted: %s", output.String())
	}
}

func TestFlowExporterFlushesCoalescedTextOnTimer(t *testing.T) {
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		received <- payload
		response.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	exporter := newFlowExporterWithInterval(server.URL, "callback-token", "job-1", "grok", "2026-08-31T00:00:00Z", "2026-08-31T00:00:01Z", testTraceID, testRootSpanID, func([]byte) {}, 10*time.Millisecond, 240000)
	payload := json.RawMessage(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking"}}}}`)
	exporter.Enqueue(flowEvent{EventID: "job-1:1:0", Sequence: 1, Type: "session/update", Source: "acp", Level: "info", ProtocolVersion: 2, Data: payload, OccurredAt: "2026-08-31T00:00:02Z"})

	select {
	case batch := <-received:
		if len(batch["events"].([]any)) != 1 {
			t.Fatalf("unexpected timer batch: %#v", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced stream did not flush on its timer")
	}
	exporter.Close()
}

func TestNormalizeJSONKeysRecursively(t *testing.T) {
	payload := json.RawMessage(`{
		"sessionId":"session-1",
		"already_snake":true,
		"flow.coalescing":{"lastSequence":3},
		"_meta":{"traceparent":"trace","vendorValue":7},
		"items":[{"availableCommands":[],"toolCallId":"tool-1","rawInput":{"mimeType":"text/plain"}}]
	}`)
	normalized, err := normalizeJSONKeys(payload)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err := json.Unmarshal(normalized, &data); err != nil {
		t.Fatal(err)
	}
	if data["session_id"] != "session-1" || data["already_snake"] != true {
		t.Fatalf("top-level keys were not normalized: %#v", data)
	}
	if data["flow_coalescing"].(map[string]any)["last_sequence"] != float64(3) {
		t.Fatalf("dotted key was not normalized: %#v", data)
	}
	metadata := data["_meta"].(map[string]any)
	if metadata["traceparent"] != "trace" || metadata["vendor_value"] != float64(7) {
		t.Fatalf("ACP metadata was not normalized correctly: %#v", metadata)
	}
	item := data["items"].([]any)[0].(map[string]any)
	if item["tool_call_id"] != "tool-1" || item["raw_input"].(map[string]any)["mime_type"] != "text/plain" {
		t.Fatalf("nested keys were not normalized: %#v", item)
	}
}

func TestNormalizeJSONKeysRejectsCollisions(t *testing.T) {
	if _, err := normalizeJSONKeys(json.RawMessage(`{"sessionId":1,"session_id":2}`)); err == nil {
		t.Fatal("expected normalized key collision to fail")
	}
}

func TestSnakeCaseJSONKey(t *testing.T) {
	tests := map[string]string{
		"availableCommands": "available_commands",
		"toolCallId":        "tool_call_id",
		"HTTPServerID":      "http_server_id",
		"flow.coalescing":   "flow_coalescing",
		"already_snake":     "already_snake",
		"_meta":             "_meta",
	}
	for input, expected := range tests {
		if actual := snakeCaseJSONKey(input); actual != expected {
			t.Errorf("snakeCaseJSONKey(%q) = %q, want %q", input, actual, expected)
		}
	}
}

func TestFlowExporterNormalizesBeforeChunking(t *testing.T) {
	var mutex sync.Mutex
	received := []flowEvent{}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var batch struct {
			Events []flowEvent `json:"events"`
		}
		if err := json.NewDecoder(request.Body).Decode(&batch); err != nil {
			t.Error(err)
		}
		mutex.Lock()
		received = append(received, batch.Events...)
		mutex.Unlock()
		response.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	var output bytes.Buffer
	logger := NewEventLogger(&output, "job-1", "grok", 2048)
	logger.exporter = newFlowExporterWithInterval(server.URL, "callback-token", "job-1", "grok", "2026-08-31T00:00:00Z", "2026-08-31T00:00:01Z", testTraceID, testRootSpanID, logger.write, time.Hour, 2048)
	payload, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "method": "session/update",
		"params": map[string]any{
			"sessionId": "session-1",
			"update": map[string]any{
				"sessionUpdate": "tool_call", "toolCallId": "tool-1",
				"rawInput": map[string]any{"availableCommands": strings.Repeat("large-value-", 1000)},
			},
		},
	})
	logger.Emit("acp", "info", "session/update", payload, 7)
	logger.exporter.Close()

	mutex.Lock()
	events := append([]flowEvent(nil), received...)
	mutex.Unlock()
	if len(events) < 2 {
		t.Fatalf("expected multiple Flow chunks, got %d", len(events))
	}
	sort.Slice(events, func(left, right int) bool { return events[left].ChunkIndex < events[right].ChunkIndex })
	var reassembled bytes.Buffer
	for _, event := range events {
		if encoded, err := json.Marshal(event); err != nil || len(encoded) > 2048 {
			t.Fatalf("Flow chunk exceeds configured limit: size=%d err=%v", len(encoded), err)
		}
		var chunk struct {
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal(event.Data, &chunk); err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(chunk.Payload)
		if err != nil {
			t.Fatal(err)
		}
		reassembled.Write(decoded)
	}
	var data map[string]any
	if err := json.Unmarshal(reassembled.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	params := data["params"].(map[string]any)
	update := params["update"].(map[string]any)
	if params["session_id"] != "session-1" || update["session_update"] != "tool_call" || update["tool_call_id"] != "tool-1" {
		t.Fatalf("reassembled Flow payload was not normalized: %#v", data)
	}
	if _, ok := update["raw_input"].(map[string]any)["available_commands"]; !ok {
		t.Fatalf("nested Flow payload was not normalized: %#v", update)
	}
}

func TestHistoryConflictFailsExporterWithoutSilentSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { response.WriteHeader(http.StatusConflict) }))
	defer server.Close()
	exporter := newFlowExporter(server.URL, "token", "job-1", "workflow", "2026-10-04T00:00:00Z", "2026-10-04T00:00:01Z", testTraceID, testRootSpanID, func([]byte) {}, 240000)
	exporter.Enqueue(flowEvent{EventID: "job-1:1:0", Sequence: 1, Type: "session/update", Source: "acp", Level: "info", ProtocolVersion: 4, Data: json.RawMessage(`{"text":"hello"}`), OccurredAt: "2026-10-04T00:00:02Z"})
	exporter.Close()
	if exporter.Error() == nil {
		t.Fatal("conflicting history must fail the exporter")
	}
}
