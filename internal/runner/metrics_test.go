package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseMetricAssignment(t *testing.T) {
	namespace, key, value, err := parseMetricAssignment("customers.pro=345")
	if err != nil || namespace != "customers" || key != "pro" || value != 345 {
		t.Fatalf("unexpected parsed metric: %q %q %v %v", namespace, key, value, err)
	}
	for _, invalid := range []string{"customers=1", "Customers.pro=1", "customers.pro.extra=1", "customers.pro=NaN"} {
		if _, _, _, err := parseMetricAssignment(invalid); err == nil {
			t.Fatalf("expected %q to be rejected", invalid)
		}
	}
}

func TestPromptWithRequiredMetrics(t *testing.T) {
	metrics := []RequiredMetric{
		{Namespace: "haiku", Key: "num_lines", Description: "number of lines in the written haiku"},
		{Namespace: "haiku", Key: "score", Description: "quality score"},
	}
	prompt, err := promptWithRequiredMetrics("Read the haiku.", metrics, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"Read the haiku.\n\n## Required metrics",
		"`metrics` MCP server's `emit` tool (`mcp.metrics.emit`)",
		"- `haiku.num_lines`: number of lines in the written haiku",
		"- `haiku.score`: quality score",
		`{"metrics":["haiku.num_lines=<number>","haiku.score=<number>"]}`,
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("prompt omitted %q:\n%s", expected, prompt)
		}
	}
	plain, err := promptWithRequiredMetrics("Unchanged.", nil, len("Unchanged."))
	if err != nil || plain != "Unchanged." {
		t.Fatalf("prompt without metrics changed: %q, %v", plain, err)
	}
	if _, err := promptWithRequiredMetrics("Read the haiku.", metrics, len("Read the haiku.")); err == nil {
		t.Fatal("expected generated instructions to count toward max_prompt_bytes")
	}
}

func TestMetricReceiverPersistsEveryObservationAndTracksContracts(t *testing.T) {
	var mu sync.Mutex
	var batches []map[string]any
	callback := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var batch map[string]any
		if err := json.NewDecoder(request.Body).Decode(&batch); err != nil {
			t.Fatal(err)
		}
		if _, eventsOK := batch["events"].([]any); !eventsOK {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, spansOK := batch["spans"].([]any); !spansOK {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, metricsOK := batch["metrics"].([]any); !metricsOK {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		batches = append(batches, batch)
		mu.Unlock()
		response.WriteHeader(http.StatusAccepted)
	}))
	defer callback.Close()

	exporter := newFlowExporterWithInterval(callback.URL, strings.Repeat("t", 32), "job-1", "workflow", "2026-09-21T00:00:00Z", "2026-09-21T00:00:01Z", testTraceID, testRootSpanID, func([]byte) {}, time.Hour, 256*1024)
	defer exporter.Close()
	receiver, err := startMetricReceiver(exporter, "job-1", testTraceID, testRootSpanID, &eventSequence{})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	step := WorkflowStep{ID: "report", RequiredMetrics: []RequiredMetric{
		{Namespace: "customers", Key: "free", Description: "free customers"},
		{Namespace: "customers", Key: "pro", Description: "pro customers"},
	}}
	receiver.SetStep(step, 0, nil)

	emit := func(assignments ...string) {
		payload, _ := json.Marshal(metricRequest{Metrics: assignments})
		request, _ := http.NewRequest(http.MethodPost, receiver.Runtime().URL, bytes.NewReader(payload))
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("unexpected metric response %d", response.StatusCode)
		}
	}
	emit("customers.free=347", "customers.pro=92")
	emit("customers.pro=95")

	if missing := receiver.Missing(step); len(missing) != 0 {
		t.Fatalf("required metrics were not satisfied: %v", missing)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 2 {
		t.Fatalf("expected two append-only batches, got %d", len(batches))
	}
	firstMetrics := batches[0]["metrics"].([]any)
	secondMetrics := batches[1]["metrics"].([]any)
	if len(firstMetrics) != 2 || len(secondMetrics) != 1 || secondMetrics[0].(map[string]any)["value"] != float64(95) {
		t.Fatalf("unexpected metric observations: %#v %#v", firstMetrics, secondMetrics)
	}
	if batches[0]["schema_version"] != float64(4) || len(batches[0]["events"].([]any)) != 2 {
		t.Fatalf("metric batch did not include protocol-v4 event projections: %#v", batches[0])
	}
	if spans, ok := batches[0]["spans"].([]any); !ok || len(spans) != 0 {
		t.Fatalf("metric-only batch must encode spans as an empty array: %#v", batches[0]["spans"])
	}
	if level := batches[0]["events"].([]any)[0].(map[string]any)["level"]; level != "info" {
		t.Fatalf("successful metric event must be informational, got %#v", level)
	}
}

func TestMetricMCPAdvertisesSimpleAssignmentTool(t *testing.T) {
	input := strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\"}}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n")
	var output bytes.Buffer
	if err := serveMetricMCP(input, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"serverInfo":{"name":"metrics"`) || !strings.Contains(output.String(), `"name":"emit"`) {
		t.Fatalf("unexpected MCP response: %s", output.String())
	}
	server := metricMCPServer(&metricRuntime{URL: "http://127.0.0.1/metrics"})
	if server.Stdio == nil || server.Stdio.Name != "metrics" {
		t.Fatalf("unexpected ACP MCP server configuration: %#v", server)
	}
	if len(server.Stdio.Env) != 1 || server.Stdio.Env[0].Name != metricEndpointEnvironment || server.Stdio.Env[0].Value != "http://127.0.0.1/metrics" {
		t.Fatalf("metric MCP server should receive only its loopback endpoint: %#v", server.Stdio.Env)
	}
}
