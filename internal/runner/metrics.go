package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	acp "github.com/coder/acp-go-sdk"
)

const (
	metricEndpointEnvironment = "FLOW_METRIC_URL"
	maxMetricsPerRequest      = 100
)

type flowMetric struct {
	MetricID        string  `json:"metric_id"`
	Sequence        int     `json:"sequence"`
	StepID          string  `json:"step_id"`
	StepIndex       int     `json:"step_index"`
	Namespace       string  `json:"namespace"`
	Key             string  `json:"key"`
	Value           float64 `json:"value"`
	ProtocolVersion int     `json:"protocol_version"`
	OccurredAt      string  `json:"occurred_at"`
	TraceID         string  `json:"trace_id"`
	SpanID          string  `json:"span_id"`
}

type metricRuntime struct {
	URL string
}

type metricReceiver struct {
	exporter   *flowExporter
	jobID      string
	traceID    string
	spanID     string
	sequence   *eventSequence
	server     *http.Server
	listener   net.Listener
	mu         sync.Mutex
	stepID     string
	stepIndex  int
	stepKind   string
	seenByStep map[string]map[string]bool
}

type metricRequest struct {
	Metrics []string `json:"metrics"`
}

/** Starts the loopback-only endpoint shared by the CLI and MCP metric surfaces. */
func startMetricReceiver(exporter *flowExporter, jobID, traceID, spanID string, sequence *eventSequence) (*metricReceiver, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for metrics: %w", err)
	}
	receiver := &metricReceiver{
		exporter: exporter, jobID: jobID, traceID: traceID, spanID: spanID,
		sequence: sequence, listener: listener,
		seenByStep: map[string]map[string]bool{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /metrics", receiver.handle)
	receiver.server = &http.Server{
		Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 35 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 * 1024,
	}
	go func() { _ = receiver.server.Serve(listener) }()
	return receiver, nil
}

func (receiver *metricReceiver) Runtime() *metricRuntime {
	return &metricRuntime{URL: "http://" + receiver.listener.Addr().String() + "/metrics"}
}

func (receiver *metricReceiver) SetStep(step WorkflowStep, index int, restored []string) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	receiver.stepID, receiver.stepIndex = step.ID, index
	receiver.stepKind = "harness"
	if step.Command != nil {
		receiver.stepKind = "command"
	}
	seen := receiver.seenByStep[step.ID]
	if seen == nil {
		seen = map[string]bool{}
		receiver.seenByStep[step.ID] = seen
	}
	for _, name := range restored {
		seen[name] = true
	}
}

func (receiver *metricReceiver) Seen(stepID string) []string {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	names := make([]string, 0, len(receiver.seenByStep[stepID]))
	for name := range receiver.seenByStep[stepID] {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (receiver *metricReceiver) Missing(step WorkflowStep) []string {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	seen := receiver.seenByStep[step.ID]
	missing := make([]string, 0)
	for _, metric := range step.RequiredMetrics {
		name := metric.Name()
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	return missing
}

func (receiver *metricReceiver) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = receiver.server.Shutdown(ctx)
}

/** Validates and durably hands metric points to the Worker's event outbox before acknowledging. */
func (receiver *metricReceiver) handle(response http.ResponseWriter, request *http.Request) {
	receiver.mu.Lock()
	stepID, stepIndex, stepKind := receiver.stepID, receiver.stepIndex, receiver.stepKind
	receiver.mu.Unlock()
	request.Body = http.MaxBytesReader(response, request.Body, 64*1024)
	var input metricRequest
	if err := decodeOne(request.Body, &input); err != nil || len(input.Metrics) < 1 || len(input.Metrics) > maxMetricsPerRequest {
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": "metrics must contain between 1 and 100 assignments"})
		return
	}
	if stepID == "" {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "no workflow step is active"})
		return
	}
	points := make([]flowMetric, 0, len(input.Metrics))
	events := make([]flowEvent, 0, len(input.Metrics))
	for _, assignment := range input.Metrics {
		namespace, key, value, err := parseMetricAssignment(assignment)
		if err != nil {
			writeJSON(response, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		sequence := receiver.sequence.Next()
		occurredAt := time.Now().UTC().Format(time.RFC3339Nano)
		metricID := fmt.Sprintf("%s:%s:%d", receiver.jobID, stepID, sequence)
		point := flowMetric{
			MetricID: metricID, Sequence: sequence, StepID: stepID, StepIndex: stepIndex,
			Namespace: namespace, Key: key, Value: value, ProtocolVersion: 4,
			OccurredAt: occurredAt, TraceID: receiver.traceID, SpanID: receiver.spanID,
		}
		data, _ := json.Marshal(map[string]any{"name": namespace + "." + key, "namespace": namespace, "key": key, "value": value})
		events = append(events, flowEvent{
			EventID: metricID + ":0", Sequence: sequence, ChunkIndex: 0, StepID: &stepID, StepIndex: &stepIndex,
			StepKind: &stepKind, Type: "metric.recorded", Source: "runner", Level: "info",
			ProtocolVersion: 4, Data: data, OccurredAt: occurredAt, TraceID: receiver.traceID, SpanID: receiver.spanID,
		})
		points = append(points, point)
	}
	if receiver.exporter == nil {
		writeJSON(response, http.StatusServiceUnavailable, map[string]any{"error": "history callback is not configured"})
		return
	}
	spanID := receiver.exporter.trace.metricSpanID()
	for index := range points {
		points[index].SpanID = spanID
		events[index].SpanID = spanID
	}
	if err := receiver.exporter.deliver(events, nil, points); err != nil {
		writeJSON(response, http.StatusBadGateway, map[string]any{"error": "metric delivery failed"})
		return
	}
	receiver.mu.Lock()
	seen := receiver.seenByStep[stepID]
	for _, point := range points {
		seen[point.Namespace+"."+point.Key] = true
	}
	receiver.mu.Unlock()
	writeJSON(response, http.StatusAccepted, map[string]any{"accepted": len(points)})
}

func parseMetricAssignment(assignment string) (string, string, float64, error) {
	name, rawValue, found := strings.Cut(strings.TrimSpace(assignment), "=")
	if !found || strings.Count(name, ".") != 1 {
		return "", "", 0, fmt.Errorf("metric %q must use namespace.key=value", assignment)
	}
	namespace, key, _ := strings.Cut(name, ".")
	if !metricPartPattern.MatchString(namespace) || !metricPartPattern.MatchString(key) {
		return "", "", 0, fmt.Errorf("metric name %q is invalid", name)
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(rawValue), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return "", "", 0, fmt.Errorf("metric %q has an invalid finite number", name)
	}
	return namespace, key, value, nil
}

/** Builds the deterministic MCP instructions appended to a step prompt. */
func requiredMetricInstructions(metrics []RequiredMetric) string {
	if len(metrics) == 0 {
		return ""
	}
	lines := []string{
		"", "", "## Required metrics", "",
		"Before completing this step, call the `metrics` MCP server's `emit` tool (`mcp.metrics.emit`). Pass a `metrics` array containing `namespace.key=value` strings with finite numeric values.",
		"", "Emit all of these required metrics:", "",
	}
	assignments := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		name := metric.Name()
		lines = append(lines, "- `"+name+"`: "+metric.Description)
		assignments = append(assignments, name+"=<number>")
	}
	example, _ := json.Marshal(metricRequest{Metrics: assignments})
	exampleText := strings.ReplaceAll(strings.ReplaceAll(string(example), `\u003c`, "<"), `\u003e`, ">")
	lines = append(lines, "", "Example arguments:", "```json", exampleText, "```")
	return strings.Join(lines, "\n")
}

/** Appends required-metric instructions while preserving the configured prompt limit. */
func promptWithRequiredMetrics(prompt string, metrics []RequiredMetric, maxBytes int) (string, error) {
	combined := prompt + requiredMetricInstructions(metrics)
	if len([]byte(combined)) > maxBytes {
		return "", fmt.Errorf("prompt plus required metric instructions exceeds max_prompt_bytes")
	}
	return combined, nil
}

func emitMetricCLI(assignments []string) error {
	endpoint := os.Getenv(metricEndpointEnvironment)
	if endpoint == "" {
		return fmt.Errorf("metric emission is unavailable outside an active workflow step")
	}
	payload, _ := json.Marshal(metricRequest{Metrics: assignments})
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("content-type", "application/json")
	response, err := (&http.Client{Timeout: 35 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusAccepted {
		return fmt.Errorf("metric endpoint returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

/** Serves the small MCP surface used by every ACP harness for metric emission. */
func serveMetricMCP(input io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var request mcpRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			continue
		}
		if len(request.ID) == 0 {
			continue
		}
		response := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(request.ID)}
		switch request.Method {
		case "initialize":
			var params struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			_ = json.Unmarshal(request.Params, &params)
			if params.ProtocolVersion == "" {
				params.ProtocolVersion = "2025-03-26"
			}
			response["result"] = map[string]any{
				"protocolVersion": params.ProtocolVersion, "capabilities": map[string]any{"tools": map[string]any{}},
				"serverInfo": map[string]string{"name": "metrics", "version": "1.0.0"},
			}
		case "ping":
			response["result"] = map[string]any{}
		case "tools/list":
			response["result"] = map[string]any{"tools": []any{map[string]any{
				"name": "emit", "description": "Persist one or more numeric metrics using namespace.key=value assignments.",
				"inputSchema": map[string]any{"type": "object", "additionalProperties": false, "required": []string{"metrics"}, "properties": map[string]any{
					"metrics": map[string]any{"type": "array", "minItems": 1, "maxItems": maxMetricsPerRequest, "items": map[string]string{"type": "string"}},
				}},
			}}}
		case "tools/call":
			var params struct {
				Name      string        `json:"name"`
				Arguments metricRequest `json:"arguments"`
			}
			err := json.Unmarshal(request.Params, &params)
			if err == nil && params.Name == "emit" {
				err = emitMetricCLI(params.Arguments.Metrics)
			} else if err == nil {
				err = fmt.Errorf("unknown tool %q", params.Name)
			}
			if err != nil {
				response["result"] = map[string]any{"isError": true, "content": []any{map[string]string{"type": "text", "text": err.Error()}}}
			} else {
				response["result"] = map[string]any{"content": []any{map[string]string{"type": "text", "text": fmt.Sprintf("Persisted %d metric(s).", len(params.Arguments.Metrics))}}}
			}
		default:
			response["error"] = map[string]any{"code": -32601, "message": "Method not found"}
		}
		if err := encoder.Encode(response); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func metricMCPServer(runtime *metricRuntime) acp.McpServer {
	return acp.McpServer{Stdio: &acp.McpServerStdio{
		Name: "metrics", Command: "/usr/local/bin/agent-runner", Args: []string{"metric-mcp"},
		Env: []acp.EnvVariable{{Name: metricEndpointEnvironment, Value: runtime.URL}},
	}}
}
