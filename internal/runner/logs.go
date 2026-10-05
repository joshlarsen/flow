package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	flowFlushInterval     = 250 * time.Millisecond
	maxCoalescedTextBytes = 32 * 1024
)

type EventLogger struct {
	writer           *eventWriter
	jobID            string
	harness          string
	stepID           string
	stepIndex        *int
	stepKind         string
	provider         string
	model            string
	maxBytes         int
	exporter         *flowExporter
	redactionSecrets []string
}

func NewEventLogger(out io.Writer, jobID, harness string, maxBytes int) *EventLogger {
	return newEventLogger(newEventWriter(out), jobID, harness, maxBytes)
}

type eventWriter struct {
	out io.Writer
	mu  sync.Mutex
}

func newEventWriter(out io.Writer) *eventWriter {
	return &eventWriter{out: out}
}

func (writer *eventWriter) write(line []byte) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	_, _ = fmt.Fprintln(writer.out, string(line))
}

func newEventLogger(writer *eventWriter, jobID, harness string, maxBytes int) *EventLogger {
	return &EventLogger{writer: writer, jobID: jobID, harness: harness, maxBytes: maxBytes}
}

func (logger *EventLogger) WithStep(step WorkflowStep, index int) *EventLogger {
	logger.stepID = step.ID
	logger.stepIndex = intPointer(index)
	if step.Command != nil {
		logger.stepKind = "command"
	} else {
		logger.stepKind = "harness"
	}
	logger.provider = step.Provider
	logger.model = step.Model
	return logger
}

func (logger *EventLogger) WithRedactionSecret(secret string) *EventLogger {
	if secret != "" {
		logger.redactionSecrets = append(logger.redactionSecrets, secret)
	}
	return logger
}

func (logger *EventLogger) Emit(source, level, eventType string, payload json.RawMessage, sequence int) {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	payload = logger.redactPayload(payload)
	traceID, spanID := logger.export(source, level, eventType, payload, sequence, timestamp)
	envelope := map[string]any{
		"timestamp":        timestamp,
		"level":            level,
		"source":           source,
		"harness":          logger.harness,
		"step_id":          nullableString(logger.stepID),
		"provider":         logger.provider,
		"model":            logger.model,
		"step_index":       logger.stepIndex,
		"step_kind":        nullableString(logger.stepKind),
		"protocol_version": 4,
		"job_id":           logger.jobID,
		"sequence":         sequence,
		"event_type":       eventType,
		"event":            payload,
	}
	if traceID != "" {
		envelope["trace_id"] = traceID
		envelope["span_id"] = spanID
	}
	encoded, err := json.Marshal(envelope)
	if err == nil && len(encoded) <= logger.maxBytes {
		logger.write(encoded)
	} else {
		logger.emitChunks(source, level, eventType, payload, sequence, traceID, spanID)
	}
}

func (logger *EventLogger) Text(source, level, eventType, message string, sequence int) {
	payload, _ := json.Marshal(map[string]string{"message": message})
	logger.Emit(source, level, eventType, payload, sequence)
}

func (logger *EventLogger) emitChunks(source, level, eventType string, payload []byte, sequence int, traceID, spanID string) {
	chunkBytes := ((logger.maxBytes - 2048) * 3) / 4
	if chunkBytes < 256 {
		chunkBytes = 256
	}
	count := (len(payload) + chunkBytes - 1) / chunkBytes
	digest := sha256.Sum256(payload)
	for index := 0; index < count; index++ {
		start := index * chunkBytes
		end := min(start+chunkBytes, len(payload))
		envelope := map[string]any{
			"timestamp":        time.Now().UTC().Format(time.RFC3339Nano),
			"level":            level,
			"source":           source,
			"harness":          logger.harness,
			"step_id":          nullableString(logger.stepID),
			"provider":         logger.provider,
			"model":            logger.model,
			"step_index":       logger.stepIndex,
			"step_kind":        nullableString(logger.stepKind),
			"protocol_version": 4,
			"job_id":           logger.jobID,
			"sequence":         sequence,
			"event_type":       eventType,
			"chunk_index":      index,
			"chunk_count":      count,
			"payload_sha256":   hex.EncodeToString(digest[:]),
			"encoding":         "base64",
			"payload":          base64.StdEncoding.EncodeToString(payload[start:end]),
		}
		if traceID != "" {
			envelope["trace_id"] = traceID
			envelope["span_id"] = spanID
		}
		encoded, _ := json.Marshal(envelope)
		logger.write(encoded)
	}
}

func (logger *EventLogger) export(source, level, eventType string, payload json.RawMessage, sequence int, timestamp string) (string, string) {
	if logger.exporter == nil || strings.HasSuffix(source, "_stderr") {
		return "", ""
	}
	event := logger.exporter.Enqueue(flowEvent{
		EventID: fmt.Sprintf("%s:%s:%d:0", logger.jobID, logger.stepID, sequence), Sequence: sequence,
		ChunkIndex: 0, Type: eventType, Source: source, Level: level,
		StepID: nullableString(logger.stepID), StepIndex: logger.stepIndex, StepKind: nullableString(logger.stepKind),
		ProtocolVersion: 4, Data: payload, OccurredAt: timestamp,
	})
	return event.TraceID, event.SpanID
}

/** Redacts the scoped credential recursively and fails closed on malformed event JSON. */
func (logger *EventLogger) redactPayload(payload json.RawMessage) json.RawMessage {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return json.RawMessage(`{"error":"event payload could not be safely encoded"}`)
	}
	for _, secret := range logger.redactionSecrets {
		value = redactJSONValue(value, secret)
	}
	redacted, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"error":"event payload could not be safely encoded"}`)
	}
	return redacted
}

func redactJSONValue(value any, secret string) any {
	switch typed := value.(type) {
	case string:
		return redactText(typed, secret)
	case []any:
		for index := range typed {
			typed[index] = redactJSONValue(typed[index], secret)
		}
	case map[string]any:
		for key, child := range typed {
			typed[key] = redactJSONValue(child, secret)
		}
	}
	return value
}

func redactText(value, secret string) string {
	if secret == "" {
		return value
	}
	return strings.ReplaceAll(value, secret, "[REDACTED]")
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

/** flowEvent is the stable run history event shape. */
type flowEvent struct {
	EventID         string          `json:"event_id"`
	Sequence        int             `json:"sequence"`
	ChunkIndex      int             `json:"chunk_index"`
	StepID          *string         `json:"step_id"`
	StepIndex       *int            `json:"step_index"`
	StepKind        *string         `json:"step_kind"`
	Type            string          `json:"type"`
	Source          string          `json:"source"`
	Level           string          `json:"level"`
	ProtocolVersion int             `json:"protocol_version"`
	Data            json.RawMessage `json:"data"`
	OccurredAt      string          `json:"occurred_at"`
	TraceID         string          `json:"trace_id"`
	SpanID          string          `json:"span_id"`
}

type flowSpan struct {
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID *string        `json:"parent_span_id"`
	Name         string         `json:"name"`
	Kind         string         `json:"kind"`
	StartedAt    string         `json:"started_at"`
	FinishedAt   string         `json:"finished_at"`
	Status       flowSpanStatus `json:"status"`
	Attributes   map[string]any `json:"attributes"`
}

type flowSpanStatus struct {
	Code string `json:"code"`
}

type flowExportItem struct {
	event *flowEvent
	span  *flowSpan
}

type flowExporter struct {
	failureMu   sync.Mutex
	failure     error
	callbackURL string
	token       string
	jobID       string
	harness     string
	createdAt   string
	startedAt   string
	traceID     string
	rootSpanID  string
	items       chan flowExportItem
	done        chan struct{}
	write       func([]byte)
	flushEvery  time.Duration
	maxBytes    int
	trace       *flowTraceBuilder
}

func newFlowExporter(callbackURL, token, jobID, harness, createdAt, startedAt, traceID, rootSpanID string, write func([]byte), maxBytes int) *flowExporter {
	return newFlowExporterWithInterval(callbackURL, token, jobID, harness, createdAt, startedAt, traceID, rootSpanID, write, flowFlushInterval, maxBytes)
}

func newFlowExporterWithInterval(callbackURL, token, jobID, harness, createdAt, startedAt, traceID, rootSpanID string, write func([]byte), flushEvery time.Duration, maxBytes int) *flowExporter {
	exporter := &flowExporter{
		callbackURL: callbackURL, token: token, jobID: jobID, harness: harness,
		createdAt: createdAt, startedAt: startedAt, traceID: traceID, rootSpanID: rootSpanID,
		items: make(chan flowExportItem, 1000), done: make(chan struct{}),
		write: write, flushEvery: flushEvery, maxBytes: maxBytes,
	}
	exporter.trace = newFlowTraceBuilder(traceID, rootSpanID)
	go exporter.run()
	return exporter
}

/** Normalizes complete ACP payloads before placing bounded events on the export queue. */
func (exporter *flowExporter) Enqueue(event flowEvent) flowEvent {
	event.ProtocolVersion = 4
	if event.Source == "acp" {
		normalized, err := normalizeJSONKeys(event.Data)
		if err != nil {
			exporter.log("error", "flow_normalize_failed", "ACP event was not exported because its keys could not be normalized")
			return event
		}
		event.Data = normalized
	}
	spans := exporter.trace.decorate(&event)
	if encoded, err := json.Marshal(event); err == nil && len(encoded) <= exporter.maxBytes {
		exporter.enqueueEvent(event)
	} else {
		exporter.enqueueChunks(event)
	}
	for index := range spans {
		exporter.enqueueSpan(spans[index])
	}
	return event
}

func (exporter *flowExporter) enqueueEvent(event flowEvent) {
	if exporter.Error() == nil {
		exporter.items <- flowExportItem{event: &event}
	}
}

func (exporter *flowExporter) enqueueSpan(span flowSpan) {
	if exporter.Error() == nil {
		exporter.items <- flowExportItem{span: &span}
	}
}

/** Chunks an already-normalized payload without exposing native ACP casing in Flow. */
func (exporter *flowExporter) enqueueChunks(event flowEvent) {
	chunkBytes := ((exporter.maxBytes - 2048) * 3) / 4
	if chunkBytes < 256 {
		chunkBytes = 256
	}
	count := (len(event.Data) + chunkBytes - 1) / chunkBytes
	digest := sha256.Sum256(event.Data)
	prefix := strings.TrimSuffix(event.EventID, fmt.Sprintf(":%d", event.ChunkIndex))
	for index := 0; index < count; index++ {
		start := index * chunkBytes
		end := min(start+chunkBytes, len(event.Data))
		chunkPayload, _ := json.Marshal(map[string]any{
			"chunk_count": count, "payload_sha256": hex.EncodeToString(digest[:]),
			"encoding": "base64", "payload": base64.StdEncoding.EncodeToString(event.Data[start:end]),
		})
		chunk := event
		chunk.EventID = fmt.Sprintf("%s:%d", prefix, index)
		chunk.ChunkIndex = index
		chunk.Data = chunkPayload
		exporter.enqueueEvent(chunk)
	}
}

/** Recursively converts JSON object keys to snake case while preserving ACP's reserved _meta key. */
func normalizeJSONKeys(payload json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, fmt.Errorf("payload contains trailing JSON data")
	}
	normalized, err := normalizeJSONValue(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}

/** Rebuilds nested JSON containers and rejects keys that collide after normalization. */
func normalizeJSONValue(value any) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(typed))
		for key, child := range typed {
			normalizedKey := snakeCaseJSONKey(key)
			if normalizedKey == "" {
				return nil, fmt.Errorf("JSON key normalizes to an empty string")
			}
			if _, exists := normalized[normalizedKey]; exists {
				return nil, fmt.Errorf("JSON keys collide after normalization")
			}
			normalizedChild, err := normalizeJSONValue(child)
			if err != nil {
				return nil, err
			}
			normalized[normalizedKey] = normalizedChild
		}
		return normalized, nil
	case []any:
		normalized := make([]any, len(typed))
		for index, child := range typed {
			normalizedChild, err := normalizeJSONValue(child)
			if err != nil {
				return nil, err
			}
			normalized[index] = normalizedChild
		}
		return normalized, nil
	default:
		return value, nil
	}
}

/** Converts camel case, acronym boundaries, and punctuation to lowercase snake case. */
func snakeCaseJSONKey(key string) string {
	if key == "_meta" {
		return key
	}
	runes := []rune(key)
	var result strings.Builder
	lastUnderscore := false
	for index, current := range runes {
		if !unicode.IsLetter(current) && !unicode.IsDigit(current) {
			if result.Len() > 0 && !lastUnderscore {
				result.WriteByte('_')
				lastUnderscore = true
			}
			continue
		}
		if unicode.IsUpper(current) && result.Len() > 0 && !lastUnderscore {
			previous := runes[index-1]
			nextIsLower := index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && nextIsLower) {
				result.WriteByte('_')
			}
		}
		result.WriteRune(unicode.ToLower(current))
		lastUnderscore = false
	}
	return strings.Trim(result.String(), "_")
}

func (exporter *flowExporter) Close() {
	for _, span := range exporter.trace.closeIncomplete(time.Now().UTC().Format(time.RFC3339Nano), "UNSET", "exporter_closed") {
		exporter.enqueueSpan(span)
	}
	close(exporter.items)
	<-exporter.done
}

func (exporter *flowExporter) run() {
	defer close(exporter.done)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	timerActive := false
	batch := make([]flowEvent, 0, 50)
	spans := make([]flowSpan, 0, 16)
	batchBytes := 0
	metrics := newCoalesceMetrics()
	var pending *coalescedStream
	startTimer := func() {
		if timerActive {
			return
		}
		timer.Reset(exporter.flushEvery)
		timerActive = true
	}
	flushBatch := func() {
		if len(batch) == 0 && len(spans) == 0 {
			return
		}
		if exporter.Error() == nil {
			if err := exporter.deliver(batch, spans, nil); err != nil {
				exporter.failureMu.Lock()
				exporter.failure = err
				exporter.failureMu.Unlock()
				exporter.log("error", "flow_export_failed", fmt.Sprintf("failed to persist %d events and %d spans after retries: %s", len(batch), len(spans), err))
			}
		}
		batch = batch[:0]
		spans = spans[:0]
		batchBytes = 0
	}
	appendSpan := func(span flowSpan) {
		spanBytes, _ := json.Marshal(span)
		if len(batch)+len(spans) > 0 && (batchBytes+len(spanBytes) > 768*1024 || len(batch)+len(spans) == 50) {
			flushBatch()
		}
		spans = append(spans, span)
		batchBytes += len(spanBytes)
		startTimer()
	}
	appendEvent := func(event flowEvent) {
		eventBytes, _ := json.Marshal(event)
		if len(batch) > 0 && (batchBytes+len(eventBytes) > 768*1024 || len(batch) == 50) {
			flushBatch()
		}
		batch = append(batch, event)
		batchBytes += len(eventBytes)
		startTimer()
	}
	flushPending := func(reason string) {
		if pending == nil {
			return
		}
		event := pending.event()
		appendEvent(event)
		metrics.record(*pending, len(event.Data), reason)
		exporter.logEvent("info", "flow_coalesce_flush", pending.audit(len(event.Data), reason))
		pending = nil
	}
	defer func() {
		exporter.logEvent("info", "flow_coalesce_summary", metrics.summary(exporter.flushEvery))
	}()
	for {
		select {
		case item, ok := <-exporter.items:
			if !ok {
				flushPending("close")
				flushBatch()
				return
			}
			if item.span != nil {
				flushPending("span")
				appendSpan(*item.span)
				continue
			}
			event := *item.event
			chunk, candidate, reason := parseStreamChunk(event)
			if candidate && chunk == nil {
				flushPending("boundary")
				exporter.logEvent("warn", "flow_coalesce_bypass", map[string]any{
					"sequence": event.Sequence, "reason": reason,
				})
				appendEvent(event)
				continue
			}
			if chunk == nil {
				flushPending("boundary")
				appendEvent(event)
				continue
			}
			metrics.observe(*chunk)
			if pending != nil && (!pending.compatible(*chunk) || pending.textBytes+chunk.textBytes > maxCoalescedTextBytes) {
				flushReason := "boundary"
				if pending.compatible(*chunk) {
					flushReason = "max_bytes"
				}
				flushPending(flushReason)
			}
			if pending == nil {
				pending = chunk
				startTimer()
			} else {
				pending.append(*chunk)
			}
		case <-timer.C:
			timerActive = false
			flushPending("timer")
			flushBatch()
		}
	}
}

type coalescedStream struct {
	first          flowEvent
	payload        map[string]any
	update         map[string]any
	content        map[string]any
	identity       string
	streamKind     string
	sessionID      string
	messageID      string
	chunkCount     int
	textBytes      int
	lastSequence   int
	lastOccurredAt string
	started        time.Time
}

func parseStreamChunk(event flowEvent) (*coalescedStream, bool, string) {
	if event.Source != "acp" || event.Type != "session/update" {
		return nil, false, ""
	}
	var payload map[string]any
	if err := json.Unmarshal(event.Data, &payload); err != nil {
		return nil, true, "invalid_json"
	}
	params, paramsOK := payload["params"].(map[string]any)
	update, updateOK := params["update"].(map[string]any)
	content, contentOK := update["content"].(map[string]any)
	kind, _ := update["session_update"].(string)
	if kind != "agent_message_chunk" && kind != "agent_thought_chunk" {
		return nil, false, ""
	}
	sessionID, _ := params["session_id"].(string)
	text, textOK := content["text"].(string)
	_, hasMeta := update["_meta"]
	_, metaOK := update["_meta"].(map[string]any)
	if !paramsOK || !updateOK || !contentOK || payload["jsonrpc"] != "2.0" || payload["method"] != "session/update" || sessionID == "" || content["type"] != "text" || !textOK || (hasMeta && update["_meta"] != nil && !metaOK) {
		return nil, true, "unsupported_shape"
	}
	messageID := ""
	if value, ok := update["message_id"].(string); ok {
		messageID = value
	}
	content["text"] = ""
	identityBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, true, "invalid_shape"
	}
	content["text"] = text
	return &coalescedStream{
		first: event, payload: payload, update: update, content: content,
		identity: string(identityBytes), streamKind: kind,
		sessionID: sessionID, messageID: messageID, chunkCount: 1,
		textBytes: len([]byte(text)), lastSequence: event.Sequence,
		lastOccurredAt: event.OccurredAt, started: time.Now(),
	}, true, ""
}

func (stream *coalescedStream) compatible(next coalescedStream) bool {
	return stream.identity == next.identity
}

func (stream *coalescedStream) append(next coalescedStream) {
	stream.content["text"] = stream.content["text"].(string) + next.content["text"].(string)
	stream.chunkCount++
	stream.textBytes += next.textBytes
	stream.lastSequence = next.lastSequence
	stream.lastOccurredAt = next.lastOccurredAt
}

func (stream *coalescedStream) event() flowEvent {
	metadata, _ := stream.update["_meta"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		stream.update["_meta"] = metadata
	}
	metadata["flow_coalescing"] = map[string]any{
		"version": 1, "chunk_count": stream.chunkCount,
		"first_sequence": stream.first.Sequence, "last_sequence": stream.lastSequence,
		"first_occurred_at": stream.first.OccurredAt, "last_occurred_at": stream.lastOccurredAt,
	}
	data, _ := json.Marshal(stream.payload)
	event := stream.first
	prefix := strings.TrimSuffix(stream.first.EventID, fmt.Sprintf(":%d:%d", stream.first.Sequence, stream.first.ChunkIndex))
	event.EventID = fmt.Sprintf("%s:%d-%d:0", prefix, stream.first.Sequence, stream.lastSequence)
	event.Data = data
	return event
}

func (stream *coalescedStream) audit(outputBytes int, reason string) map[string]any {
	return map[string]any{
		"stream_kind": stream.streamKind, "session_id": stream.sessionID,
		"message_id": stream.messageID, "input_events": stream.chunkCount,
		"input_text_bytes": stream.textBytes, "output_payload_bytes": outputBytes,
		"first_sequence": stream.first.Sequence, "last_sequence": stream.lastSequence,
		"first_occurred_at": stream.first.OccurredAt, "last_occurred_at": stream.lastOccurredAt,
		"coalesce_ms": time.Since(stream.started).Milliseconds(), "flush_reason": reason,
	}
}

type coalesceKindMetrics struct {
	RawChunks        int `json:"raw_chunks"`
	ExportedSegments int `json:"exported_segments"`
	TextBytes        int `json:"text_bytes"`
}

type coalesceMetrics struct {
	RawChunks        int
	ExportedSegments int
	TextBytes        int
	PayloadBytes     int
	MaxLatencyMS     int64
	ByKind           map[string]*coalesceKindMetrics
	FlushReasons     map[string]int
}

func newCoalesceMetrics() *coalesceMetrics {
	return &coalesceMetrics{ByKind: map[string]*coalesceKindMetrics{}, FlushReasons: map[string]int{}}
}

func (metrics *coalesceMetrics) observe(stream coalescedStream) {
	metrics.RawChunks++
	metrics.TextBytes += stream.textBytes
	kind := metrics.ByKind[stream.streamKind]
	if kind == nil {
		kind = &coalesceKindMetrics{}
		metrics.ByKind[stream.streamKind] = kind
	}
	kind.RawChunks++
	kind.TextBytes += stream.textBytes
}

func (metrics *coalesceMetrics) record(stream coalescedStream, payloadBytes int, reason string) {
	metrics.ExportedSegments++
	metrics.PayloadBytes += payloadBytes
	latency := time.Since(stream.started).Milliseconds()
	metrics.MaxLatencyMS = max(metrics.MaxLatencyMS, latency)
	metrics.FlushReasons[reason]++
	metrics.ByKind[stream.streamKind].ExportedSegments++
}

func (metrics *coalesceMetrics) summary(flushEvery time.Duration) map[string]any {
	reduction := 0.0
	if metrics.RawChunks > 0 {
		reduction = 1 - float64(metrics.ExportedSegments)/float64(metrics.RawChunks)
	}
	return map[string]any{
		"coalescing_version": 1, "flush_interval_ms": flushEvery.Milliseconds(),
		"max_segment_text_bytes": maxCoalescedTextBytes,
		"raw_stream_events":      metrics.RawChunks, "exported_stream_segments": metrics.ExportedSegments,
		"event_reduction_ratio": reduction, "input_text_bytes": metrics.TextBytes,
		"exported_payload_bytes": metrics.PayloadBytes, "max_coalesce_ms": metrics.MaxLatencyMS,
		"by_kind": metrics.ByKind, "flush_reasons": metrics.FlushReasons,
	}
}

func (exporter *flowExporter) deliver(events []flowEvent, spans []flowSpan, metrics []flowMetric) error {
	if events == nil {
		events = []flowEvent{}
	}
	if spans == nil {
		spans = []flowSpan{}
	}
	if metrics == nil {
		metrics = []flowMetric{}
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version": 4,
		"run": map[string]any{
			"source_run_id": exporter.jobID, "harness": exporter.harness, "status": "running",
			"created_at": exporter.createdAt, "started_at": exporter.startedAt, "finished_at": nil,
			"trace_id": exporter.traceID, "root_span_id": exporter.rootSpanID,
		},
		"events": events, "spans": spans, "metrics": metrics,
	})
	if err != nil {
		return fmt.Errorf("encode callback payload: %w", err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	var lastError error
	retryDeadline := time.Now().Add(2 * time.Minute)
	for attempt := 0; time.Now().Before(retryDeadline); attempt++ {
		request, requestErr := http.NewRequest(http.MethodPost, exporter.callbackURL, bytes.NewReader(payload))
		if requestErr != nil {
			return fmt.Errorf("create callback request: %w", requestErr)
		}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("authorization", "Bearer "+exporter.token)
		response, requestErr := client.Do(request)
		if requestErr != nil {
			lastError = requestErr
		} else {
			responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
			if response.StatusCode == http.StatusAccepted {
				return nil
			}
			lastError = fmt.Errorf("callback returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
			if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != 429 {
				return lastError
			}
		}
		time.Sleep(time.Duration(1<<min(attempt, 4)) * 100 * time.Millisecond)
	}
	return lastError
}

func (exporter *flowExporter) log(level, event, message string) {
	exporter.logEvent(level, event, map[string]any{"message": message})
}

func (exporter *flowExporter) logEvent(level, event string, data map[string]any) {
	entry, _ := json.Marshal(map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "level": level,
		"source": "runner", "job_id": exporter.jobID, "event_type": event,
		"event": data,
	})
	exporter.write(entry)
}

func (logger *EventLogger) write(line []byte) {
	logger.writer.write(line)
}

func (exporter *flowExporter) Error() error {
	exporter.failureMu.Lock()
	defer exporter.failureMu.Unlock()
	return exporter.failure
}
