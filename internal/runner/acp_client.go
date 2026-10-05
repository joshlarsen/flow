package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	acp "github.com/coder/acp-go-sdk"
)

type ACPUsage struct {
	TotalTokens       *int `json:"total_tokens,omitempty"`
	TotalInputTokens  *int `json:"total_input_tokens,omitempty"`
	InputTokens       *int `json:"input_tokens,omitempty"`
	OutputTokens      *int `json:"output_tokens,omitempty"`
	ThoughtTokens     *int `json:"thought_tokens,omitempty"`
	CachedReadTokens  *int `json:"cached_read_tokens,omitempty"`
	CachedWriteTokens *int `json:"cached_write_tokens,omitempty"`
	Used              *int `json:"used,omitempty"`
	Size              *int `json:"size,omitempty"`
}

type acpState struct {
	messageID            string
	message              strings.Builder
	usage                *ACPUsage
	harnessDiagnostic    string
	terminalHarnessError bool
	mu                   sync.Mutex
}

type runnerACPClient struct {
	runner    *Runner
	state     *acpState
	broker    InteractionBroker
	inspector harnessSessionUpdateInspector
}

var _ acp.Client = (*runnerACPClient)(nil)
var _ acp.ExtensionMethodHandler = (*runnerACPClient)(nil)

const (
	codexAuthStatusUpdateMethod     = "_auth/status_update"
	grokMCPInitProgressMethod       = "_x.ai/mcp/init_progress"
	grokMCPServersUpdatedMethod     = "_x.ai/mcp/servers_updated"
	grokMCPServerStatusMethod       = "_x.ai/mcp/server_status"
	grokMCPInitializedMethod        = "_x.ai/mcp_initialized"
	grokSessionsChangedMethod       = "_x.ai/sessions/changed"
	grokQueueChangedMethod          = "_x.ai/queue/changed"
	grokSessionNotificationMethod   = "_x.ai/session_notification"
	grokSessionPromptCompleteMethod = "_x.ai/session/prompt_complete"
)

func (client *runnerACPClient) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	assessment := harnessSessionUpdateAssessment{}
	if client.inspector != nil {
		assessment = client.inspector.AssessSessionUpdate(params.Update)
	}
	client.runner.logACP("session/update", "agent_to_client", "notification", params, sessionUpdateLevel(params.Update, assessment))
	client.state.mu.Lock()
	defer client.state.mu.Unlock()
	update := params.Update
	if assessment.Diagnostic != "" {
		client.state.harnessDiagnostic = truncateText(redactText(assessment.Diagnostic, client.runner.proxyToken), 4096)
	}
	if assessment.Terminal {
		client.state.terminalHarnessError = true
	}
	if chunk := update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
		if chunk.MessageId != nil && client.state.messageID != "" && client.state.messageID != *chunk.MessageId {
			client.state.message.Reset()
		}
		if chunk.MessageId != nil {
			client.state.messageID = *chunk.MessageId
		}
		client.state.message.WriteString(chunk.Content.Text.Text)
	}
	if usage := update.UsageUpdate; usage != nil {
		client.state.ensureUsage().Used = intPointer(usage.Used)
		client.state.usage.Size = intPointer(usage.Size)
	}
	return nil
}

/** Marks explicit ACP and harness failures as errors without failing recoverable retries. */
func sessionUpdateLevel(update acp.SessionUpdate, assessment harnessSessionUpdateAssessment) string {
	if assessment.Error {
		return "error"
	}
	if update.ToolCall != nil && update.ToolCall.Status == acp.ToolCallStatusFailed {
		return "error"
	}
	if update.ToolCallUpdate != nil && update.ToolCallUpdate.Status != nil && *update.ToolCallUpdate.Status == acp.ToolCallStatusFailed {
		return "error"
	}
	return "info"
}

func (state *acpState) ensureUsage() *ACPUsage {
	if state.usage == nil {
		state.usage = &ACPUsage{}
	}
	return state.usage
}

func (state *acpState) capturePromptUsage(response acp.PromptResponse) {
	state.mu.Lock()
	defer state.mu.Unlock()
	usage := response.Usage
	if usage != nil {
		target := state.ensureUsage()
		target.TotalTokens = intPointer(usage.TotalTokens)
		target.InputTokens = intPointer(usage.InputTokens)
		target.OutputTokens = intPointer(usage.OutputTokens)
		target.ThoughtTokens = copyIntPointer(usage.ThoughtTokens)
		target.CachedReadTokens = copyIntPointer(usage.CachedReadTokens)
		target.CachedWriteTokens = copyIntPointer(usage.CachedWriteTokens)
		setTotalInputTokens(target)
		return
	}
	if metadataUsage := usageFromMetadata(response.Meta); metadataUsage != nil {
		target := state.ensureUsage()
		target.TotalTokens = metadataUsage.TotalTokens
		target.InputTokens = metadataUsage.InputTokens
		target.OutputTokens = metadataUsage.OutputTokens
		target.ThoughtTokens = metadataUsage.ThoughtTokens
		target.CachedReadTokens = metadataUsage.CachedReadTokens
		target.CachedWriteTokens = metadataUsage.CachedWriteTokens
		setTotalInputTokens(target)
	}
}

func usageFromMetadata(meta map[string]any) *ACPUsage {
	if len(meta) == 0 {
		return nil
	}
	source := meta
	if nested, ok := meta["usage"].(map[string]any); ok {
		source = nested
	}
	total, totalOK := nonNegativeInteger(source["totalTokens"])
	input, inputOK := nonNegativeInteger(source["inputTokens"])
	output, outputOK := nonNegativeInteger(source["outputTokens"])
	if !totalOK || !inputOK || !outputOK {
		return nil
	}
	result := &ACPUsage{
		TotalTokens:  intPointer(total),
		InputTokens:  intPointer(input),
		OutputTokens: intPointer(output),
	}
	result.CachedReadTokens = optionalMetadataInteger(source, "cachedReadTokens")
	result.CachedWriteTokens = firstMetadataInteger(source, "cachedWriteTokens", "cacheCreationTokens")
	result.ThoughtTokens = firstMetadataInteger(source, "thoughtTokens", "reasoningTokens", "reasoningOutputTokens")

	// Some provider metadata reports input tokens inclusive of cache hits and
	// writes. ACP reports uncached input separately, so normalize only when the
	// provider's own total proves that its input count is inclusive.
	if total == input+output {
		cached := pointerValue(result.CachedReadTokens) + pointerValue(result.CachedWriteTokens)
		if cached <= input {
			result.InputTokens = intPointer(input - cached)
		}
	}
	setTotalInputTokens(result)
	return result
}

/** Derives inclusive input from the mutually exclusive input and cache buckets. */
func setTotalInputTokens(usage *ACPUsage) {
	usage.TotalInputTokens = nil
	if usage.InputTokens == nil {
		return
	}
	total := *usage.InputTokens + pointerValue(usage.CachedReadTokens) + pointerValue(usage.CachedWriteTokens)
	usage.TotalInputTokens = intPointer(total)
}

func optionalMetadataInteger(source map[string]any, key string) *int {
	value, ok := nonNegativeInteger(source[key])
	if !ok {
		return nil
	}
	return intPointer(value)
}

func firstMetadataInteger(source map[string]any, keys ...string) *int {
	for _, key := range keys {
		if value := optionalMetadataInteger(source, key); value != nil {
			return value
		}
	}
	return nil
}

func nonNegativeInteger(value any) (int, bool) {
	switch number := value.(type) {
	case int:
		return number, number >= 0
	case float64:
		converted := int(number)
		return converted, number >= 0 && number == float64(converted)
	default:
		return 0, false
	}
}

func intPointer(value int) *int {
	return &value
}

func copyIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	return intPointer(*value)
}

func pointerValue(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func (client *runnerACPClient) RequestPermission(ctx context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	client.runner.logACP("session/request_permission", "agent_to_client", "request", params, "info")
	payload, _ := json.Marshal(params)
	decision, err := client.broker.Resolve(ctx, InteractionRequest{Kind: InteractionPermission, SessionID: string(params.SessionId), Payload: payload})
	if err != nil {
		client.runner.logACP("session/request_permission", "client_to_agent", "response", map[string]any{"error": err.Error()}, "error")
		return acp.RequestPermissionResponse{}, err
	}
	if decision.Action == InteractionApprove {
		optionID, ok := selectAllowOption(params.Options)
		if !ok {
			err := fmt.Errorf("permission request offered no allow option")
			client.runner.logACP("session/request_permission", "client_to_agent", "response", map[string]any{"error": err.Error()}, "error")
			return acp.RequestPermissionResponse{}, err
		}
		response := acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Selected: &acp.RequestPermissionOutcomeSelected{OptionId: optionID}}}
		client.runner.logACP("session/request_permission", "client_to_agent", "response", response, "info")
		return response, nil
	}
	response := acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}}
	client.runner.logACP("session/request_permission", "client_to_agent", "response", response, "info")
	return response, nil
}

func (client *runnerACPClient) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, error) {
	if method == codexAuthStatusUpdateMethod {
		client.runner.logACP(method, "agent_to_client", "notification", json.RawMessage(params), "info")
		return nil, nil
	}
	switch method {
	case grokMCPInitProgressMethod,
		grokMCPServersUpdatedMethod,
		grokMCPServerStatusMethod,
		grokMCPInitializedMethod,
		grokSessionsChangedMethod,
		grokQueueChangedMethod,
		grokSessionNotificationMethod,
		grokSessionPromptCompleteMethod:
		return nil, nil
	}
	if method != "x.ai/ask_user_question" && method != "_x.ai/ask_user_question" {
		client.runner.logACP(method, "agent_to_client", "request", json.RawMessage(params), "info")
	}
	switch method {
	case "x.ai/exit_plan_mode", "_x.ai/exit_plan_mode":
		decision, err := client.broker.Resolve(ctx, InteractionRequest{Kind: InteractionPlanApproval, Payload: params})
		if err != nil {
			client.runner.logACP(method, "client_to_agent", "response", map[string]any{"error": err.Error()}, "error")
			return nil, err
		}
		response := map[string]any{"outcome": "cancelled"}
		if decision.Action == InteractionApprove {
			response["outcome"] = "approved"
		}
		client.runner.logACP(method, "client_to_agent", "response", response, "info")
		return response, nil
	case "x.ai/mcp/elicit", "_x.ai/mcp/elicit":
		response := map[string]any{"action": "cancel"}
		client.runner.logACP(method, "client_to_agent", "response", response, "info")
		return response, nil
	case "x.ai/ask_user_question", "_x.ai/ask_user_question":
		payload, mappings, err := normalizeGrokQuestions(params)
		if err != nil {
			requestErr := acp.NewInvalidParams(map[string]any{"error": err.Error()})
			client.runner.logACP("elicitation/create", "client_to_agent", "response", map[string]any{"error": err.Error()}, "error")
			return nil, requestErr
		}
		client.runner.logACP("elicitation/create", "agent_to_client", "request", json.RawMessage(payload), "info")
		if !client.runner.allowUserInput {
			client.runner.logACP("elicitation/create", "client_to_agent", "response", map[string]any{"action": "cancel"}, "info")
			return map[string]any{"outcome": "cancelled"}, nil
		}
		decision, err := client.runner.resolveElicitation(ctx, payload)
		if err != nil {
			client.runner.logACP("elicitation/create", "client_to_agent", "response", map[string]any{"error": err.Error()}, "error")
			return nil, err
		}
		response := grokQuestionResponse(decision, mappings)
		client.runner.logACP("elicitation/create", "client_to_agent", "response", elicitationTelemetryResponse(decision), "info")
		return response, nil
	default:
		err := acp.NewMethodNotFound(method)
		client.runner.logACP(method, "client_to_agent", "response", map[string]any{"error": err.Error()}, "error")
		return nil, err
	}
}

func (client *runnerACPClient) UnstableCreateElicitation(ctx context.Context, params acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
	client.runner.logACP("elicitation/create", "agent_to_client", "request", params, "info")
	if params.Form == nil || !client.runner.allowUserInput {
		response := acp.NewUnstableCreateElicitationResponseCancel()
		client.runner.logACP("elicitation/create", "client_to_agent", "response", response, "info")
		return response, nil
	}
	payload, _ := json.Marshal(params)
	decision, err := client.runner.resolveElicitation(ctx, payload)
	if err != nil {
		client.runner.logACP("elicitation/create", "client_to_agent", "response", map[string]any{"error": err.Error()}, "error")
		return acp.UnstableCreateElicitationResponse{}, err
	}
	var response acp.UnstableCreateElicitationResponse
	switch decision.Action {
	case InteractionAccept:
		response = acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: decision.Content}}
	case InteractionDecline:
		response = acp.NewUnstableCreateElicitationResponseDecline()
	default:
		response = acp.NewUnstableCreateElicitationResponseCancel()
	}
	client.runner.logACP("elicitation/create", "client_to_agent", "response", elicitationTelemetryResponse(decision), "info")
	return response, nil
}

/** Returns the provider-neutral elicitation result recorded in ACP telemetry. */
func elicitationTelemetryResponse(decision InteractionDecision) map[string]any {
	response := map[string]any{"action": string(decision.Action)}
	if decision.Action == InteractionAccept {
		response["content"] = decision.Content
	}
	if decision.Pending {
		// The harness receives its native cancellation response so the prompt can
		// stop before checkpointing; telemetry records that the workflow suspended.
		response["action"] = "cancel"
		response["status"] = "waiting_for_input"
	}
	return response
}

func (client *runnerACPClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, acp.NewMethodNotFound("fs/read_text_file")
}
func (client *runnerACPClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, acp.NewMethodNotFound("fs/write_text_file")
}
func (client *runnerACPClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, acp.NewMethodNotFound("terminal/create")
}
func (client *runnerACPClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, acp.NewMethodNotFound("terminal/kill")
}
func (client *runnerACPClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, acp.NewMethodNotFound("terminal/output")
}
func (client *runnerACPClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, acp.NewMethodNotFound("terminal/release")
}
func (client *runnerACPClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, acp.NewMethodNotFound("terminal/wait_for_exit")
}
