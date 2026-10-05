package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	acp "github.com/coder/acp-go-sdk"
)

const (
	flowRunHarness                 = "workflow"
	lastWorkerStartupEventSequence = 3
)

type JobError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type Completion struct {
	Status     string    `json:"status"`
	SessionID  *string   `json:"session_id"`
	StopReason *string   `json:"stop_reason"`
	Message    string    `json:"message"`
	Usage      *ACPUsage `json:"usage"`
	ExitCode   int       `json:"exit_code"`
	Error      *JobError `json:"error"`
	Stdout     string    `json:"-"`
	Stderr     string    `json:"-"`
}

type RunRequest struct {
	JobID          string         `json:"job_id"`
	CallbackToken  string         `json:"callback_token"`
	CallbackURL    string         `json:"callback_url"`
	InteractionURL string         `json:"interaction_url"`
	BudgetURL      string         `json:"budget_url"`
	RunHarness     string         `json:"run_harness"`
	RunCreatedAt   string         `json:"run_created_at"`
	RunStartedAt   string         `json:"run_started_at"`
	TraceID        string         `json:"trace_id"`
	RunSpanID      string         `json:"run_span_id"`
	DeadlineAt     string         `json:"deadline_at"`
	Resume         *ResumeRequest `json:"resume,omitempty"`
}

type ResumeRequest struct {
	Kind            string               `json:"kind,omitempty"`
	StepIndex       int                  `json:"step_index"`
	RemainingStepMS int64                `json:"remaining_step_ms"`
	SessionID       string               `json:"session_id,omitempty"`
	Response        *InteractionResponse `json:"response,omitempty"`
	Steps           []StepCompletion     `json:"steps"`
	Usage           *ACPUsage            `json:"usage,omitempty"`
	EventSequence   int                  `json:"event_sequence"`
}

type StepCompletion struct {
	ID              string    `json:"id"`
	Prompt          string    `json:"prompt,omitempty"`
	Harness         string    `json:"harness,omitempty"`
	Provider        string    `json:"provider,omitempty"`
	Model           string    `json:"model,omitempty"`
	ReasoningEffort *string   `json:"reasoning_effort"`
	Command         []string  `json:"command,omitempty"`
	Status          string    `json:"status"`
	StartedAt       string    `json:"started_at"`
	FinishedAt      string    `json:"finished_at"`
	SessionID       *string   `json:"session_id"`
	StopReason      *string   `json:"stop_reason"`
	Message         string    `json:"message"`
	Usage           *ACPUsage `json:"usage"`
	ExitCode        int       `json:"exit_code"`
	Error           *JobError `json:"error"`
	Stdout          string    `json:"stdout,omitempty"`
	Stderr          string    `json:"stderr,omitempty"`
	EmittedMetrics  []string  `json:"emitted_metrics,omitempty"`
}

/** MarshalJSON keeps the existing agent result shape while emitting command-specific output fields. */
func (step StepCompletion) MarshalJSON() ([]byte, error) {
	type common struct {
		ID         string    `json:"id"`
		Status     string    `json:"status"`
		StartedAt  string    `json:"started_at"`
		FinishedAt string    `json:"finished_at"`
		ExitCode   int       `json:"exit_code"`
		Error      *JobError `json:"error"`
	}
	base := common{ID: step.ID, Status: step.Status, StartedAt: step.StartedAt, FinishedAt: step.FinishedAt, ExitCode: step.ExitCode, Error: step.Error}
	if step.Command != nil {
		return json.Marshal(struct {
			common
			Command []string `json:"command"`
			Stdout  string   `json:"stdout"`
			Stderr  string   `json:"stderr"`
		}{common: base, Command: step.Command, Stdout: step.Stdout, Stderr: step.Stderr})
	}
	return json.Marshal(struct {
		common
		Prompt          string    `json:"prompt"`
		Harness         string    `json:"harness"`
		Provider        string    `json:"provider"`
		Model           string    `json:"model"`
		ReasoningEffort *string   `json:"reasoning_effort"`
		SessionID       *string   `json:"session_id"`
		StopReason      *string   `json:"stop_reason"`
		Message         string    `json:"message"`
		Usage           *ACPUsage `json:"usage"`
		EmittedMetrics  []string  `json:"emitted_metrics,omitempty"`
	}{common: base, Prompt: step.Prompt, Harness: step.Harness, Provider: step.Provider, Model: step.Model, ReasoningEffort: step.ReasoningEffort,
		SessionID: step.SessionID, StopReason: step.StopReason, Message: step.Message, Usage: step.Usage, EmittedMetrics: step.EmittedMetrics})
}

type WorkflowCompletion struct {
	Status         string           `json:"status"`
	Workflow       string           `json:"workflow"`
	WorkflowDigest string           `json:"workflow_digest"`
	Steps          []StepCompletion `json:"steps"`
	Usage          *ACPUsage        `json:"usage"`
	Error          *JobError        `json:"error"`
	ArtifactError  *JobError        `json:"artifact_error,omitempty"`
	MemoryError    *JobError        `json:"memory_error,omitempty"`
	EventSequence  int              `json:"event_sequence"`
	BudgetResetAt  string           `json:"budget_reset_at,omitempty"`
}

type Runner struct {
	config               Config
	harness              ResolvedHarness
	workspace            string
	proxyToken           string
	caBundle             string
	logger               *EventLogger
	broker               InteractionBroker
	commandHook          func(*exec.Cmd)
	sequence             *eventSequence
	pendingInteractionID string
	interactionMu        sync.Mutex
	interactionGate      chan struct{}
	promptCancel         context.CancelCauseFunc
	allowUserInput       bool
	resumeSessionID      string
	metrics              *metricRuntime
}

func NewRunner(config Config, jobID string, step WorkflowStep, out io.Writer) *Runner {
	return newWorkflowStepRunner(config, jobID, step, 0, newEventWriter(out), &eventSequence{}, nil)
}

func newWorkflowStepRunner(config Config, jobID string, step WorkflowStep, index int, writer *eventWriter, sequence *eventSequence, exporter *flowExporter) *Runner {
	harness, _ := config.resolveStep(step)
	proxyToken := os.Getenv("RUNNER_EGRESS_TOKEN")
	logger := newEventLogger(writer, jobID, step.Harness, config.MaxLogBytes).WithStep(step, index).WithRedactionSecret(proxyToken)
	logger.exporter = exporter
	return &Runner{
		config:          config,
		harness:         harness,
		workspace:       envOr("RUNNER_WORKSPACE", "/workspace"),
		proxyToken:      proxyToken,
		logger:          logger,
		broker:          nonInteractiveBroker{},
		sequence:        sequence,
		interactionGate: make(chan struct{}, 1),
		allowUserInput:  step.AllowUserInput,
	}
}

func newCommandRunner(config Config, jobID string, step WorkflowStep, out io.Writer) *Runner {
	return newWorkflowCommandRunner(config, jobID, step, 0, newEventWriter(out), &eventSequence{}, nil)
}

func newWorkflowCommandRunner(config Config, jobID string, step WorkflowStep, index int, writer *eventWriter, sequence *eventSequence, exporter *flowExporter) *Runner {
	proxyToken := os.Getenv("RUNNER_EGRESS_TOKEN")
	logger := newEventLogger(writer, jobID, "command", config.MaxLogBytes).WithStep(step, index).WithRedactionSecret(proxyToken)
	logger.exporter = exporter
	return &Runner{
		config: config, workspace: envOr("RUNNER_WORKSPACE", "/workspace"),
		proxyToken: proxyToken, logger: logger, sequence: sequence,
	}
}

func (runner *Runner) Execute(parent context.Context, prompt string) Completion {
	ctx, cancel := context.WithTimeout(parent, time.Duration(runner.config.DefaultStepTimeoutMS)*time.Millisecond)
	defer cancel()
	return runner.execute(ctx, prompt, nil)
}

func (runner *Runner) execute(parent context.Context, prompt string, started chan<- error) Completion {
	ctx, cancel := context.WithCancelCause(parent)
	runner.setPromptCancel(cancel)
	defer func() {
		runner.setPromptCancel(nil)
		cancel(nil)
	}()
	driver, err := newHarnessDriver(runner.harness.Type)
	if err != nil {
		notifyStarted(started, err)
		return failedCompletion("runner_setup_failed", err.Error(), false, -1)
	}
	if err := runner.prepare(driver); err != nil {
		notifyStarted(started, err)
		return failedCompletion("runner_setup_failed", err.Error(), false, -1)
	}
	cmd, err := driver.Command(runner)
	if err != nil {
		notifyStarted(started, err)
		return failedCompletion("runner_setup_failed", err.Error(), false, -1)
	}
	configureProcess(cmd)
	cmd.WaitDelay = time.Duration(runner.config.ShutdownGraceMS) * time.Millisecond
	if runner.commandHook != nil {
		runner.commandHook(cmd)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		notifyStarted(started, err)
		return failedCompletion("harness_start_failed", err.Error(), true, -1)
	}
	stdout, stdoutWriter := io.Pipe()
	stderr, stderrWriter := io.Pipe()
	// Supplying writers makes os/exec own the child-side copy goroutines, so Wait
	// cannot close or outrun streams still being consumed by ACP and the logger.
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		_ = stderr.Close()
		_ = stderrWriter.Close()
		notifyStarted(started, err)
		return failedCompletion("harness_start_failed", err.Error(), true, -1)
	}
	defer stdout.Close()
	defer stderr.Close()
	stderrState := &stderrCapture{}
	stderrDone := make(chan struct{})
	go func() {
		runner.consumeStderr(stderr, stderrState)
		close(stderrDone)
	}()
	waitDone := make(chan error, 1)
	go func() {
		waitErr := cmd.Wait()
		_ = stdoutWriter.Close()
		_ = stderrWriter.Close()
		waitDone <- waitErr
	}()

	state := &acpState{}
	client := &runnerACPClient{runner: runner, state: state, broker: runner.broker}
	if inspector, ok := driver.(harnessSessionUpdateInspector); ok {
		client.inspector = inspector
	}
	connection := acp.NewClientSideConnection(client, stdin, stdout)
	// The SDK's default logger writes free-form diagnostics to stderr. The runner
	// emits its own structured lifecycle and ACP events, so suppress that duplicate
	// stream to keep every container log machine-readable.
	connection.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	clientCapabilities := acp.ClientCapabilities{}
	if runner.allowUserInput && runner.config.Interactions.Provider == "callback" {
		clientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	}
	initializeRequest := acp.InitializeRequest{
		ProtocolVersion:    acp.ProtocolVersionNumber,
		ClientCapabilities: clientCapabilities,
		ClientInfo:         &acp.Implementation{Name: "flow", Title: stringPointer("Flow"), Version: "0.1.0"},
	}
	runner.logACP("initialize", "client_to_agent", "request", initializeRequest, "info")
	initResponse, err := connection.Initialize(ctx, initializeRequest)
	if err != nil {
		runner.logACP("initialize", "agent_to_client", "response", map[string]any{"error": err.Error()}, "error")
		waitErr := runner.stopProcess(cmd, stdin, stdout, waitDone, stderrDone)
		if completion, ok := harnessContextCompletion(context.Cause(ctx), exitCode(waitErr)); ok {
			notifyStarted(started, nil)
			return completion
		}
		message := failureText(err, stderrState, waitErr)
		notifyStarted(started, errors.New(message))
		return failedCompletion("acp_initialize_failed", message, false, exitCode(waitErr))
	}
	runner.logACP("initialize", "agent_to_client", "response", initResponse, "info")
	if initResponse.ProtocolVersion != acp.ProtocolVersionNumber {
		err = fmt.Errorf("agent selected ACP protocol version %d; runner requires %d", initResponse.ProtocolVersion, acp.ProtocolVersionNumber)
		notifyStarted(started, err)
		waitErr := runner.stopProcess(cmd, stdin, stdout, waitDone, stderrDone)
		return failedCompletion("acp_protocol_mismatch", err.Error(), false, exitCode(waitErr))
	}
	if err := driver.ConfigureACP(ctx, runner, connection); err != nil {
		waitErr := runner.stopProcess(cmd, stdin, stdout, waitDone, stderrDone)
		if completion, ok := harnessContextCompletion(context.Cause(ctx), exitCode(waitErr)); ok {
			notifyStarted(started, nil)
			return completion
		}
		message := failureText(err, stderrState, waitErr)
		notifyStarted(started, errors.New(message))
		return failedCompletion("acp_configuration_failed", message, false, exitCode(waitErr))
	}
	var sessionID acp.SessionId
	if runner.resumeSessionID != "" {
		resumeRequest := acp.ResumeSessionRequest{SessionId: acp.SessionId(runner.resumeSessionID), Cwd: runner.workspace, McpServers: runner.metricMCPServers()}
		runner.logACP("session/resume", "client_to_agent", "request", resumeRequest, "info")
		resumeResponse, resumeErr := connection.ResumeSession(ctx, resumeRequest)
		if resumeErr != nil {
			waitErr := runner.stopProcess(cmd, stdin, stdout, waitDone, stderrDone)
			message := failureText(resumeErr, stderrState, waitErr)
			notifyStarted(started, errors.New(message))
			return failedCompletion("acp_session_resume_failed", message, false, exitCode(waitErr))
		}
		runner.logACP("session/resume", "agent_to_client", "response", resumeResponse, "info")
		sessionID = acp.SessionId(runner.resumeSessionID)
	} else {
		newSessionRequest := acp.NewSessionRequest{Cwd: runner.workspace, McpServers: runner.metricMCPServers()}
		runner.logACP("session/new", "client_to_agent", "request", newSessionRequest, "info")
		session, sessionErr := connection.NewSession(ctx, newSessionRequest)
		if sessionErr != nil {
			runner.logACP("session/new", "agent_to_client", "response", map[string]any{"error": sessionErr.Error()}, "error")
			waitErr := runner.stopProcess(cmd, stdin, stdout, waitDone, stderrDone)
			if completion, ok := harnessContextCompletion(context.Cause(ctx), exitCode(waitErr)); ok {
				notifyStarted(started, nil)
				return completion
			}
			message := failureText(sessionErr, stderrState, waitErr)
			notifyStarted(started, errors.New(message))
			return failedCompletion("acp_session_failed", message, false, exitCode(waitErr))
		}
		runner.logACP("session/new", "agent_to_client", "response", session, "info")
		if err := runner.configureReasoningEffort(ctx, connection, session); err != nil {
			waitErr := runner.stopProcess(cmd, stdin, stdout, waitDone, stderrDone)
			if completion, ok := harnessContextCompletion(context.Cause(ctx), exitCode(waitErr)); ok {
				notifyStarted(started, nil)
				return completion
			}
			message := failureText(err, stderrState, waitErr)
			notifyStarted(started, errors.New(message))
			return failedCompletion("acp_configuration_failed", message, false, exitCode(waitErr))
		}
		sessionID = session.SessionId
	}
	notifyStarted(started, nil)

	promptRequest := acp.PromptRequest{
		SessionId: sessionID,
		Prompt:    []acp.ContentBlock{acp.TextBlock(prompt)},
	}
	runner.logACP("session/prompt", "client_to_agent", "request", promptRequest, "info")
	promptResponse, promptErr := connection.Prompt(ctx, promptRequest)
	if promptErr != nil {
		if errors.Is(context.Cause(ctx), errInteractionPending) {
			runner.logACP("session/prompt", "agent_to_client", "response", map[string]any{"status": "waiting_for_input"}, "info")
		} else {
			runner.logACP("session/prompt", "agent_to_client", "response", map[string]any{"error": promptErr.Error()}, "error")
		}
	} else {
		runner.logACP("session/prompt", "agent_to_client", "response", promptResponse, "info")
	}
	if ctx.Err() == nil && runner.pendingInteraction() == "" {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Duration(runner.config.ShutdownGraceMS)*time.Millisecond)
		closeRequest := acp.CloseSessionRequest{SessionId: sessionID}
		runner.logACP("session/close", "client_to_agent", "request", closeRequest, "info")
		closeResponse, closeErr := connection.CloseSession(closeCtx, closeRequest)
		if closeErr != nil {
			runner.logACP("session/close", "agent_to_client", "response", map[string]any{"error": closeErr.Error()}, "error")
		} else {
			runner.logACP("session/close", "agent_to_client", "response", closeResponse, "info")
		}
		closeCancel()
	}
	waitErr := runner.stopProcess(cmd, stdin, stdout, waitDone, stderrDone)
	exit := exitCode(waitErr)
	if runner.pendingInteraction() != "" {
		value := string(sessionID)
		return Completion{Status: "waiting_for_input", SessionID: &value, Message: "", ExitCode: exit}
	}
	if completion, ok := harnessContextCompletion(context.Cause(ctx), exit); ok {
		return completion
	}
	if promptErr != nil {
		return failedCompletion("acp_prompt_failed", failureText(promptErr, stderrState, waitErr), false, exit)
	}
	state.capturePromptUsage(promptResponse)

	state.mu.Lock()
	message := state.message.String()
	usage := state.usage
	harnessDiagnostic := state.harnessDiagnostic
	terminalHarnessError := state.terminalHarnessError
	state.mu.Unlock()
	if terminalHarnessError {
		if harnessDiagnostic == "" {
			harnessDiagnostic = "Harness reported a terminal system error"
		}
		return failedCompletion("acp_prompt_failed", redactText(harnessDiagnostic, runner.proxyToken), false, exit)
	}
	if len([]byte(message)) > runner.config.MaxResultBytes {
		return failedCompletion("output_too_large", "Final agent message exceeded the configured result limit", false, exit)
	}
	stopReason := string(promptResponse.StopReason)
	sessionIDValue := string(sessionID)
	completion := Completion{SessionID: &sessionIDValue, StopReason: &stopReason, Message: message, Usage: usage, ExitCode: exit}
	switch promptResponse.StopReason {
	case acp.StopReasonEndTurn:
		if message == "" {
			return failedCompletion("incomplete_acp_output", "Harness completed without a final agent message", false, exit)
		}
		completion.Status = "succeeded"
	case acp.StopReasonMaxTokens, acp.StopReasonMaxTurnRequests, acp.StopReasonRefusal:
		completion.Status = "partial"
	case acp.StopReasonCancelled:
		return failedCompletion("cancelled", "Harness cancelled the prompt", false, exit)
	default:
		return failedCompletion("invalid_stop_reason", fmt.Sprintf("Harness returned unknown ACP stop reason %q", stopReason), false, exit)
	}
	return completion
}

func (runner *Runner) setPendingInteraction(id string) {
	runner.interactionMu.Lock()
	runner.pendingInteractionID = id
	cancel := runner.promptCancel
	runner.interactionMu.Unlock()
	if cancel != nil {
		cancel(errInteractionPending)
	}
}

func (runner *Runner) setPromptCancel(cancel context.CancelCauseFunc) {
	runner.interactionMu.Lock()
	runner.promptCancel = cancel
	runner.interactionMu.Unlock()
}

/** Resolves at most one user-input request at a time for an opted-in workflow step. */
func (runner *Runner) resolveElicitation(ctx context.Context, payload json.RawMessage) (InteractionDecision, error) {
	if !runner.allowUserInput {
		return InteractionDecision{Action: InteractionCancel}, nil
	}
	select {
	case runner.interactionGate <- struct{}{}:
		defer func() { <-runner.interactionGate }()
	default:
		return InteractionDecision{}, fmt.Errorf("another user input request is already active")
	}
	decision, err := runner.broker.Resolve(ctx, InteractionRequest{Kind: InteractionElicitation, Payload: payload})
	if err == nil && decision.InteractionID != "" && (decision.Pending || decision.Action == InteractionDecline || decision.Action == InteractionCancel) {
		runner.setPendingInteraction(decision.InteractionID)
	}
	return decision, err
}

func (runner *Runner) pendingInteraction() string {
	runner.interactionMu.Lock()
	defer runner.interactionMu.Unlock()
	return runner.pendingInteractionID
}

/** Maps context termination consistently even when it interrupts ACP setup. */
func harnessContextCompletion(err error, exit int) (Completion, bool) {
	if errors.Is(err, context.DeadlineExceeded) {
		return failedCompletion("timeout", "Harness exceeded the configured execution timeout", false, exitCodeWithFallback(exit, 124)), true
	}
	if errors.Is(err, context.Canceled) {
		return failedCompletion("cancelled", "Runner was cancelled", false, exitCodeWithFallback(exit, 130)), true
	}
	return Completion{}, false
}

/** Executes one deterministic command with bounded, redacted output. */
func (runner *Runner) executeCommand(ctx context.Context, step WorkflowStep, started chan<- error) Completion {
	if err := runner.prepareRuntime(); err != nil {
		notifyStarted(started, err)
		return failedCompletion("command_setup_failed", err.Error(), false, -1)
	}
	cmd := exec.Command(step.Command[0], step.Command[1:]...)
	cmd.Dir = runner.workspace
	extra := map[string]string{"NODE_USE_ENV_PROXY": "1"}
	if runner.caBundle != "" {
		extra["NODE_EXTRA_CA_CERTS"] = runner.caBundle
	}
	cmd.Env = runner.childEnvironment(extra)
	configureProcess(cmd)
	cmd.WaitDelay = time.Duration(runner.config.ShutdownGraceMS) * time.Millisecond
	if runner.commandHook != nil {
		runner.commandHook(cmd)
	}
	capture := &commandOutputCapture{limit: runner.config.MaxResultBytes, exceeded: make(chan struct{})}
	cmd.Stdout = capture.writer(true)
	cmd.Stderr = capture.writer(false)
	if err := cmd.Start(); err != nil {
		notifyStarted(started, err)
		return failedCompletion("command_start_failed", err.Error(), false, -1)
	}
	notifyStarted(started, nil)
	startedPayload, _ := json.Marshal(map[string]any{"command": step.Command})
	runner.logger.Emit("command", "info", "command.started", startedPayload, runner.nextSequence())
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	stop := func() error {
		terminateProcess(cmd, syscall.SIGTERM)
		select {
		case err := <-waitDone:
			return err
		case <-time.After(time.Duration(runner.config.ShutdownGraceMS) * time.Millisecond):
			terminateProcess(cmd, syscall.SIGKILL)
			return <-waitDone
		}
	}
	var waitErr error
	select {
	case waitErr = <-waitDone:
	case <-ctx.Done():
		waitErr = stop()
	case <-capture.exceeded:
		waitErr = stop()
	}
	stdoutText, stderrText, overflow := capture.result(runner.proxyToken)
	if stdoutText != "" {
		runner.logger.Text("command", "info", "command.stdout", stdoutText, runner.nextSequence())
	}
	if stderrText != "" {
		runner.logger.Text("command", "warn", "command.stderr", stderrText, runner.nextSequence())
	}
	withOutput := func(completion Completion) Completion {
		completion.Stdout, completion.Stderr = stdoutText, stderrText
		finishedPayload, _ := json.Marshal(map[string]any{"status": completion.Status, "exit_code": completion.ExitCode})
		level := "error"
		if completion.Status == "succeeded" || completion.Status == "partial" {
			level = "info"
		}
		runner.logger.Emit("command", level, "command.finished", finishedPayload, runner.nextSequence())
		return completion
	}
	if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		return withOutput(failedCompletion("timeout", "Command exceeded the configured step timeout", false, 124))
	}
	if errors.Is(context.Cause(ctx), context.Canceled) {
		return withOutput(failedCompletion("cancelled", "Command was cancelled", false, 130))
	}
	if overflow {
		return withOutput(failedCompletion("output_too_large", "Command output exceeded the configured result limit", false, exitCode(waitErr)))
	}
	if waitErr != nil {
		exit := exitCode(waitErr)
		return withOutput(failedCompletion("command_failed", fmt.Sprintf("Command exited with status %d", exit), false, exit))
	}
	return withOutput(Completion{Status: "succeeded", ExitCode: 0})
}

/** Executes every configured workflow step serially in the shared workspace. */
func executeWorkflow(parent context.Context, config Config, request RunRequest, out io.Writer, started chan<- error, progress func(WorkflowCompletion)) (result WorkflowCompletion) {
	writer := newEventWriter(out)
	sequenceValue := lastWorkerStartupEventSequence
	if request.Resume != nil && request.Resume.EventSequence > sequenceValue {
		sequenceValue = request.Resume.EventSequence
	}
	sequence := &eventSequence{value: sequenceValue}
	var exporter *flowExporter
	if request.CallbackURL != "" {
		exporter = newFlowExporter(request.CallbackURL, request.CallbackToken, request.JobID, request.RunHarness, request.RunCreatedAt, request.RunStartedAt, request.TraceID, request.RunSpanID, writer.write, config.MaxLogBytes)
		defer func() {
			exporter.Close()
			if err := exporter.Error(); err != nil && result.Status != "cancelled" && result.Status != "timed_out" {
				result.Status = "failed"
				result.Error = &JobError{Code: "history_persist_failed", Message: err.Error(), Retryable: true}
				progress(result)
			}
		}()
	}
	metricReceiver, metricError := startMetricReceiver(exporter, request.JobID, request.TraceID, request.RunSpanID, sequence)
	if metricError != nil {
		notifyStarted(started, metricError)
		steps := make([]StepCompletion, len(config.Workflow.Steps))
		for index, step := range config.Workflow.Steps {
			steps[index] = pendingStepResult(step)
		}
		return WorkflowCompletion{Status: "failed", Workflow: config.Workflow.Name, WorkflowDigest: config.Workflow.BundleDigest, Steps: steps, Error: &JobError{Code: "metric_setup_failed", Message: metricError.Error(), Retryable: true}, EventSequence: sequence.Value()}
	}
	defer metricReceiver.Close()
	deadline := time.Now().Add(time.Duration(config.WorkflowTimeoutMS) * time.Millisecond)
	if request.DeadlineAt != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, request.DeadlineAt); err == nil && parsed.Before(deadline) {
			deadline = parsed
		}
	}
	workflowCtx, workflowTimeout := newPausableTimeout(parent, time.Until(deadline))
	defer workflowTimeout.Stop()
	result = WorkflowCompletion{
		Status: "running", Workflow: config.Workflow.Name, WorkflowDigest: config.Workflow.BundleDigest,
		Steps: make([]StepCompletion, len(config.Workflow.Steps)),
	}
	for index, step := range config.Workflow.Steps {
		result.Steps[index] = pendingStepResult(step)
	}
	startIndex := 0
	if request.Resume != nil {
		startIndex = request.Resume.StepIndex
		if len(request.Resume.Steps) == len(result.Steps) {
			copy(result.Steps, request.Resume.Steps)
		}
		result.Usage = request.Resume.Usage
		if result.Usage == nil {
			for _, completed := range result.Steps {
				result.Usage = addUsage(result.Usage, completed.Usage)
			}
		}
	}
	progress(result)
	totalResultBytes := 0
	for index := startIndex; index < len(config.Workflow.Steps); index++ {
		step := config.Workflow.Steps[index]
		metricReceiver.SetStep(step, index, result.Steps[index].EmittedMetrics)
		if workflowCtx.Err() != nil {
			completion := contextCompletion(context.Cause(workflowCtx))
			notifyStarted(started, errors.New(completion.Error.Message))
			started = nil
			result.Steps[index] = stepResult(step, completion, time.Now(), time.Now())
			result.Status, result.Error = completion.Status, completion.Error
			progress(result)
			return result
		}
		stepStarted := time.Now()
		resumingStep := request.Resume != nil && request.Resume.Kind != "budget" && index == request.Resume.StepIndex
		if resumingStep {
			if original, err := time.Parse(time.RFC3339Nano, result.Steps[index].StartedAt); err == nil {
				stepStarted = original
			}
		}
		stepCtx, stepTimeout := newPausableTimeout(workflowCtx, time.Duration(step.TimeoutMS)*time.Millisecond)
		if resumingStep && request.Resume.RemainingStepMS > 0 {
			stepTimeout.Stop()
			stepCtx, stepTimeout = newPausableTimeout(workflowCtx, time.Duration(request.Resume.RemainingStepMS)*time.Millisecond)
		}
		var runner *Runner
		if step.Command != nil {
			runner = newWorkflowCommandRunner(config, request.JobID, step, index, writer, sequence, exporter)
		} else {
			runner = newWorkflowStepRunner(config, request.JobID, step, index, writer, sequence, exporter)
			if step.AllowUserInput && config.Interactions.Provider == "callback" && request.InteractionURL != "" {
				runner.broker = callbackBroker{
					url: request.InteractionURL, token: request.CallbackToken,
					maxRequestBytes: config.Interactions.MaxRequestBytes, maxResponseBytes: config.Interactions.MaxResponseBytes,
					liveWait: time.Duration(config.Interactions.LiveWaitTimeoutMS) * time.Millisecond,
					pause:    func() { workflowTimeout.Pause(); stepTimeout.Pause() },
					resume:   func() { stepTimeout.Resume(); workflowTimeout.Resume() },
					stepID:   step.ID, remainingStep: stepTimeout.Remaining,
				}
			}
			if resumingStep {
				runner.resumeSessionID = request.Resume.SessionID
				if request.Resume.Response == nil || request.Resume.Response.Action != InteractionAccept {
					runner.allowUserInput = false
				}
			}
		}
		if len(step.RequiredMetrics) > 0 {
			runner.metrics = metricReceiver.Runtime()
		}
		if resumingStep {
			runner.emitStepResumed(step, stepStarted)
		} else {
			runner.emitStepStarted(step, stepStarted)
		}
		result.Steps[index].Status = "running"
		result.Steps[index].StartedAt = stepStarted.UTC().Format(time.RFC3339Nano)
		progress(result)
		stepStartedChannel := started
		var completion Completion
		if step.Command != nil {
			completion = runner.executeCommand(stepCtx, step, stepStartedChannel)
		} else {
			promptPath := filepath.Join(envOr("RUNNER_WORKSPACE", "/workspace"), "prompts", filepath.FromSlash(step.Prompt))
			promptBytes, err := os.ReadFile(promptPath)
			if err != nil || len(promptBytes) == 0 || len(promptBytes) > config.MaxPromptBytes {
				message := fmt.Sprintf("read prompt %s", step.Prompt)
				if err != nil {
					message += ": " + err.Error()
				} else {
					message += ": file is empty or exceeds max_prompt_bytes"
				}
				completion = failedCompletion("prompt_read_failed", message, false, -1)
				notifyStarted(started, errors.New(message))
			} else {
				prompt := string(promptBytes)
				if resumingStep {
					encoded, _ := json.Marshal(request.Resume.Response)
					prompt = "The user has responded to the pending elicitation. Continue the task using this response: " + string(encoded)
				}
				promptWithMetrics, promptErr := promptWithRequiredMetrics(prompt, step.RequiredMetrics, config.MaxPromptBytes)
				if promptErr != nil {
					message := fmt.Sprintf("build prompt for step %s: %s", step.ID, promptErr)
					completion = failedCompletion("prompt_too_large", message, false, -1)
					notifyStarted(started, errors.New(message))
				} else {
					completion = runner.execute(stepCtx, promptWithMetrics, stepStartedChannel)
				}
			}
		}
		stepTimeout.Stop()
		started = nil
		completion = redactCompletion(completion, runner.proxyToken)
		if completion.Status == "succeeded" || completion.Status == "partial" {
			if missing := metricReceiver.Missing(step); len(missing) > 0 {
				original := completion
				completion = failedCompletion("required_metrics_missing", "Step did not emit required metrics: "+strings.Join(missing, ", "), false, original.ExitCode)
				completion.Message, completion.SessionID, completion.StopReason, completion.Usage = original.Message, original.SessionID, original.StopReason, original.Usage
			}
		}
		budgetAction := "continue"
		budgetResetAt := ""
		if step.Command == nil && config.Workflow.TokenBudget != nil && (completion.Status == "succeeded" || completion.Status == "partial") {
			var tokens *int
			if completion.Usage != nil {
				tokens = completion.Usage.TotalTokens
			}
			charge, chargeErr := reportBudgetCharge(workflowCtx, request, step, index, tokens, workflowTimeout.Remaining())
			if chargeErr != nil {
				original := completion
				completion = failedCompletion("budget_accounting_failed", chargeErr.Error(), true, original.ExitCode)
				completion.Message, completion.SessionID, completion.StopReason, completion.Usage = original.Message, original.SessionID, original.StopReason, original.Usage
			} else if tokens == nil {
				original := completion
				completion = failedCompletion("token_usage_unavailable", "Budgeted agent step did not return total token usage", false, original.ExitCode)
				completion.Message, completion.SessionID, completion.StopReason = original.Message, original.SessionID, original.StopReason
			} else {
				budgetAction, budgetResetAt = charge.Action, charge.ResetAt
			}
		}
		finished := time.Now()
		totalResultBytes += len([]byte(completion.Message)) + len([]byte(completion.Stdout)) + len([]byte(completion.Stderr))
		if totalResultBytes > config.MaxResultBytes {
			stdout, stderr := completion.Stdout, completion.Stderr
			completion = failedCompletion("output_too_large", "Combined step output exceeded the configured result limit", false, completion.ExitCode)
			completion.Stdout, completion.Stderr = stdout, stderr
		}
		result.Steps[index] = stepResult(step, completion, stepStarted, finished)
		result.Steps[index].EmittedMetrics = metricReceiver.Seen(step.ID)
		if completion.Status == "waiting_for_input" {
			result.Steps[index].Status = "running"
			result.Steps[index].FinishedAt = ""
		}
		if completion.Status == "waiting_for_input" {
			runner.emitStepSuspended(completion)
		} else {
			runner.emitStepFinished(completion)
		}
		result.Usage = addUsage(result.Usage, completion.Usage)
		progress(result)
		if completion.Status == "succeeded" && budgetAction == "suspend" && index+1 < len(config.Workflow.Steps) {
			result.Status, result.Error = "budget_suspended", nil
			result.EventSequence = sequence.Value()
			result.BudgetResetAt = budgetResetAt
			progress(result)
			return result
		}
		if completion.Status == "waiting_for_input" {
			result.Status, result.Error = completion.Status, nil
			result.EventSequence = sequence.Value()
			progress(result)
			return result
		}
		if completion.Status != "succeeded" {
			result.Status, result.Error = completion.Status, completion.Error
			progress(result)
			return result
		}
	}
	result.Status = "succeeded"
	result.EventSequence = sequence.Value()
	progress(result)
	return result
}

/** Emits the stable metadata that opens one attempted workflow step. */
func (runner *Runner) emitStepStarted(step WorkflowStep, startedAt time.Time) {
	payload := stepLifecyclePayload(step)
	payload["started_at"] = startedAt.UTC().Format(time.RFC3339Nano)
	encoded, _ := json.Marshal(payload)
	runner.logger.Emit("runner", "info", "step.started", encoded, runner.nextSequence())
}

/** Emits a continuation marker for a previously suspended logical workflow step. */
func (runner *Runner) emitStepResumed(step WorkflowStep, startedAt time.Time) {
	payload := stepLifecyclePayload(step)
	payload["started_at"] = startedAt.UTC().Format(time.RFC3339Nano)
	encoded, _ := json.Marshal(payload)
	runner.logger.Emit("runner", "info", "step.resumed", encoded, runner.nextSequence())
}

func stepLifecyclePayload(step WorkflowStep) map[string]any {
	payload := map[string]any{"timeout_ms": step.TimeoutMS}
	if step.Command != nil {
		payload["command"] = step.Command
	} else {
		payload["prompt"] = step.Prompt
		payload["harness"] = step.Harness
		payload["provider"] = step.Provider
		payload["model"] = step.Model
		payload["reasoning_effort"] = step.ReasoningEffort
	}
	return payload
}

/** Emits a non-terminal checkpoint marker for a workflow step awaiting input. */
func (runner *Runner) emitStepSuspended(completion Completion) {
	runner.emitStepCompletion("step.suspended", completion, "info")
}

/** Emits the terminal status and stable completion fields for one workflow step. */
func (runner *Runner) emitStepFinished(completion Completion) {
	level := "info"
	if completion.Status != "succeeded" {
		level = "error"
	}
	runner.emitStepCompletion("step.finished", completion, level)
}

func (runner *Runner) emitStepCompletion(eventType string, completion Completion, level string) {
	payload, _ := json.Marshal(map[string]any{
		"status": completion.Status, "exit_code": completion.ExitCode,
		"session_id": completion.SessionID, "stop_reason": completion.StopReason,
		"usage": completion.Usage, "error": completion.Error,
	})
	runner.logger.Emit("runner", level, eventType, payload, runner.nextSequence())
}

/** Records one normalized ACP request, response, or notification. */
func (runner *Runner) logACP(method, direction, phase string, value any, level string) {
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "method": method, "direction": direction, "phase": phase,
		map[string]string{"request": "params", "notification": "params", "response": "result"}[phase]: value,
	})
	if err != nil {
		runner.logger.Text("runner", "error", "acp.log_encode_failed", err.Error(), runner.nextSequence())
		return
	}
	runner.logger.Emit("acp", level, method, payload, runner.nextSequence())
}

func redactCompletion(completion Completion, secret string) Completion {
	completion.Message = redactText(completion.Message, secret)
	completion.Stdout = redactText(completion.Stdout, secret)
	completion.Stderr = redactText(completion.Stderr, secret)
	if completion.Error != nil {
		copy := *completion.Error
		copy.Message = redactText(copy.Message, secret)
		completion.Error = &copy
	}
	return completion
}

func contextCompletion(err error) Completion {
	if errors.Is(err, context.Canceled) {
		return failedCompletion("cancelled", "Workflow was cancelled", false, 130)
	}
	return failedCompletion("timeout", "Workflow exceeded its configured execution timeout", false, 124)
}

func stepResult(step WorkflowStep, completion Completion, startedAt, finishedAt time.Time) StepCompletion {
	result := StepCompletion{
		ID: step.ID, Prompt: step.Prompt, Harness: step.Harness, Provider: step.Provider, Model: step.Model, ReasoningEffort: step.ReasoningEffort,
		Status: completion.Status, StartedAt: startedAt.UTC().Format(time.RFC3339Nano), FinishedAt: finishedAt.UTC().Format(time.RFC3339Nano),
		SessionID: completion.SessionID, StopReason: completion.StopReason, Message: completion.Message, Usage: completion.Usage,
		ExitCode: completion.ExitCode, Error: completion.Error, Command: step.Command, Stdout: completion.Stdout, Stderr: completion.Stderr,
	}
	return result
}

func (runner *Runner) metricMCPServers() []acp.McpServer {
	if runner.metrics == nil {
		return []acp.McpServer{}
	}
	return []acp.McpServer{metricMCPServer(runner.metrics)}
}

func pendingStepResult(step WorkflowStep) StepCompletion {
	return StepCompletion{ID: step.ID, Prompt: step.Prompt, Harness: step.Harness, Provider: step.Provider, Model: step.Model, ReasoningEffort: step.ReasoningEffort,
		Command: step.Command, Status: "pending"}
}

/** Adds only token counters; context-window occupancy is meaningful per step, not in aggregate. */
func addUsage(total, step *ACPUsage) *ACPUsage {
	if step == nil {
		return total
	}
	if total == nil {
		total = &ACPUsage{}
	}
	add := func(target **int, value *int) {
		if value == nil {
			return
		}
		if *target == nil {
			*target = intPointer(0)
		}
		**target += *value
	}
	add(&total.TotalTokens, step.TotalTokens)
	add(&total.InputTokens, step.InputTokens)
	add(&total.OutputTokens, step.OutputTokens)
	add(&total.ThoughtTokens, step.ThoughtTokens)
	add(&total.CachedReadTokens, step.CachedReadTokens)
	add(&total.CachedWriteTokens, step.CachedWriteTokens)
	setTotalInputTokens(total)
	return total
}

func (runner *Runner) prepareRuntime() error {
	if runner.proxyToken == "" {
		return fmt.Errorf("RUNNER_EGRESS_TOKEN is required")
	}
	if err := os.MkdirAll(runner.workspace, 0o755); err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}
	runtimeDir := envOr("RUNNER_RUNTIME_DIR", "/tmp/agent-runner")
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		return fmt.Errorf("create runtime directory: %w", err)
	}
	cloudflareCA, err := os.ReadFile("/etc/cloudflare/certs/cloudflare-containers-ca.crt")
	if err == nil {
		systemCA, readError := os.ReadFile("/etc/ssl/certs/ca-certificates.crt")
		if readError != nil {
			return fmt.Errorf("read system CA bundle: %w", readError)
		}
		runner.caBundle = filepath.Join(runtimeDir, "ca-certificates.crt")
		combined := append(append([]byte(nil), systemCA...), '\n')
		combined = append(combined, cloudflareCA...)
		if writeError := os.WriteFile(runner.caBundle, combined, 0o644); writeError != nil {
			return fmt.Errorf("write CA bundle: %w", writeError)
		}
		if setError := os.Setenv("SSL_CERT_FILE", runner.caBundle); setError != nil {
			return fmt.Errorf("configure CA bundle: %w", setError)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read Cloudflare interception CA: %w", err)
	}
	return nil
}

func (runner *Runner) prepare(driver harnessDriver) error {
	if err := runner.prepareRuntime(); err != nil {
		return err
	}
	return driver.Prepare(runner)
}

/** Stops an ACP process and waits until its diagnostic stream has been fully consumed. */
func (runner *Runner) stopProcess(cmd *exec.Cmd, stdin, stdout io.Closer, waitDone <-chan error, stderrDone <-chan struct{}) error {
	_ = stdin.Close()
	waitForDrain := func(err error) error {
		<-stderrDone
		return err
	}
	select {
	case err := <-waitDone:
		return waitForDrain(err)
	case <-time.After(time.Duration(runner.config.ShutdownGraceMS) * time.Millisecond):
		_ = stdout.Close()
		terminateProcess(cmd, syscall.SIGTERM)
	}
	select {
	case err := <-waitDone:
		return waitForDrain(err)
	case <-time.After(time.Duration(runner.config.ShutdownGraceMS) * time.Millisecond):
		terminateProcess(cmd, syscall.SIGKILL)
		return waitForDrain(<-waitDone)
	}
}

type stderrCapture struct {
	mu      sync.Mutex
	message string
}

type commandOutputCapture struct {
	mu       sync.Mutex
	stdout   bytes.Buffer
	stderr   bytes.Buffer
	limit    int
	written  int
	overflow bool
	exceeded chan struct{}
}

type commandStreamWriter struct {
	capture *commandOutputCapture
	stdout  bool
}

func (capture *commandOutputCapture) writer(stdout bool) io.Writer {
	return commandStreamWriter{capture: capture, stdout: stdout}
}

func (writer commandStreamWriter) Write(value []byte) (int, error) {
	writer.capture.mu.Lock()
	defer writer.capture.mu.Unlock()
	remaining := writer.capture.limit - writer.capture.written
	if remaining > 0 {
		retained := min(remaining, len(value))
		if writer.stdout {
			_, _ = writer.capture.stdout.Write(value[:retained])
		} else {
			_, _ = writer.capture.stderr.Write(value[:retained])
		}
		writer.capture.written += retained
	}
	if len(value) > max(remaining, 0) {
		if !writer.capture.overflow {
			writer.capture.overflow = true
			close(writer.capture.exceeded)
		}
	}
	return len(value), nil
}

func (capture *commandOutputCapture) result(scopedToken string) (string, string, bool) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return redactText(capture.stdout.String(), scopedToken), redactText(capture.stderr.String(), scopedToken), capture.overflow
}

func (runner *Runner) consumeStderr(reader io.Reader, capture *stderrCapture) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 16*1024), 4*1024*1024)
	for scanner.Scan() {
		line := redactText(scanner.Text(), runner.proxyToken)
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			capture.mu.Lock()
			capture.message = truncateText(trimmed, 4096)
			capture.mu.Unlock()
		}
		runner.logger.Text(runner.harness.Type+"_stderr", "warn", "stderr", line, runner.nextSequence())
	}
}

func (runner *Runner) nextSequence() int {
	return runner.sequence.Next()
}

type eventSequence struct {
	mu    sync.Mutex
	value int
}

func (sequence *eventSequence) Next() int {
	sequence.mu.Lock()
	defer sequence.mu.Unlock()
	sequence.value++
	return sequence.value
}

func (sequence *eventSequence) Value() int {
	sequence.mu.Lock()
	defer sequence.mu.Unlock()
	return sequence.value
}

func failureText(primary error, stderr *stderrCapture, waitErr error) string {
	stderr.mu.Lock()
	stderrMessage := stderr.message
	stderr.mu.Unlock()
	if primary != nil {
		message := primary.Error()
		if stderrMessage != "" && stderrMessage != message {
			message += "; stderr: " + stderrMessage
		}
		return truncateText(message, 4096)
	}
	if stderrMessage != "" {
		return stderrMessage
	}
	if waitErr != nil {
		return waitErr.Error()
	}
	return "harness failed"
}

func notifyStarted(channel chan<- error, err error) {
	if channel != nil {
		channel <- err
	}
}

func truncateText(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func failedCompletion(code, message string, retryable bool, exitCode int) Completion {
	status := "failed"
	if code == "timeout" {
		status = "timed_out"
	} else if code == "cancelled" {
		status = "cancelled"
	}
	return Completion{Status: status, Message: "", ExitCode: exitCode, Error: &JobError{Code: code, Message: message, Retryable: retryable}}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	return -1
}

func exitCodeWithFallback(code, fallback int) int {
	if code < 0 {
		return fallback
	}
	return code
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func stringPointer(value string) *string { return &value }

func configureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if os.Geteuid() == 0 {
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: 10001, Gid: 10001}
	}
}

func terminateProcess(cmd *exec.Cmd, signal syscall.Signal) {
	if cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, signal)
}
