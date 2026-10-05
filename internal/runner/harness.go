package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	acp "github.com/coder/acp-go-sdk"
)

type harnessDriver interface {
	Name() string
	Prepare(*Runner) error
	Command(*Runner) (*exec.Cmd, error)
	ConfigureACP(context.Context, *Runner, *acp.ClientSideConnection) error
}

type harnessSessionUpdateAssessment struct {
	Diagnostic string
	Error      bool
	Terminal   bool
}

type harnessSessionUpdateInspector interface {
	AssessSessionUpdate(acp.SessionUpdate) harnessSessionUpdateAssessment
}

func newHarnessDriver(kind string) (harnessDriver, error) {
	switch kind {
	case "codex":
		return codexDriver{}, nil
	case "grok":
		return grokDriver{}, nil
	case "claude-code":
		return claudeCodeDriver{}, nil
	case "opencode":
		return openCodeDriver{}, nil
	case "pi":
		return piDriver{}, nil
	default:
		return nil, fmt.Errorf("unsupported harness %q", kind)
	}
}

type codexDriver struct{}

func (codexDriver) Name() string { return "codex" }

func (codexDriver) Prepare(runner *Runner) error {
	home := envOr("CODEX_HOME", "/tmp/codex-home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		return fmt.Errorf("create CODEX_HOME: %w", err)
	}
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(renderCodexConfig(runner.harness, runner.allowUserInput)), 0o644); err != nil {
		return fmt.Errorf("write Codex config: %w", err)
	}
	return nil
}

func (codexDriver) Command(runner *Runner) (*exec.Cmd, error) {
	path := envOr("CODEX_ACP_PATH", "codex-acp")
	cmd := exec.Command(path)
	extra := map[string]string{
		"CODEX_API_KEY":      runner.proxyToken,
		"CODEX_HOME":         envOr("CODEX_HOME", "/tmp/codex-home"),
		"INITIAL_AGENT_MODE": "agent-full-access",
		"MODEL_PROVIDER":     "cloudflare_proxy",
		"NO_BROWSER":         "1",
	}
	cmd.Env = runner.childEnvironment(extra)
	return cmd, nil
}

func (codexDriver) ConfigureACP(context.Context, *Runner, *acp.ClientSideConnection) error {
	return nil
}

/** Converts Codex's extension metadata into provider-neutral error signals. */
func (codexDriver) AssessSessionUpdate(update acp.SessionUpdate) harnessSessionUpdateAssessment {
	if update.SessionInfoUpdate == nil {
		return harnessSessionUpdateAssessment{}
	}
	codex, ok := update.SessionInfoUpdate.Meta["codex"].(map[string]any)
	if !ok {
		return harnessSessionUpdateAssessment{}
	}
	assessment := harnessSessionUpdateAssessment{}
	if reported, ok := codex["error"].(map[string]any); ok {
		assessment.Error = true
		assessment.Diagnostic = metadataString(reported, "additionalDetails")
		if assessment.Diagnostic == "" {
			assessment.Diagnostic = metadataString(reported, "message")
		}
		if willRetry, present := reported["willRetry"].(bool); present && !willRetry {
			assessment.Terminal = true
		}
	}
	if status, ok := codex["threadStatus"].(map[string]any); ok && metadataString(status, "type") == "systemError" {
		assessment.Error = true
		assessment.Terminal = true
	}
	return assessment
}

func metadataString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

type grokDriver struct{}

func (grokDriver) Name() string { return "grok" }

func (grokDriver) Prepare(runner *Runner) error {
	home := envOr("GROK_HOME", "/tmp/grok-home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		return fmt.Errorf("create GROK_HOME: %w", err)
	}
	return nil
}

func (grokDriver) Command(runner *Runner) (*exec.Cmd, error) {
	path := envOr("GROK_PATH", "grok")
	args := []string{"agent"}
	if runner.harness.Model != nil {
		args = append(args, "--model", *runner.harness.Model)
	}
	if runner.harness.ReasoningEffort != nil {
		args = append(args, "--reasoning-effort", *runner.harness.ReasoningEffort)
	}
	args = append(args,
		"--always-approve",
		"--no-leader",
		"--xai-api-base-url", runner.harness.Provider.BaseURL,
		"stdio",
	)
	cmd := exec.Command(path, args...)
	overlay, err := json.Marshal(map[string]any{
		"features": map[string]bool{
			"telemetry":         false,
			"remote_fetch":      false,
			"ask_user_question": runner.allowUserInput,
			"title_refresh":     false,
			"session_recap":     false,
		},
		"session": map[string]bool{"load_envrc": false},
		"shell_environment_policy": map[string]any{
			"inherit":                 "all",
			"ignore_default_excludes": true,
			"exclude":                 []string{"XAI_API_KEY", "RUNNER_*"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("encode Grok configuration: %w", err)
	}
	extra := map[string]string{
		"GROK_CONFIG": string(overlay),
		"GROK_HOME":   envOr("GROK_HOME", "/tmp/grok-home"),
		"NO_BROWSER":  "1",
		"XAI_API_KEY": runner.proxyToken,
	}
	cmd.Env = runner.childEnvironment(extra)
	return cmd, nil
}

func (grokDriver) ConfigureACP(context.Context, *Runner, *acp.ClientSideConnection) error {
	return nil
}

type claudeCodeDriver struct{}

func (claudeCodeDriver) Name() string { return "claude-code" }

func (claudeCodeDriver) Prepare(runner *Runner) error {
	home := envOr("CLAUDE_CONFIG_DIR", "/tmp/claude-home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		return fmt.Errorf("create CLAUDE_CONFIG_DIR: %w", err)
	}
	settings := map[string]any{"permissions": map[string]string{"defaultMode": "bypassPermissions"}}
	data, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("encode Claude settings: %w", err)
	}
	if err := os.WriteFile(filepath.Join(home, "settings.json"), data, 0o600); err != nil {
		return fmt.Errorf("write Claude settings: %w", err)
	}
	return nil
}

func (claudeCodeDriver) Command(runner *Runner) (*exec.Cmd, error) {
	path := envOr("CLAUDE_ACP_PATH", "claude-agent-acp")
	cmd := exec.Command(path)
	baseURL := strings.TrimSuffix(runner.harness.Provider.BaseURL, "/")
	if runner.harness.Provider.Protocol == "anthropic" {
		baseURL = strings.TrimSuffix(baseURL, "/v1")
	}
	extra := map[string]string{
		"ANTHROPIC_AUTH_TOKEN": runner.proxyToken,
		"ANTHROPIC_BASE_URL":   baseURL,
		"CLAUDE_CODE_EXECUTABLE": envOr(
			"CLAUDE_CODE_EXECUTABLE",
			"/usr/local/bin/claude",
		),
		"CLAUDE_CONFIG_DIR":  envOr("CLAUDE_CONFIG_DIR", "/tmp/claude-home"),
		"CLAUDE_CODE_REMOTE": "1",
		"IS_SANDBOX":         "1",
		"NODE_USE_ENV_PROXY": "1",
		"NO_BROWSER":         "1",
	}
	if runner.caBundle != "" {
		extra["NODE_EXTRA_CA_CERTS"] = runner.caBundle
	}
	if len(runner.harness.Provider.StaticHeaders) > 0 {
		headers := make([]string, 0, len(runner.harness.Provider.StaticHeaders))
		for name, value := range runner.harness.Provider.StaticHeaders {
			headers = append(headers, name+": "+value)
		}
		extra["ANTHROPIC_CUSTOM_HEADERS"] = strings.Join(headers, "\n")
	}
	if runner.harness.Model != nil {
		model := *runner.harness.Model
		extra["ANTHROPIC_MODEL"] = model
		modelConfig, err := json.Marshal(map[string]any{"availableModels": []string{model}})
		if err != nil {
			return nil, fmt.Errorf("encode Claude model configuration: %w", err)
		}
		extra["CLAUDE_MODEL_CONFIG"] = string(modelConfig)
		if claudeCodeNeedsCustomModelOption(model) {
			extra["ANTHROPIC_CUSTOM_MODEL_OPTION"] = model
		}
	}
	if runner.harness.ReasoningEffort != nil {
		effort := *runner.harness.ReasoningEffort
		if !claudeCodeSupportsReasoningEffort(effort) {
			return nil, fmt.Errorf("claude-code does not support reasoning effort %q", effort)
		}
		extra["CLAUDE_CODE_EFFORT_LEVEL"] = effort
	}
	cmd.Env = runner.childEnvironment(extra)
	return cmd, nil
}

func claudeCodeSupportsReasoningEffort(effort string) bool {
	switch effort {
	case "low", "medium", "high", "xhigh", "max":
		return true
	default:
		return false
	}
}

/** Identifies model IDs that Claude Code may otherwise normalize to a semantic alias. */
func claudeCodeNeedsCustomModelOption(model string) bool {
	parts := strings.Split(strings.ToLower(model), "-")
	if len(parts) < 3 || parts[0] != "claude" {
		return false
	}
	if _, err := strconv.Atoi(parts[1]); err == nil {
		return false
	}
	major, err := strconv.Atoi(parts[2])
	if err != nil {
		return false
	}
	if major >= 5 {
		return true
	}
	if parts[1] != "opus" || major != 4 || len(parts) < 4 {
		return false
	}
	minor, err := strconv.Atoi(parts[3])
	return err == nil && minor >= 8
}

func (claudeCodeDriver) ConfigureACP(ctx context.Context, runner *Runner, connection *acp.ClientSideConnection) error {
	// Claude's ACP adapter currently follows the newer providerId draft while
	// acp-go-sdk follows the id draft. The native harness reads the equivalent
	// provider configuration from the environment prepared above, so avoid the
	// incompatible unstable providers/set extension until the schemas converge.
	_ = ctx
	_ = runner
	_ = connection
	return nil
}

type openCodeDriver struct{}

func (openCodeDriver) Name() string { return "opencode" }

func (openCodeDriver) Prepare(runner *Runner) error {
	home := envOr("OPENCODE_HOME", "/tmp/opencode-home")
	for _, directory := range []string{
		home,
		filepath.Join(home, "config"),
		filepath.Join(home, "data"),
		filepath.Join(home, "state"),
		filepath.Join(home, "cache"),
	} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("create OpenCode directory: %w", err)
		}
	}
	return nil
}

func (openCodeDriver) Command(runner *Runner) (*exec.Cmd, error) {
	configuration, err := renderOpenCodeConfig(runner.harness)
	if err != nil {
		return nil, err
	}
	home := envOr("OPENCODE_HOME", "/tmp/opencode-home")
	path := envOr("OPENCODE_PATH", "opencode")
	cmd := exec.Command(path, "acp", "--cwd", runner.workspace)
	extra := map[string]string{
		"NO_BROWSER":                      "1",
		"OPENCODE_AUTH_CONTENT":           "{}",
		"OPENCODE_CONFIG_CONTENT":         configuration,
		"OPENCODE_DISABLE_AUTOUPDATE":     "1",
		"OPENCODE_DISABLE_MODELS_FETCH":   "1",
		"OPENCODE_DISABLE_PROJECT_CONFIG": "1",
		"OPENCODE_HOME":                   home,
		"OPENCODE_PURE":                   "1",
		"RUNNER_PROVIDER_TOKEN":           runner.proxyToken,
		"XDG_CACHE_HOME":                  filepath.Join(home, "cache"),
		"XDG_CONFIG_HOME":                 filepath.Join(home, "config"),
		"XDG_DATA_HOME":                   filepath.Join(home, "data"),
		"XDG_STATE_HOME":                  filepath.Join(home, "state"),
	}
	cmd.Env = runner.childEnvironment(extra)
	return cmd, nil
}

func (openCodeDriver) ConfigureACP(context.Context, *Runner, *acp.ClientSideConnection) error {
	return nil
}

type piDriver struct{}

func (piDriver) Name() string { return "pi" }

func (piDriver) Prepare(runner *Runner) error {
	home := envOr("PI_CODING_AGENT_DIR", "/tmp/pi-home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		return fmt.Errorf("create Pi configuration directory: %w", err)
	}
	models, settings, err := renderPiConfig(runner.harness)
	if err != nil {
		return err
	}
	for name, data := range map[string][]byte{"models.json": models, "settings.json": settings} {
		if err := os.WriteFile(filepath.Join(home, name), data, 0o600); err != nil {
			return fmt.Errorf("write Pi %s: %w", name, err)
		}
	}
	return nil
}

func (piDriver) Command(runner *Runner) (*exec.Cmd, error) {
	path := envOr("PI_ACP_PATH", "pi-acp")
	cmd := exec.Command(path)
	extra := map[string]string{
		"NO_BROWSER":            "1",
		"NODE_USE_ENV_PROXY":    "1",
		"PI_CODING_AGENT_DIR":   envOr("PI_CODING_AGENT_DIR", "/tmp/pi-home"),
		"PI_OFFLINE":            "1",
		"PI_SKIP_VERSION_CHECK": "1",
		"PI_TELEMETRY":          "0",
		"RUNNER_PROVIDER_TOKEN": runner.proxyToken,
	}
	if runner.caBundle != "" {
		extra["NODE_EXTRA_CA_CERTS"] = runner.caBundle
	}
	cmd.Env = runner.childEnvironment(extra)
	return cmd, nil
}

func (piDriver) ConfigureACP(context.Context, *Runner, *acp.ClientSideConnection) error {
	return nil
}

/** Configures and verifies ACP-exposed reasoning controls before prompting. */
func (runner *Runner) configureReasoningEffort(ctx context.Context, connection *acp.ClientSideConnection, session acp.NewSessionResponse) error {
	if runner.harness.ReasoningEffort == nil {
		return nil
	}
	requested := *runner.harness.ReasoningEffort
	wireValue := reasoningEffortWireValue(runner.harness.Type, requested)
	option := reasoningConfigOption(session.ConfigOptions, runner.harness.Type)
	if option == nil {
		if runner.harness.Type == "grok" {
			return nil
		}
		return fmt.Errorf("%s did not advertise a reasoning configuration option for model %q", runner.harness.Type, stringPointerValue(runner.harness.Model))
	}
	if !configOptionSupports(option, wireValue) {
		return fmt.Errorf("%s model %q does not support reasoning effort %q", runner.harness.Type, stringPointerValue(runner.harness.Model), requested)
	}
	request := acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{
		SessionId: session.SessionId,
		ConfigId:  option.Id,
		Value:     acp.SessionConfigValueId(wireValue),
	}}
	runner.logACP("session/set_config_option", "client_to_agent", "request", request, "info")
	response, err := connection.SetSessionConfigOption(ctx, request)
	if err != nil {
		runner.logACP("session/set_config_option", "agent_to_client", "response", map[string]any{"error": err.Error()}, "error")
		return fmt.Errorf("set %s reasoning effort %q: %w", runner.harness.Type, requested, err)
	}
	runner.logACP("session/set_config_option", "agent_to_client", "response", response, "info")
	effective := configOptionByID(response.ConfigOptions, option.Id)
	if effective == nil || string(effective.CurrentValue) != wireValue {
		actual := "missing"
		if effective != nil {
			actual = string(effective.CurrentValue)
		}
		return fmt.Errorf("%s did not apply reasoning effort %q (effective value %q)", runner.harness.Type, requested, actual)
	}
	return nil
}

func reasoningEffortWireValue(harness, requested string) string {
	if harness == "pi" && requested == "none" {
		return "off"
	}
	return requested
}

func reasoningConfigOption(options []acp.SessionConfigOption, harness string) *acp.SessionConfigOptionSelect {
	ids := map[string]string{"codex": "reasoning_effort", "claude-code": "effort", "opencode": "effort", "pi": "thinkingLevel"}
	expectedID := ids[harness]
	for _, option := range options {
		if option.Select != nil && string(option.Select.Id) == expectedID {
			return option.Select
		}
	}
	for _, option := range options {
		if option.Select != nil && option.Select.Category != nil && *option.Select.Category == acp.SessionConfigOptionCategoryThoughtLevel {
			return option.Select
		}
	}
	return nil
}

func configOptionSupports(option *acp.SessionConfigOptionSelect, value string) bool {
	if option.Options.Ungrouped != nil {
		for _, candidate := range *option.Options.Ungrouped {
			if string(candidate.Value) == value {
				return true
			}
		}
	}
	if option.Options.Grouped != nil {
		for _, group := range *option.Options.Grouped {
			for _, candidate := range group.Options {
				if string(candidate.Value) == value {
					return true
				}
			}
		}
	}
	return false
}

func configOptionByID(options []acp.SessionConfigOption, id acp.SessionConfigId) *acp.SessionConfigOptionSelect {
	for _, option := range options {
		if option.Select != nil && option.Select.Id == id {
			return option.Select
		}
	}
	return nil
}

func stringPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

const codexConfigTemplate = `model_provider = "cloudflare_proxy"
approval_policy = "never"
sandbox_mode = "danger-full-access"
%s
[shell_environment_policy]
inherit = "all"
ignore_default_excludes = true

[shell_environment_policy.filters]
"CODEX_API_KEY" = "exclude"
"RUNNER_*" = "exclude"

[model_providers.cloudflare_proxy]
name = "Cloudflare Responses proxy"
base_url = %s
env_key = "CODEX_API_KEY"
wire_api = "responses"
requires_openai_auth = false
`

func renderCodexConfig(config ResolvedHarness, allowUserInput bool) string {
	model := ""
	if config.Model != nil {
		model = "model = " + strconv.Quote(*config.Model) + "\n"
	}
	features := "[features]\ndefault_mode_request_user_input = " + strconv.FormatBool(allowUserInput) + "\n\n"
	return fmt.Sprintf(codexConfigTemplate, model+features, strconv.Quote(config.Provider.BaseURL))
}

func renderOpenCodeConfig(config ResolvedHarness) (string, error) {
	if config.Model == nil {
		return "", fmt.Errorf("OpenCode requires a configured model")
	}
	providerID := "runner"
	npmPackage := "@ai-sdk/openai-compatible"
	credentialOption := "apiKey"
	baseURL := config.Provider.BaseURL
	switch config.Provider.Protocol {
	case "openai-responses":
		providerID = "openai"
		npmPackage = "@ai-sdk/openai"
	case "anthropic":
		providerID = "anthropic"
		npmPackage = "@ai-sdk/anthropic"
		credentialOption = "authToken"
		// The AI SDK's Anthropic provider appends /messages, while Claude Code
		// appends /v1/messages to the same configured provider base URL.
		baseURL = strings.TrimSuffix(baseURL, "/")
		if !strings.HasSuffix(baseURL, "/v1") {
			baseURL += "/v1"
		}
	case "openai-compatible":
	default:
		return "", fmt.Errorf("unsupported OpenCode provider protocol %q", config.Provider.Protocol)
	}
	options := map[string]any{
		"baseURL":        baseURL,
		credentialOption: "{env:RUNNER_PROVIDER_TOKEN}",
	}
	if len(config.Provider.StaticHeaders) > 0 {
		options["headers"] = config.Provider.StaticHeaders
	}
	selectedModel := providerID + "/selected"
	payload := map[string]any{
		"$schema":           "https://opencode.ai/config.json",
		"autoupdate":        false,
		"enabled_providers": []string{providerID},
		"model":             selectedModel,
		"small_model":       selectedModel,
		"permission":        map[string]string{"*": "allow", "question": "deny"},
		"share":             "disabled",
		"provider": map[string]any{
			providerID: map[string]any{
				"name":    config.ProviderName,
				"npm":     npmPackage,
				"options": options,
				"models": map[string]any{
					"selected": map[string]any{"id": *config.Model, "name": *config.Model, "reasoning": true},
				},
			},
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode OpenCode configuration: %w", err)
	}
	return string(data), nil
}

func renderPiConfig(config ResolvedHarness) ([]byte, []byte, error) {
	if config.Model == nil {
		return nil, nil, fmt.Errorf("Pi requires a configured model")
	}
	api := ""
	baseURL := strings.TrimSuffix(config.Provider.BaseURL, "/")
	switch config.Provider.Protocol {
	case "xai", "openai-responses":
		api = "openai-responses"
	case "anthropic":
		api = "anthropic-messages"
		baseURL = strings.TrimSuffix(baseURL, "/v1")
	case "openai-compatible":
		api = "openai-completions"
	default:
		return nil, nil, fmt.Errorf("unsupported Pi provider protocol %q", config.Provider.Protocol)
	}
	provider := map[string]any{
		"name":       config.ProviderName,
		"baseUrl":    baseURL,
		"api":        api,
		"apiKey":     "$RUNNER_PROVIDER_TOKEN",
		"authHeader": true,
		"models": []map[string]any{{
			"id":        *config.Model,
			"name":      *config.Model,
			"reasoning": true,
		}},
	}
	if len(config.Provider.StaticHeaders) > 0 {
		provider["headers"] = config.Provider.StaticHeaders
	}
	models, err := json.Marshal(map[string]any{"providers": map[string]any{"runner": provider}})
	if err != nil {
		return nil, nil, fmt.Errorf("encode Pi model configuration: %w", err)
	}
	settings, err := json.Marshal(map[string]any{
		"defaultProvider":        "runner",
		"defaultModel":           *config.Model,
		"enableInstallTelemetry": false,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("encode Pi settings: %w", err)
	}
	return models, settings, nil
}

func (runner *Runner) childEnvironment(extra map[string]string) []string {
	inherited := []string{
		"PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR", "TMP", "TEMP",
		"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy", "SSL_CERT_DIR",
	}
	environment := make([]string, 0, len(inherited)+len(runner.config.RuntimeEnv)+len(extra)+len(runner.config.CredentialEnv)+3)
	for _, name := range inherited {
		if value, ok := os.LookupEnv(name); ok {
			environment = append(environment, name+"="+value)
		}
	}
	for _, name := range runner.config.RuntimeEnv {
		if value, ok := os.LookupEnv(name); ok {
			environment = append(environment, name+"="+value)
		}
	}
	environment = append(environment, "HOME=/home/runner", "RUNNER_JOB_ID="+runner.logger.jobID)
	if runner.metrics != nil {
		environment = append(environment, metricEndpointEnvironment+"="+runner.metrics.URL)
	}
	if runner.config.Workflow != nil && runner.config.Workflow.MemoryEnabled {
		environment = append(environment, "AGENT_MEMORY_DB="+filepath.Join(runner.workspace, "memory.sqlite3"))
	}
	for _, name := range runner.config.CredentialEnv {
		environment = append(environment, name+"="+runner.proxyToken)
	}
	if runner.caBundle != "" {
		environment = append(environment, "SSL_CERT_FILE="+runner.caBundle)
	}
	for name, value := range extra {
		environment = append(environment, name+"="+value)
	}
	return environment
}
