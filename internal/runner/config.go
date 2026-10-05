package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
)

type Config struct {
	Port                  int                          `json:"port"`
	WorkflowTimeoutMS     int64                        `json:"workflow_timeout_ms"`
	DefaultStepTimeoutMS  int64                        `json:"default_step_timeout_ms"`
	ShutdownGraceMS       int64                        `json:"shutdown_grace_ms"`
	MaxPromptBytes        int                          `json:"max_prompt_bytes"`
	MaxPromptFiles        int                          `json:"max_prompt_files"`
	MaxPromptBundleBytes  int                          `json:"max_prompt_bundle_bytes"`
	MaxAssetBytes         int                          `json:"max_asset_bytes"`
	MaxAssetFiles         int                          `json:"max_asset_files"`
	MaxAssetBundleBytes   int                          `json:"max_asset_bundle_bytes"`
	MaxResultBytes        int                          `json:"max_result_bytes"`
	MaxArtifactFiles      int                          `json:"max_artifact_files"`
	MaxArtifactFileBytes  int64                        `json:"max_artifact_file_bytes"`
	MaxArtifactTotalBytes int64                        `json:"max_artifact_total_bytes"`
	MaxMemoryBytes        int64                        `json:"max_memory_bytes"`
	MemoryPersistenceMS   int64                        `json:"memory_persistence_timeout_ms"`
	MaxLogBytes           int                          `json:"max_log_bytes"`
	Providers             map[string]ProviderConfig    `json:"providers"`
	Models                map[string]map[string]string `json:"models"`
	Harnesses             map[string]HarnessConfig     `json:"harnesses"`
	Workflow              *WorkflowConfig              `json:"workflow"`
	Interactions          InteractionsConfig           `json:"interactions"`
	CredentialEnv         []string                     `json:"credential_environment"`
	RuntimeEnv            []string                     `json:"runtime_environment"`
}

type ProviderConfig struct {
	Protocol      string            `json:"protocol"`
	BaseURL       string            `json:"base_url"`
	StaticHeaders map[string]string `json:"static_headers"`
}

type HarnessConfig struct {
	Type string `json:"type"`
}

type WorkflowConfig struct {
	Name          string         `json:"name"`
	BundleDigest  string         `json:"bundle_digest"`
	MemoryEnabled bool           `json:"memory_enabled"`
	TokenBudget   *TokenBudget   `json:"token_budget,omitempty"`
	Steps         []WorkflowStep `json:"steps"`
}

type TokenBudget struct {
	Limit  int64  `json:"limit"`
	Period string `json:"period"`
}

type WorkflowStep struct {
	ID              string           `json:"id"`
	Prompt          string           `json:"prompt,omitempty"`
	AllowUserInput  bool             `json:"allow_user_input,omitempty"`
	Harness         string           `json:"harness,omitempty"`
	Provider        string           `json:"provider,omitempty"`
	Model           string           `json:"model,omitempty"`
	ModelID         string           `json:"model_id,omitempty"`
	ReasoningEffort *string          `json:"reasoning_effort"`
	Command         []string         `json:"command,omitempty"`
	RequiredMetrics []RequiredMetric `json:"required_metrics,omitempty"`
	TimeoutMS       int64            `json:"timeout_ms"`
}

type RequiredMetric struct {
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
	Description string `json:"description"`
}

func (metric RequiredMetric) Name() string {
	return metric.Namespace + "." + metric.Key
}

type ResolvedHarness struct {
	Type            string
	ProviderName    string
	Provider        ProviderConfig
	Model           *string
	ReasoningEffort *string
}

type InteractionsConfig struct {
	Provider                string `json:"provider"`
	LiveWaitTimeoutMS       int64  `json:"live_wait_timeout_ms,omitempty"`
	MaxRequestBytes         int    `json:"max_request_bytes,omitempty"`
	MaxResponseBytes        int    `json:"max_response_bytes,omitempty"`
	CheckpointMaxFiles      int    `json:"checkpoint_max_files,omitempty"`
	CheckpointMaxFileBytes  int64  `json:"checkpoint_max_file_bytes,omitempty"`
	CheckpointMaxTotalBytes int64  `json:"checkpoint_max_total_bytes,omitempty"`
}

var profileNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var modelNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var promptPathPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*\.md$`)
var openAIModelPattern = regexp.MustCompile(`(?i)^(openai/|gpt-|o[0-9])`)
var environmentNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
var reasoningEfforts = map[string]bool{"none": true, "minimal": true, "low": true, "medium": true, "high": true, "xhigh": true, "max": true}
var metricPartPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

var reservedCredentialEnvironmentNames = map[string]bool{
	"HOME": true, "CODEX_HOME": true, "GROK_HOME": true, "CLAUDE_CONFIG_DIR": true, "OPENCODE_HOME": true, "PI_CODING_AGENT_DIR": true,
	"CODEX_API_KEY": true, "OPENAI_API_KEY": true, "XAI_API_KEY": true, "ANTHROPIC_API_KEY": true, "ANTHROPIC_AUTH_TOKEN": true,
	"CODEX_PROXY_TOKEN": true, "RUNNER_EGRESS_TOKEN": true, "RUNNER_PROVIDER_TOKEN": true,
	"RUNNER_CONFIG": true, "RUNNER_CONFIG_JSON": true, "OPENCODE_CONFIG_CONTENT": true, "OPENCODE_AUTH_CONTENT": true,
	"SSL_CERT_FILE": true, "NODE_EXTRA_CA_CERTS": true, "NODE_USE_ENV_PROXY": true, "GROK_CONFIG": true, "MODEL_PROVIDER": true, "ANTHROPIC_BASE_URL": true,
	"ANTHROPIC_CUSTOM_HEADERS": true, "ANTHROPIC_MODEL": true, "ANTHROPIC_CUSTOM_MODEL_OPTION": true,
	"ANTHROPIC_CUSTOM_MODEL_OPTION_SUPPORTED_CAPABILITIES": true, "CLAUDE_MODEL_CONFIG": true, "CLAUDE_CODE_EXECUTABLE": true,
	"CLAUDE_CODE_ALLOW_MODEL_CAPABILITY_OVERRIDES": true, "CLAUDE_CODE_EFFORT_LEVEL": true,
	"INITIAL_AGENT_MODE": true, "NO_BROWSER": true, "PI_ACP_PATH": true, "PI_OFFLINE": true,
	"PI_SKIP_VERSION_CHECK": true, "PI_TELEMETRY": true, "RUNNER_JOB_ID": true, "AGENT_MEMORY_DB": true,
	"FLOW_METRIC_URL": true,
	"PATH":            true, "LANG": true, "LC_ALL": true, "LC_CTYPE": true, "TZ": true, "TMPDIR": true, "TMP": true, "TEMP": true,
	"HTTP_PROXY": true, "HTTPS_PROXY": true, "NO_PROXY": true, "SSL_CERT_DIR": true,
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	return decodeConfig(data)
}

func loadRuntimeConfig() (Config, error) {
	var config Config
	var err error
	if value := os.Getenv("RUNNER_CONFIG_JSON"); value != "" {
		config, err = decodeConfig([]byte(value))
	} else {
		config, err = loadConfig(envOr("RUNNER_CONFIG", "/etc/agent-runner/config.json"))
	}
	if err != nil {
		return Config{}, err
	}
	for _, name := range config.RuntimeEnv {
		if value, ok := os.LookupEnv(name); !ok || strings.TrimSpace(value) == "" {
			return Config{}, fmt.Errorf("plaintext environment variable %q is not configured", name)
		}
	}
	return config, nil
}

func decodeConfig(data []byte) (Config, error) {
	var config Config
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if config.Port < 1 || config.Port > 65535 || config.WorkflowTimeoutMS < 1 || config.DefaultStepTimeoutMS < 1 ||
		config.DefaultStepTimeoutMS > config.WorkflowTimeoutMS || config.ShutdownGraceMS < 1 || config.MaxPromptBytes < 1 || config.MaxPromptFiles < 1 || config.MaxPromptBundleBytes < config.MaxPromptBytes ||
		config.MaxAssetBytes < 1 || config.MaxAssetFiles < 1 || config.MaxAssetBundleBytes < config.MaxAssetBytes ||
		config.MaxResultBytes < 1024 || config.MaxLogBytes < 1024 || config.MaxArtifactFiles < 1 ||
		config.MaxArtifactFileBytes < 1 || config.MaxArtifactTotalBytes < config.MaxArtifactFileBytes ||
		config.MaxMemoryBytes < 4096 || config.MemoryPersistenceMS < 1 {
		return Config{}, fmt.Errorf("config contains invalid or missing limits")
	}
	if len(config.Providers) == 0 || len(config.Models) == 0 || len(config.Harnesses) == 0 {
		return Config{}, fmt.Errorf("config must define providers, models, and harnesses")
	}
	for name, provider := range config.Providers {
		if !profileNamePattern.MatchString(name) {
			return Config{}, fmt.Errorf("config provider name %q is invalid", name)
		}
		if err := validateProvider(provider); err != nil {
			return Config{}, fmt.Errorf("config provider %q: %w", name, err)
		}
	}
	for name, harness := range config.Harnesses {
		if !profileNamePattern.MatchString(name) {
			return Config{}, fmt.Errorf("config harness name %q is invalid", name)
		}
		if err := validateHarnessType(harness.Type); err != nil {
			return Config{}, fmt.Errorf("config harness %q: %w", name, err)
		}
	}
	for modelName, providers := range config.Models {
		if !modelNamePattern.MatchString(modelName) || len(providers) == 0 {
			return Config{}, fmt.Errorf("config model %q is invalid", modelName)
		}
		for providerName, modelID := range providers {
			provider, ok := config.Providers[providerName]
			if !ok || strings.TrimSpace(modelID) == "" {
				return Config{}, fmt.Errorf("config model %q has invalid provider %q", modelName, providerName)
			}
			if provider.Protocol == "openai-compatible" && openAIModelPattern.MatchString(modelID) {
				return Config{}, fmt.Errorf("OpenAI-family models must use an openai-responses provider")
			}
		}
	}
	if config.Workflow == nil {
		return Config{}, fmt.Errorf("config workflow is required")
	}
	if err := config.validateWorkflow(); err != nil {
		return Config{}, err
	}
	if config.Interactions.Provider != "none" && config.Interactions.Provider != "callback" {
		return Config{}, fmt.Errorf("config interaction provider must be none or callback")
	}
	if config.Interactions.Provider == "callback" && (config.Interactions.LiveWaitTimeoutMS < 1 || config.Interactions.LiveWaitTimeoutMS > 60_000 || config.Interactions.MaxRequestBytes < 1024 || config.Interactions.MaxResponseBytes < 1024) {
		return Config{}, fmt.Errorf("config callback interaction limits are invalid")
	}
	if (config.Interactions.Provider == "callback" || config.Workflow.TokenBudget != nil) && (config.Interactions.CheckpointMaxFiles < 1 || config.Interactions.CheckpointMaxFileBytes < 1 || config.Interactions.CheckpointMaxTotalBytes < config.Interactions.CheckpointMaxFileBytes) {
		return Config{}, fmt.Errorf("config checkpoint limits are invalid")
	}
	credentialEnvironmentNames := map[string]bool{}
	for _, name := range config.CredentialEnv {
		if !environmentNamePattern.MatchString(name) || reservedCredentialEnvironmentNames[name] || credentialEnvironmentNames[name] {
			return Config{}, fmt.Errorf("credential environment variable %q is invalid, reserved, or duplicated", name)
		}
		credentialEnvironmentNames[name] = true
	}
	runtimeEnvironmentNames := map[string]bool{}
	for _, name := range config.RuntimeEnv {
		if !environmentNamePattern.MatchString(name) || reservedCredentialEnvironmentNames[name] || credentialEnvironmentNames[name] || runtimeEnvironmentNames[name] {
			return Config{}, fmt.Errorf("runtime environment variable %q is invalid, reserved, or duplicated", name)
		}
		runtimeEnvironmentNames[name] = true
	}
	return config, nil
}

func (config Config) validateWorkflow() error {
	workflow := config.Workflow
	if !profileNamePattern.MatchString(workflow.Name) || !digestPattern.MatchString(workflow.BundleDigest) || len(workflow.Steps) == 0 || len(workflow.Steps) > 100 {
		return fmt.Errorf("config workflow is invalid")
	}
	if workflow.TokenBudget != nil && (workflow.TokenBudget.Limit < 1 || workflow.TokenBudget.Period != "day" && workflow.TokenBudget.Period != "week") {
		return fmt.Errorf("config workflow token budget is invalid")
	}
	seen := map[string]bool{}
	for index, step := range workflow.Steps {
		if !profileNamePattern.MatchString(step.ID) || seen[step.ID] || step.TimeoutMS < 1 || step.TimeoutMS > config.WorkflowTimeoutMS {
			return fmt.Errorf("config workflow step %d contains invalid fields", index)
		}
		seen[step.ID] = true
		if step.Command != nil {
			if step.Prompt != "" || step.AllowUserInput || step.Harness != "" || step.Provider != "" || step.Model != "" || step.ModelID != "" || step.ReasoningEffort != nil || len(step.RequiredMetrics) > 0 || len(step.Command) < 1 || len(step.Command) > 64 || strings.TrimSpace(step.Command[0]) == "" {
				return fmt.Errorf("config workflow command step %q contains invalid fields", step.ID)
			}
			for _, argument := range step.Command {
				if len([]byte(argument)) > 4096 {
					return fmt.Errorf("config workflow command step %q contains an oversized argument", step.ID)
				}
			}
			continue
		}
		if !safePromptPath(step.Prompt) {
			return fmt.Errorf("config workflow step %q has an invalid prompt", step.ID)
		}
		if step.AllowUserInput && config.Interactions.Provider != "callback" {
			return fmt.Errorf("config workflow step %q allows user input without callback interactions", step.ID)
		}
		if step.ReasoningEffort != nil && !reasoningEfforts[*step.ReasoningEffort] {
			return fmt.Errorf("config workflow step %q has an invalid reasoning_effort", step.ID)
		}
		if len(step.RequiredMetrics) > 100 {
			return fmt.Errorf("config workflow step %q has too many required metrics", step.ID)
		}
		metricNames := map[string]bool{}
		for _, metric := range step.RequiredMetrics {
			name := metric.Name()
			description := strings.TrimSpace(metric.Description)
			if !metricPartPattern.MatchString(metric.Namespace) || !metricPartPattern.MatchString(metric.Key) || description == "" || description != metric.Description || strings.ContainsAny(description, "\r\n") || len([]byte(description)) > 512 || metricNames[name] {
				return fmt.Errorf("config workflow step %q has an invalid or duplicate required metric", step.ID)
			}
			metricNames[name] = true
		}
		harness, ok := config.Harnesses[step.Harness]
		if !ok {
			return fmt.Errorf("config workflow step %q references undefined harness %q", step.ID, step.Harness)
		}
		if step.AllowUserInput && (harness.Type == "pi" || harness.Type == "opencode") {
			return fmt.Errorf("config workflow step %q uses allow_user_input with unsupported harness %q", step.ID, harness.Type)
		}
		provider, ok := config.Providers[step.Provider]
		if !ok {
			return fmt.Errorf("config workflow step %q references undefined provider %q", step.ID, step.Provider)
		}
		providers, ok := config.Models[step.Model]
		if !ok || providers[step.Provider] != step.ModelID {
			return fmt.Errorf("config workflow step %q has invalid model mapping", step.ID)
		}
		if err := validateHarness(HarnessConfig{Type: harness.Type}, provider, step.ModelID); err != nil {
			return fmt.Errorf("config workflow step %q: %w", step.ID, err)
		}
	}
	return nil
}

func safePromptPath(value string) bool {
	if !promptPathPattern.MatchString(value) || strings.Contains(value, "\\") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." || strings.HasPrefix(part, ".") {
			return false
		}
	}
	return true
}

func validateProvider(provider ProviderConfig) error {
	switch provider.Protocol {
	case "xai", "openai-responses", "anthropic", "openai-compatible":
	default:
		return fmt.Errorf("unsupported protocol %q", provider.Protocol)
	}
	baseURL, err := url.Parse(provider.BaseURL)
	if err != nil || baseURL.Scheme != "https" || baseURL.Host == "" || baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return fmt.Errorf("base_url must be an HTTPS URL without credentials, query, or fragment")
	}
	for name := range provider.StaticHeaders {
		switch strings.ToLower(name) {
		case "authorization", "proxy-authorization", "cookie", "x-api-key", "api-key", "cf-aig-authorization":
			return fmt.Errorf("static header %q is credential-bearing", name)
		}
	}
	return nil
}

func validateHarnessType(kind string) error {
	switch kind {
	case "grok", "codex", "claude-code", "opencode", "pi":
		return nil
	default:
		return fmt.Errorf("unsupported type %q", kind)
	}
}

func validateHarness(harness HarnessConfig, provider ProviderConfig, model string) error {
	allowed := false
	switch harness.Type {
	case "grok":
		allowed = provider.Protocol == "xai"
	case "codex":
		allowed = provider.Protocol == "openai-responses"
	case "claude-code":
		allowed = provider.Protocol == "anthropic"
	case "opencode":
		allowed = provider.Protocol == "openai-responses" || provider.Protocol == "anthropic" || provider.Protocol == "openai-compatible"
	case "pi":
		allowed = provider.Protocol == "xai" || provider.Protocol == "openai-responses" || provider.Protocol == "anthropic" || provider.Protocol == "openai-compatible"
	default:
		return validateHarnessType(harness.Type)
	}
	if !allowed {
		return fmt.Errorf("type %s does not support provider protocol %s", harness.Type, provider.Protocol)
	}
	if strings.TrimSpace(model) == "" {
		return fmt.Errorf("model must be non-empty")
	}
	return nil
}

func (config Config) resolveStep(step WorkflowStep) (ResolvedHarness, error) {
	if step.Command != nil {
		return ResolvedHarness{}, fmt.Errorf("command step %q does not select a harness", step.ID)
	}
	harness, ok := config.Harnesses[step.Harness]
	if !ok {
		return ResolvedHarness{}, fmt.Errorf("harness %q is not defined", step.Harness)
	}
	provider, ok := config.Providers[step.Provider]
	if !ok {
		return ResolvedHarness{}, fmt.Errorf("provider %q is not defined", step.Provider)
	}
	if err := validateHarness(harness, provider, step.ModelID); err != nil {
		return ResolvedHarness{}, err
	}
	model := step.ModelID
	return ResolvedHarness{Type: harness.Type, ProviderName: step.Provider, Provider: provider, Model: &model, ReasoningEffort: step.ReasoningEffort}, nil
}
