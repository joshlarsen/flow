package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigAndResolveWorkflowStep(t *testing.T) {
	effort := "high"
	configValue := testConfig()
	configValue.Workflow.Steps[0].ReasoningEffort = &effort
	data, _ := json.Marshal(configValue)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := config.resolveStep(config.Workflow.Steps[0])
	if err != nil || resolved.Type != "grok" || resolved.Model == nil || *resolved.Model != "grok-4.6" || resolved.Provider.Protocol != "xai" || resolved.ReasoningEffort == nil || *resolved.ReasoningEffort != "high" {
		t.Fatalf("unexpected resolved step: %+v, %v", resolved, err)
	}
}

func TestLoadConfigValidatesStructuredRequiredMetrics(t *testing.T) {
	valid := testConfig()
	valid.Workflow.Steps[0].RequiredMetrics = []RequiredMetric{{
		Namespace: "haiku", Key: "num_lines", Description: "number of lines",
	}}
	data, _ := json.Marshal(valid)
	if _, err := decodeConfig(data); err != nil {
		t.Fatalf("expected structured required metric to pass: %v", err)
	}

	invalid := []RequiredMetric{{Namespace: "haiku", Key: "num_lines", Description: "number of lines"}, {Namespace: "haiku", Key: "num_lines", Description: "duplicate"}}
	valid.Workflow.Steps[0].RequiredMetrics = invalid
	data, _ = json.Marshal(valid)
	if _, err := decodeConfig(data); err == nil {
		t.Fatal("expected duplicate required metric to fail")
	}

	var legacy map[string]any
	if err := json.Unmarshal(data, &legacy); err != nil {
		t.Fatal(err)
	}
	workflow := legacy["workflow"].(map[string]any)
	steps := workflow["steps"].([]any)
	steps[0].(map[string]any)["required_metrics"] = []any{"haiku.num_lines"}
	legacyData, _ := json.Marshal(legacy)
	if _, err := decodeConfig(legacyData); err == nil {
		t.Fatal("expected legacy required metric strings to fail")
	}
}

func TestLoadConfigRejectsInvalidReasoningEffort(t *testing.T) {
	invalid := "extreme"
	config := testConfig()
	config.Workflow.Steps[0].ReasoningEffort = &invalid
	data, _ := json.Marshal(config)
	if _, err := decodeConfig(data); err == nil || !strings.Contains(err.Error(), "reasoning_effort") {
		t.Fatalf("expected invalid reasoning effort error, got %v", err)
	}

	config = testConfig()
	config.Workflow.Steps = []WorkflowStep{{ID: "check", Command: []string{"true"}, ReasoningEffort: stringPointer("high"), TimeoutMS: 1000}}
	data, _ = json.Marshal(config)
	if _, err := decodeConfig(data); err == nil {
		t.Fatal("expected command reasoning effort to fail")
	}
}

func TestLoadConfigAcceptsCommandsAnywhereAndCommandOnlyWorkflows(t *testing.T) {
	config := testConfig()
	agent := config.Workflow.Steps[0]
	config.Workflow.Steps = []WorkflowStep{
		{ID: "preflight", Command: []string{"curl", "--fail", "https://example.com"}, TimeoutMS: 1000},
		agent,
		{ID: "verify", Command: []string{"test", "-f", "output/result.json"}, TimeoutMS: 1000},
	}
	data, _ := json.Marshal(config)
	if _, err := decodeConfig(data); err != nil {
		t.Fatal(err)
	}
	config.Workflow.Steps = []WorkflowStep{{ID: "check", Command: []string{"true"}, TimeoutMS: 1000}}
	data, _ = json.Marshal(config)
	if _, err := decodeConfig(data); err != nil {
		t.Fatal(err)
	}
}

func TestLoadConfigRejectsInvalidCommandWorkflows(t *testing.T) {
	tests := []struct {
		name  string
		steps []WorkflowStep
	}{
		{"blank executable", []WorkflowStep{{ID: "check", Command: []string{" "}, TimeoutMS: 1000}, testConfig().Workflow.Steps[0]}},
		{"mixed fields", []WorkflowStep{{ID: "check", Command: []string{"true"}, Prompt: "foo.md", TimeoutMS: 1000}, testConfig().Workflow.Steps[0]}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig()
			config.Workflow.Steps = test.steps
			data, _ := json.Marshal(config)
			if _, err := decodeConfig(data); err == nil {
				t.Fatal("expected invalid command workflow to fail")
			}
		})
	}
}

func TestLoadConfigRequiresCallbackInteractionsForUserInput(t *testing.T) {
	config := testConfig()
	config.Workflow.Steps[0].AllowUserInput = true
	data, _ := json.Marshal(config)
	if _, err := decodeConfig(data); err == nil || !strings.Contains(err.Error(), "allows user input") {
		t.Fatalf("expected user input provider error, got %v", err)
	}
	config.Interactions = InteractionsConfig{
		Provider: "callback", LiveWaitTimeoutMS: 1000, MaxRequestBytes: 4096, MaxResponseBytes: 4096,
		CheckpointMaxFiles: 1, CheckpointMaxFileBytes: 1024, CheckpointMaxTotalBytes: 1024,
	}
	data, _ = json.Marshal(config)
	if _, err := decodeConfig(data); err != nil {
		t.Fatalf("expected callback interactions to allow user input: %v", err)
	}
	for _, kind := range []string{"pi", "opencode"} {
		unsupported := config
		unsupported.Harnesses = map[string]HarnessConfig{"unsupported": {Type: kind}}
		unsupported.Workflow = &WorkflowConfig{Name: config.Workflow.Name, BundleDigest: config.Workflow.BundleDigest, Steps: append([]WorkflowStep(nil), config.Workflow.Steps...)}
		unsupported.Workflow.Steps[0].Harness = "unsupported"
		data, _ = json.Marshal(unsupported)
		if _, err := decodeConfig(data); err == nil || !strings.Contains(err.Error(), "unsupported harness") {
			t.Fatalf("expected %s user input to be rejected, got %v", kind, err)
		}
	}

	config.Workflow.Steps = []WorkflowStep{{ID: "check", Command: []string{"true"}, AllowUserInput: true, TimeoutMS: 1000}}
	data, _ = json.Marshal(config)
	if _, err := decodeConfig(data); err == nil {
		t.Fatal("expected command user input opt-in to fail")
	}
}

func TestLoadConfigRejectsMissingValues(t *testing.T) {
	if _, err := decodeConfig([]byte(`{"port":8080}`)); err == nil {
		t.Fatal("expected invalid config to fail")
	}
}

func TestLoadRuntimeConfigFromEnvironment(t *testing.T) {
	data, _ := json.Marshal(testConfig())
	t.Setenv("RUNNER_CONFIG_JSON", string(data))
	for _, name := range testConfig().RuntimeEnv {
		t.Setenv(name, "test")
	}
	if _, err := loadRuntimeConfig(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadConfigRejectsInvalidRuntimeEnvironment(t *testing.T) {
	for _, invalid := range [][]string{{"SERVICE_URL", "SERVICE_URL"}, {"RUNNER_EGRESS_TOKEN"}, {"lowercase"}, {"GH_TOKEN"}} {
		config := testConfig()
		config.RuntimeEnv = invalid
		data, _ := json.Marshal(config)
		if _, err := decodeConfig(data); err == nil {
			t.Fatalf("expected runtime environment %v to fail", invalid)
		}
	}
}

func TestLoadRuntimeConfigRequiresDeclaredEnvironment(t *testing.T) {
	config := testConfig()
	config.RuntimeEnv = []string{"SERVICE_URL"}
	data, _ := json.Marshal(config)
	t.Setenv("RUNNER_CONFIG_JSON", string(data))
	if _, err := loadRuntimeConfig(); err == nil || !strings.Contains(err.Error(), "SERVICE_URL") {
		t.Fatalf("expected missing runtime environment error, got %v", err)
	}
	t.Setenv("SERVICE_URL", "https://service.example")
	if _, err := loadRuntimeConfig(); err != nil {
		t.Fatalf("expected configured runtime environment, got %v", err)
	}
}

func TestLoadConfigRejectsIncompatibleProtocolAndModelMapping(t *testing.T) {
	config := testConfig()
	config.Harnesses["grok"] = HarnessConfig{Type: "codex"}
	data, _ := json.Marshal(config)
	if _, err := decodeConfig(data); err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("expected protocol error, got %v", err)
	}

	config = testConfig()
	config.Workflow.Steps[0].ModelID = "different"
	data, _ = json.Marshal(config)
	if _, err := decodeConfig(data); err == nil || !strings.Contains(err.Error(), "model mapping") {
		t.Fatalf("expected model mapping error, got %v", err)
	}
}

func TestLoadConfigRequiresResponsesForOpenAIModels(t *testing.T) {
	config := testConfig()
	config.Providers["xai"] = ProviderConfig{Protocol: "openai-compatible", BaseURL: "https://example.com/v1"}
	config.Models["grok-4.6"]["xai"] = "gpt-5.6-terra"
	config.Workflow.Steps[0].ModelID = "gpt-5.6-terra"
	data, _ := json.Marshal(config)
	if _, err := decodeConfig(data); err == nil || !strings.Contains(err.Error(), "openai-responses") {
		t.Fatalf("expected Responses-only error, got %v", err)
	}
}

func TestPiSupportsEveryConfiguredProviderProtocol(t *testing.T) {
	for _, protocol := range []string{"xai", "openai-responses", "anthropic", "openai-compatible"} {
		if err := validateHarness(HarnessConfig{Type: "pi"}, ProviderConfig{Protocol: protocol}, "provider-model"); err != nil {
			t.Fatalf("expected Pi to support %s: %v", protocol, err)
		}
	}
}

func TestLoadConfigRejectsInvalidCredentialEnvironment(t *testing.T) {
	for _, invalid := range [][]string{{"GH_TOKEN", "GH_TOKEN"}, {"RUNNER_EGRESS_TOKEN"}, {"lowercase"}} {
		config := testConfig()
		config.CredentialEnv = invalid
		data, _ := json.Marshal(config)
		if _, err := decodeConfig(data); err == nil {
			t.Fatalf("expected credential environment %v to fail", invalid)
		}
	}
}
