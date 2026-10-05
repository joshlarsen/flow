package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func stageSupervisor(t *testing.T, supervisor *Supervisor) {
	t.Helper()
	content := "hello"
	script := "#!/bin/sh\necho ok\n"
	skill := "# Test skill\n"
	fileDigest := sha256.Sum256([]byte(content))
	scriptDigest := sha256.Sum256([]byte(script))
	skillDigest := sha256.Sum256([]byte(skill))
	var archive bytes.Buffer
	gzipWriter := gzip.NewWriter(&archive)
	tarWriter := tar.NewWriter(gzipWriter)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "prompts", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.WriteHeader(&tar.Header{Name: "scripts", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.WriteHeader(&tar.Header{Name: "skills", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.WriteHeader(&tar.Header{Name: "prompts/foo.md", Typeflag: tar.TypeReg, Mode: 0o444, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.WriteHeader(&tar.Header{Name: "scripts/check.sh", Typeflag: tar.TypeReg, Mode: 0o555, Size: int64(len(script))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte(script)); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.WriteHeader(&tar.Header{Name: "skills/example/SKILL.md", Typeflag: tar.TypeReg, Mode: 0o444, Size: int64(len(skill))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte(skill)); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	archiveDigest := sha256.Sum256(archive.Bytes())
	archiveHash := hex.EncodeToString(archiveDigest[:])
	manifest := StageManifest{
		Version: 1, Digest: supervisor.config.Workflow.BundleDigest, SortKey: "20260903T214512.347Z", Workflow: json.RawMessage(`{"version":1}`),
		Archive: BundleArchive{Key: "bundles/20260903T214512.347Z-" + supervisor.config.Workflow.BundleDigest[:12] + "/bundle.tgz", Size: int64(archive.Len()), SHA256: archiveHash},
		Files: []BundleFile{
			{Kind: "prompt", Path: "foo.md", Size: int64(len(content)), SHA256: hex.EncodeToString(fileDigest[:])},
			{Kind: "script", Path: "check.sh", Size: int64(len(script)), SHA256: hex.EncodeToString(scriptDigest[:]), Executable: true},
			{Kind: "skill", Path: "example/SKILL.md", Size: int64(len(skill)), SHA256: hex.EncodeToString(skillDigest[:])},
		}, TotalBytes: int64(len(content) + len(script) + len(skill)),
	}
	body, _ := json.Marshal(manifest)
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/stage", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("stage failed: %d %s", response.Code, response.Body.String())
	}
	archiveRequest := httptest.NewRequest(http.MethodPut, "http://container.internal/stage/archive", bytes.NewReader(archive.Bytes()))
	archiveRequest.Header.Set("content-type", "application/gzip")
	archiveResponse := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(archiveResponse, archiveRequest)
	if archiveResponse.Code != http.StatusOK {
		t.Fatalf("archive stage failed: %d %s", archiveResponse.Code, archiveResponse.Body.String())
	}
}

func TestStageMaterializesScriptsSkillsAndClaudeLink(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	t.Setenv("RUNNER_WORKSPACE", workspace)
	supervisor := NewSupervisor(testConfig(), io.Discard)
	stageSupervisor(t, supervisor)
	if info, err := os.Stat(filepath.Join(workspace, "scripts", "check.sh")); err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("script was not installed executable: %v %+v", err, info)
	}
	if _, err := os.Stat(filepath.Join(workspace, ".agents", "skills", "example", "SKILL.md")); err != nil {
		t.Fatalf("skill was not installed: %v", err)
	}
	if target, err := os.Readlink(filepath.Join(workspace, ".claude")); err != nil || target != ".agents" {
		t.Fatalf("unexpected Claude link %q: %v", target, err)
	}
}

func TestMemoryInitializeSnapshotAndExport(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	t.Setenv("RUNNER_WORKSPACE", workspace)
	config := testConfig()
	config.Workflow.MemoryEnabled = true
	supervisor := NewSupervisor(config, io.Discard)
	stageSupervisor(t, supervisor)

	initialized := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(initialized, httptest.NewRequest(http.MethodPost, "http://container.internal/memory/initialize", nil))
	if initialized.Code != http.StatusOK {
		t.Fatalf("memory initialization failed: %d %s", initialized.Code, initialized.Body.String())
	}
	database := filepath.Join(workspace, "memory.sqlite3")
	if output, err := exec.Command("sqlite3", database, "PRAGMA journal_mode=WAL; CREATE TABLE memories(value TEXT); INSERT INTO memories VALUES ('remembered');").CombinedOutput(); err != nil {
		t.Fatalf("write memory database: %v: %s", err, output)
	}
	snapshot, err := createMemorySnapshot(config)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.mu.Lock()
	supervisor.memorySnapshot = snapshot
	supervisor.completion = &WorkflowCompletion{Status: "succeeded", Workflow: "default", WorkflowDigest: strings.Repeat("a", 64)}
	supervisor.mu.Unlock()

	exported := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(exported, httptest.NewRequest(http.MethodGet, "http://container.internal/memory", nil))
	if exported.Code != http.StatusOK || exported.Header().Get("x-content-sha256") != snapshot.SHA256 || int64(exported.Body.Len()) != snapshot.Size {
		t.Fatalf("unexpected memory export: %d %+v", exported.Code, exported.Header())
	}
	copyPath := filepath.Join(t.TempDir(), "copy.sqlite3")
	if err := os.WriteFile(copyPath, exported.Body.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("sqlite3", copyPath, "SELECT value FROM memories;").CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "remembered" {
		t.Fatalf("snapshot did not retain memory: %v: %s", err, output)
	}
}

func TestMemoryImportFailsClosed(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	t.Setenv("RUNNER_WORKSPACE", workspace)
	config := testConfig()
	config.Workflow.MemoryEnabled = true
	supervisor := NewSupervisor(config, io.Discard)
	stageSupervisor(t, supervisor)

	request := httptest.NewRequest(http.MethodPut, "http://container.internal/memory", strings.NewReader("not a sqlite database"))
	response := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected invalid database rejection, got %d: %s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(workspace, "memory.sqlite3")); !os.IsNotExist(err) {
		t.Fatalf("invalid memory database was materialized: %v", err)
	}
}

func TestExtractWorkflowArchiveRejectsTraversalAndLinks(t *testing.T) {
	for _, header := range []*tar.Header{
		{Name: "scripts/../escape", Typeflag: tar.TypeReg, Mode: 0o444},
		{Name: "skills/link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd", Mode: 0o777},
	} {
		var archive bytes.Buffer
		gzipWriter := gzip.NewWriter(&archive)
		tarWriter := tar.NewWriter(gzipWriter)
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if err := tarWriter.Close(); err != nil {
			t.Fatal(err)
		}
		if err := gzipWriter.Close(); err != nil {
			t.Fatal(err)
		}
		if err := extractWorkflowArchive(&StageManifest{}, archive.Bytes(), t.TempDir()); err == nil {
			t.Fatalf("expected archive entry %q to be rejected", header.Name)
		}
	}
}

func TestRunReturnsAcceptedAfterHarnessStarts(t *testing.T) {
	t.Setenv("RUNNER_WORKSPACE", filepath.Join(t.TempDir(), "workspace"))
	t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
	t.Setenv("RUNNER_EGRESS_TOKEN", "proxy-token")
	supervisor := NewSupervisor(testConfig(), io.Discard)
	stageSupervisor(t, supervisor)
	t.Setenv("GROK_PATH", fakeACPAgent(t, "success"))
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer callback.Close()
	body, _ := json.Marshal(RunRequest{JobID: "job-1", CallbackToken: strings.Repeat("c", 32), CallbackURL: callback.URL, RunHarness: "workflow", RunCreatedAt: "2026-08-31T00:00:00Z", RunStartedAt: "2026-08-31T00:00:01Z", TraceID: testTraceID, RunSpanID: testRootSpanID, DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)})
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}
	<-supervisor.done
	if supervisor.completion == nil || supervisor.completion.Status != "succeeded" || len(supervisor.completion.Steps) != 1 {
		t.Fatalf("unexpected completion: %+v", supervisor.completion)
	}
}

func TestRunReturnsAcceptedAfterResumedLaterStepStarts(t *testing.T) {
	t.Setenv("RUNNER_WORKSPACE", filepath.Join(t.TempDir(), "workspace"))
	t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
	t.Setenv("RUNNER_EGRESS_TOKEN", "proxy-token")
	config := testConfig()
	config.Workflow.Steps = []WorkflowStep{
		{ID: "prepare", Command: []string{"should-not-run"}, TimeoutMS: 2000},
		{ID: "ask-user", Prompt: "foo.md", Harness: "grok", Provider: "xai", Model: "grok-4.6", ModelID: "grok-4.6", TimeoutMS: 2000},
	}
	var logs bytes.Buffer
	supervisor := NewSupervisor(config, &logs)
	stageSupervisor(t, supervisor)
	t.Setenv("GROK_PATH", fakeACPAgent(t, "success"))
	sessionID := "session-1"
	body, _ := json.Marshal(RunRequest{
		JobID: "job-1", CallbackToken: strings.Repeat("c", 32), DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
		Resume: &ResumeRequest{
			StepIndex: 1, RemainingStepMS: 1000, SessionID: sessionID, Response: &InteractionResponse{Action: InteractionAccept}, EventSequence: 69,
			Steps: []StepCompletion{
				{ID: "prepare", Command: []string{"should-not-run"}, Status: "succeeded", StartedAt: "2026-09-15T14:30:50Z", FinishedAt: "2026-09-15T14:30:51Z", ExitCode: 0},
				{ID: "ask-user", Prompt: "foo.md", Harness: "grok", Provider: "xai", Model: "grok-4.6", Status: "running", StartedAt: "2026-09-15T14:30:51Z", SessionID: &sessionID},
			},
		},
	})
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	startedAt := time.Now()
	supervisor.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected resumed run to return 202, got %d: %s", response.Code, response.Body.String())
	}
	if time.Since(startedAt) > 5*time.Second {
		t.Fatal("resumed later step did not acknowledge startup promptly")
	}
	<-supervisor.done
	if supervisor.completion == nil || supervisor.completion.Status != "succeeded" || len(supervisor.completion.Steps) != 2 {
		t.Fatalf("unexpected resumed completion: %+v", supervisor.completion)
	}
	if supervisor.completion.Steps[0].Status != "succeeded" || supervisor.completion.Steps[0].Command[0] != "should-not-run" {
		t.Fatalf("prior step was not preserved: %+v", supervisor.completion.Steps[0])
	}
	if !strings.Contains(logs.String(), `"event_type":"session/resume"`) || !strings.Contains(logs.String(), `\"action\":\"accept\"`) {
		t.Fatalf("resume did not restore the ACP session and response: %s", logs.String())
	}
}

func TestRunReturnsAcceptedWhenFirstStepTimesOutDuringACPSetup(t *testing.T) {
	t.Setenv("RUNNER_WORKSPACE", filepath.Join(t.TempDir(), "workspace"))
	t.Setenv("GROK_HOME", filepath.Join(t.TempDir(), "grok-home"))
	t.Setenv("RUNNER_EGRESS_TOKEN", "proxy-token")
	config := testConfig()
	config.Workflow.Steps[0].TimeoutMS = 100
	supervisor := NewSupervisor(config, io.Discard)
	stageSupervisor(t, supervisor)
	t.Setenv("GROK_PATH", fakeACPAgent(t, "timeout"))
	body, _ := json.Marshal(RunRequest{JobID: "job-1", CallbackToken: strings.Repeat("c", 32), DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)})
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}
	<-supervisor.done
	if supervisor.completion == nil || supervisor.completion.Status != "timed_out" || supervisor.completion.Error == nil || supervisor.completion.Error.Code != "timeout" {
		t.Fatalf("unexpected completion: %+v", supervisor.completion)
	}
}

func TestRunRequiresStableWorkflowHarnessForCallbacks(t *testing.T) {
	for _, harness := range []string{"", "grok"} {
		body, _ := json.Marshal(RunRequest{
			JobID: "job-1", CallbackToken: strings.Repeat("c", 32),
			CallbackURL: "https://runner.test/internal/v1/jobs/job-1/events", RunHarness: harness,
			RunCreatedAt: "2026-08-31T00:00:00Z", RunStartedAt: "2026-08-31T00:00:01Z",
			TraceID: testTraceID, RunSpanID: testRootSpanID,
			DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
		})
		request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", bytes.NewReader(body))
		request.Header.Set("content-type", "application/json")
		response := httptest.NewRecorder()
		NewSupervisor(testConfig(), io.Discard).Handler().ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "run_harness must be workflow") {
			t.Fatalf("expected invalid run harness %q to be rejected, got %d: %s", harness, response.Code, response.Body.String())
		}
	}
}

func TestRunRequiresBudgetCallbackForBudgetedWorkflow(t *testing.T) {
	config := testConfig()
	config.Workflow.TokenBudget = &TokenBudget{Limit: 100, Period: "day"}
	body, _ := json.Marshal(RunRequest{
		JobID: "job-1", CallbackToken: strings.Repeat("c", 32),
		DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
	})
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	NewSupervisor(config, io.Discard).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "budget_url is required") {
		t.Fatalf("expected missing budget callback rejection, got %d: %s", response.Code, response.Body.String())
	}
}

func TestRunRejectsInvalidFlowTraceContext(t *testing.T) {
	body, _ := json.Marshal(RunRequest{
		JobID: "job-1", CallbackToken: strings.Repeat("c", 32),
		CallbackURL: "https://runner.test/internal/v1/jobs/job-1/events", RunHarness: "workflow",
		RunCreatedAt: "2026-08-31T00:00:00Z", RunStartedAt: "2026-08-31T00:00:01Z",
		TraceID: strings.Repeat("0", 32), RunSpanID: "not-a-span", DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
	})
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	NewSupervisor(testConfig(), io.Discard).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "valid trace_id and run_span_id") {
		t.Fatalf("expected invalid trace context rejection, got %d: %s", response.Code, response.Body.String())
	}
}

func TestRunReportsInvalidJSONStructure(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", strings.NewReader(`{"unexpected":true}`))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()

	NewSupervisor(testConfig(), io.Discard).Handler().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `unknown field \"unexpected\"`) {
		t.Fatalf("expected structural JSON error, got status %d body %s", response.Code, response.Body.String())
	}
}

func TestResultReturnsProgressAndCompletion(t *testing.T) {
	supervisor := NewSupervisor(testConfig(), io.Discard)
	pending := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(pending, httptest.NewRequest(http.MethodGet, "http://container.internal/result", nil))
	if pending.Code != http.StatusAccepted {
		t.Fatalf("expected pending result, got %d", pending.Code)
	}
	supervisor.mu.Lock()
	supervisor.completion = &WorkflowCompletion{Status: "succeeded", Workflow: "default", WorkflowDigest: strings.Repeat("a", 64), Steps: []StepCompletion{}}
	supervisor.mu.Unlock()
	ready := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "http://container.internal/result", nil))
	if ready.Code != http.StatusOK || !strings.Contains(ready.Body.String(), `"workflow":"default"`) {
		t.Fatalf("unexpected result: %d %s", ready.Code, ready.Body.String())
	}
}

func TestRunRequiresStagedWorkspace(t *testing.T) {
	supervisor := NewSupervisor(testConfig(), io.Discard)
	body, _ := json.Marshal(RunRequest{JobID: "job-1", CallbackToken: strings.Repeat("c", 32), DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)})
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("expected staging conflict, got %d", response.Code)
	}
}

func TestRunRequiresStagedMemoryWhenEnabled(t *testing.T) {
	t.Setenv("RUNNER_WORKSPACE", filepath.Join(t.TempDir(), "workspace"))
	config := testConfig()
	config.Workflow.MemoryEnabled = true
	supervisor := NewSupervisor(config, io.Discard)
	stageSupervisor(t, supervisor)
	body, _ := json.Marshal(RunRequest{JobID: "job-1", CallbackToken: strings.Repeat("c", 32), DeadlineAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)})
	request := httptest.NewRequest(http.MethodPost, "http://container.internal/run", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	response := httptest.NewRecorder()
	supervisor.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "memory is not staged") {
		t.Fatalf("expected memory staging conflict, got %d: %s", response.Code, response.Body.String())
	}
}

func TestStageRequiresPromptsOnlyForLLMSteps(t *testing.T) {
	t.Setenv("RUNNER_WORKSPACE", filepath.Join(t.TempDir(), "workspace"))
	config := testConfig()
	config.Workflow.Steps = append([]WorkflowStep{{ID: "check", Command: []string{"true"}, TimeoutMS: 1000}}, config.Workflow.Steps...)
	supervisor := NewSupervisor(config, io.Discard)
	stageSupervisor(t, supervisor)
}

func TestArtifactScanIsBoundedAndSorted(t *testing.T) {
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "output", "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "output", "z.txt"), []byte("z"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "output", "nested", "a.txt"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts, err := scanArtifacts(testConfig(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 2 || artifacts[0].Path != "nested/a.txt" || artifacts[1].Index != 1 {
		t.Fatalf("unexpected artifacts: %+v", artifacts)
	}
	config := testConfig()
	config.MaxArtifactTotalBytes = 1
	if _, err := scanArtifacts(config, workspace); err == nil {
		t.Fatal("expected aggregate artifact limit failure")
	}
}

func TestCommandOnlyManifestAcceptsNoPromptFiles(t *testing.T) {
	config := testConfig()
	config.Workflow.Steps = []WorkflowStep{{ID: "check", Command: []string{"true"}, TimeoutMS: 1000}}
	manifest := StageManifest{Version: 1, Digest: config.Workflow.BundleDigest, SortKey: "20260903T214512.347Z", Workflow: json.RawMessage(`{"version":1}`), Archive: BundleArchive{Key: "bundles/20260903T214512.347Z-" + config.Workflow.BundleDigest[:12] + "/bundle.tgz", Size: 100, SHA256: strings.Repeat("a", 64)}, Files: []BundleFile{}, TotalBytes: 0}
	if err := validateStageManifest(config, &manifest); err != nil {
		t.Fatalf("command-only manifest rejected: %v", err)
	}
	config.Workflow.Steps = append(config.Workflow.Steps, WorkflowStep{ID: "agent", Prompt: "missing.md"})
	if err := validateStageManifest(config, &manifest); err == nil {
		t.Fatal("agent workflow accepted without its prompt")
	}
}
