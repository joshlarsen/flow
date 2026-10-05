package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type BundleArchive struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type BundleFile struct {
	Kind       string `json:"kind"`
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	Executable bool   `json:"executable"`
}

type StageManifest struct {
	Version    int             `json:"version"`
	Digest     string          `json:"digest"`
	SortKey    string          `json:"sort_key"`
	Workflow   json.RawMessage `json:"workflow"`
	Archive    BundleArchive   `json:"archive"`
	Files      []BundleFile    `json:"files"`
	TotalBytes int64           `json:"total_bytes"`
}

var bundleSortKeyPattern = regexp.MustCompile(`^\d{8}T\d{6}\.\d{3}Z$`)

type Artifact struct {
	Index       int    `json:"index"`
	Path        string `json:"path"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
	absolute    string
}

type MemorySnapshot struct {
	Size     int64
	SHA256   string
	absolute string
}

type boundedOutput struct {
	mu    sync.Mutex
	value bytes.Buffer
	limit int
}

func (output *boundedOutput) Write(value []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	remaining := output.limit - output.value.Len()
	if remaining > 0 {
		_, _ = output.value.Write(value[:min(len(value), remaining)])
	}
	return len(value), nil
}

func (output *boundedOutput) String() string {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.value.String()
}

type Supervisor struct {
	config         Config
	out            io.Writer
	mu             sync.Mutex
	manifest       *StageManifest
	stageRoot      string
	staged         bool
	started        bool
	cancel         context.CancelFunc
	progress       *WorkflowCompletion
	completion     *WorkflowCompletion
	artifacts      []Artifact
	memoryReady    bool
	memorySnapshot *MemorySnapshot
	done           chan struct{}
}

func NewSupervisor(config Config, out io.Writer) *Supervisor {
	return &Supervisor{config: config, out: out, done: make(chan struct{})}
}

func (supervisor *Supervisor) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("content-type", "text/plain; charset=utf-8")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /stage", supervisor.handleStage)
	mux.HandleFunc("PUT /stage/archive", supervisor.handleStageArchive)
	mux.HandleFunc("POST /memory/initialize", supervisor.handleMemoryInitialize)
	mux.HandleFunc("PUT /memory", supervisor.handleMemoryImport)
	mux.HandleFunc("GET /memory", supervisor.handleMemoryExport)
	mux.HandleFunc("GET /checkpoint", supervisor.handleCheckpointExport)
	mux.HandleFunc("PUT /checkpoint", supervisor.handleCheckpointImport)
	mux.HandleFunc("POST /run", supervisor.handleRun)
	mux.HandleFunc("POST /cancel", supervisor.handleCancel)
	mux.HandleFunc("GET /result", supervisor.handleResult)
	mux.HandleFunc("GET /artifacts", supervisor.handleArtifacts)
	mux.HandleFunc("GET /artifacts/{index}", supervisor.handleArtifact)
	return mux
}

func memoryDatabasePath() string {
	return filepath.Join(envOr("RUNNER_WORKSPACE", "/workspace"), "memory.sqlite3")
}

/** Runs one fixed SQLite maintenance command with bounded diagnostics and execution time. */
func runSQLite(config Config, database string, commands ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.MemoryPersistenceMS)*time.Millisecond)
	defer cancel()
	arguments := append([]string{"-batch", "-bail", database}, commands...)
	command := exec.CommandContext(ctx, "sqlite3", arguments...)
	output := &boundedOutput{limit: 64 * 1024}
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	if ctx.Err() != nil {
		return output.String(), fmt.Errorf("sqlite command timed out: %w", ctx.Err())
	}
	if err != nil {
		return output.String(), fmt.Errorf("sqlite command failed: %w", err)
	}
	return output.String(), nil
}

/** Rejects malformed databases without modifying the source file. */
func validateSQLiteDatabase(config Config, database string) error {
	info, err := os.Lstat(database)
	if err != nil {
		return fmt.Errorf("inspect SQLite database: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > config.MaxMemoryBytes {
		return fmt.Errorf("SQLite database is not a bounded regular file")
	}
	output, err := runSQLite(config, database, "PRAGMA quick_check;")
	if err != nil {
		return err
	}
	if strings.TrimSpace(output) != "ok" {
		return fmt.Errorf("SQLite quick_check failed")
	}
	return nil
}

func (supervisor *Supervisor) canPrepareMemory() bool {
	return supervisor.config.Workflow != nil && supervisor.config.Workflow.MemoryEnabled && supervisor.staged && !supervisor.started && !supervisor.memoryReady
}

/** Creates the first canonical empty database for a memory-enabled workflow. */
func (supervisor *Supervisor) handleMemoryInitialize(response http.ResponseWriter, _ *http.Request) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if !supervisor.canPrepareMemory() {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "memory cannot be initialized in the current state"})
		return
	}
	workspace := envOr("RUNNER_WORKSPACE", "/workspace")
	temporary, err := os.CreateTemp(workspace, ".memory-initialize-")
	if err != nil {
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "create memory staging file"})
		return
	}
	temporaryPath := temporary.Name()
	_ = temporary.Close()
	defer os.Remove(temporaryPath)
	if _, err = runSQLite(supervisor.config, temporaryPath, "VACUUM;"); err != nil {
		writeJSON(response, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()})
		return
	}
	if err = validateSQLiteDatabase(supervisor.config, temporaryPath); err != nil {
		writeJSON(response, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()})
		return
	}
	if err = os.Chmod(temporaryPath, 0o600); err != nil || os.Rename(temporaryPath, memoryDatabasePath()) != nil {
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "install initialized memory database"})
		return
	}
	supervisor.memoryReady = true
	writeJSON(response, http.StatusOK, map[string]any{"initialized": true})
}

/** Streams and validates an existing canonical database before exposing it to agents. */
func (supervisor *Supervisor) handleMemoryImport(response http.ResponseWriter, request *http.Request) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if !supervisor.canPrepareMemory() {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "memory cannot be imported in the current state"})
		return
	}
	workspace := envOr("RUNNER_WORKSPACE", "/workspace")
	temporary, err := os.CreateTemp(workspace, ".memory-import-")
	if err != nil {
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "create memory staging file"})
		return
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	request.Body = http.MaxBytesReader(response, request.Body, supervisor.config.MaxMemoryBytes+1)
	written, copyError := io.Copy(temporary, request.Body)
	closeError := temporary.Close()
	if copyError != nil || closeError != nil || written < 1 || written > supervisor.config.MaxMemoryBytes {
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": "memory database has an invalid size"})
		return
	}
	if err = validateSQLiteDatabase(supervisor.config, temporaryPath); err != nil {
		writeJSON(response, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()})
		return
	}
	if err = os.Chmod(temporaryPath, 0o600); err != nil || os.Rename(temporaryPath, memoryDatabasePath()) != nil {
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "install imported memory database"})
		return
	}
	supervisor.memoryReady = true
	writeJSON(response, http.StatusOK, map[string]any{"imported": true, "size": written})
}

/** Serves the immutable post-workflow snapshot used for the canonical R2 write. */
func (supervisor *Supervisor) handleMemoryExport(response http.ResponseWriter, request *http.Request) {
	supervisor.mu.Lock()
	snapshot := supervisor.memorySnapshot
	completion := supervisor.completion
	supervisor.mu.Unlock()
	if completion == nil {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "memory snapshot is not ready"})
		return
	}
	if snapshot == nil {
		writeJSON(response, http.StatusUnprocessableEntity, map[string]any{"error": "memory snapshot failed"})
		return
	}
	file, err := os.Open(snapshot.absolute)
	if err != nil {
		http.NotFound(response, request)
		return
	}
	defer file.Close()
	response.Header().Set("content-type", "application/vnd.sqlite3")
	response.Header().Set("content-length", strconv.FormatInt(snapshot.Size, 10))
	response.Header().Set("x-content-sha256", snapshot.SHA256)
	_, _ = io.Copy(response, io.LimitReader(file, snapshot.Size+1))
}

func (supervisor *Supervisor) handleStage(response http.ResponseWriter, request *http.Request) {
	if !isJSON(request) {
		writeJSON(response, http.StatusUnsupportedMediaType, map[string]any{"error": "content-type must be application/json"})
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 2*1024*1024)
	var input StageManifest
	if err := decodeOne(request.Body, &input); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if supervisor.started || supervisor.staged || supervisor.manifest != nil {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "workspace was already staged"})
		return
	}
	if err := validateStageManifest(supervisor.config, &input); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	workspace := envOr("RUNNER_WORKSPACE", "/workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "create workspace"})
		return
	}
	stageRoot, err := os.MkdirTemp(workspace, ".workflow-bundle-")
	if err != nil {
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "create bundle staging directory"})
		return
	}
	for _, directory := range []string{"prompts", "scripts", "skills"} {
		if err := os.MkdirAll(filepath.Join(stageRoot, directory), 0o755); err != nil {
			_ = os.RemoveAll(stageRoot)
			writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "create bundle namespace"})
			return
		}
	}
	supervisor.manifest = &input
	supervisor.stageRoot = stageRoot
	writeJSON(response, http.StatusOK, map[string]any{"accepted": true, "files": len(input.Files)})
}

func (supervisor *Supervisor) handleStageArchive(response http.ResponseWriter, request *http.Request) {
	if !strings.HasPrefix(strings.ToLower(request.Header.Get("content-type")), "application/gzip") {
		writeJSON(response, http.StatusUnsupportedMediaType, map[string]any{"error": "content-type must be application/gzip"})
		return
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if supervisor.started || supervisor.staged || supervisor.manifest == nil || supervisor.stageRoot == "" {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "workflow manifest is not awaiting an archive"})
		return
	}
	manifest := supervisor.manifest
	request.Body = http.MaxBytesReader(response, request.Body, manifest.Archive.Size+1)
	archive, err := io.ReadAll(request.Body)
	if err != nil || int64(len(archive)) != manifest.Archive.Size {
		supervisor.clearPendingStage()
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": "workflow archive has an invalid size"})
		return
	}
	digest := sha256.Sum256(archive)
	if hex.EncodeToString(digest[:]) != manifest.Archive.SHA256 {
		supervisor.clearPendingStage()
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": "workflow archive failed integrity validation"})
		return
	}
	if err := extractWorkflowArchive(manifest, archive, supervisor.stageRoot); err != nil {
		supervisor.clearPendingStage()
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := installWorkflowBundle(envOr("RUNNER_WORKSPACE", "/workspace"), supervisor.stageRoot); err != nil {
		supervisor.clearPendingStage()
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	supervisor.manifest = nil
	supervisor.stageRoot = ""
	supervisor.staged = true
	writeJSON(response, http.StatusOK, map[string]any{"staged": true, "files": len(manifest.Files)})
}

func (supervisor *Supervisor) clearPendingStage() {
	if supervisor.stageRoot != "" {
		_ = os.RemoveAll(supervisor.stageRoot)
	}
	supervisor.stageRoot = ""
	supervisor.manifest = nil
}

func safeBundlePath(value string) bool {
	if value == "" || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") || len(value) > 1024 {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." || len(part) > 255 || strings.ContainsRune(part, 0) {
			return false
		}
	}
	return true
}

func validateStageManifest(config Config, manifest *StageManifest) error {
	if config.Workflow == nil || manifest.Version != 1 || manifest.Digest != config.Workflow.BundleDigest || !digestPattern.MatchString(manifest.Digest) || !bundleSortKeyPattern.MatchString(manifest.SortKey) || len(manifest.Workflow) == 0 {
		return fmt.Errorf("manifest does not match configured workflow")
	}
	archiveLimit := int64(config.MaxPromptBundleBytes + config.MaxAssetBundleBytes + (config.MaxPromptFiles+config.MaxAssetFiles+16)*1024 + 1024*1024)
	if manifest.Archive.Size < 1 || manifest.Archive.Size > archiveLimit || !digestPattern.MatchString(manifest.Archive.SHA256) ||
		!strings.HasSuffix(manifest.Archive.Key, "/"+manifest.SortKey+"-"+manifest.Digest[:12]+"/bundle.tgz") {
		return fmt.Errorf("manifest contains invalid archive metadata")
	}
	seen := map[string]bool{}
	prompts := map[string]bool{}
	var promptBytes, assetBytes int64
	promptFiles, assetFiles := 0, 0
	for _, file := range manifest.Files {
		identity := file.Kind + ":" + file.Path
		if !safeBundlePath(file.Path) || seen[identity] || file.Size < 0 || !digestPattern.MatchString(file.SHA256) {
			return fmt.Errorf("manifest contains invalid file %q", identity)
		}
		seen[identity] = true
		switch file.Kind {
		case "prompt":
			if !safePromptPath(file.Path) || file.Size < 1 || file.Size > int64(config.MaxPromptBytes) {
				return fmt.Errorf("manifest contains invalid prompt %q", file.Path)
			}
			promptFiles++
			promptBytes += file.Size
			prompts[file.Path] = true
		case "script", "skill":
			if file.Size > int64(config.MaxAssetBytes) {
				return fmt.Errorf("manifest contains oversized asset %q", file.Path)
			}
			assetFiles++
			assetBytes += file.Size
		default:
			return fmt.Errorf("manifest contains unknown file kind %q", file.Kind)
		}
	}
	if promptFiles > config.MaxPromptFiles || promptBytes > int64(config.MaxPromptBundleBytes) || assetFiles > config.MaxAssetFiles || assetBytes > int64(config.MaxAssetBundleBytes) || promptBytes+assetBytes != manifest.TotalBytes {
		return fmt.Errorf("manifest file totals are invalid")
	}
	for _, step := range config.Workflow.Steps {
		if step.Command == nil && !prompts[step.Prompt] {
			return fmt.Errorf("missing workflow prompt %q", step.Prompt)
		}
	}
	return nil
}

func extractWorkflowArchive(manifest *StageManifest, archive []byte, stageRoot string) error {
	expected := map[string]BundleFile{}
	for _, file := range manifest.Files {
		expected[file.Kind+":"+file.Path] = file
	}
	gzipReader, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return fmt.Errorf("open workflow archive: %w", err)
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	seen := map[string]bool{}
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read workflow archive: %w", err)
		}
		name := strings.TrimSuffix(header.Name, "/")
		if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, "\\") || filepath.ToSlash(filepath.Clean(name)) != name {
			return fmt.Errorf("workflow archive contains unsafe path %q", header.Name)
		}
		parts := strings.SplitN(name, "/", 2)
		if len(parts) == 1 {
			if header.Typeflag != tar.TypeDir || (parts[0] != "prompts" && parts[0] != "scripts" && parts[0] != "skills") {
				return fmt.Errorf("workflow archive contains invalid root %q", header.Name)
			}
			continue
		}
		if parts[0] != "prompts" && parts[0] != "scripts" && parts[0] != "skills" {
			return fmt.Errorf("workflow archive contains invalid namespace %q", parts[0])
		}
		if !safeBundlePath(parts[1]) {
			return fmt.Errorf("workflow archive contains unsafe path %q", header.Name)
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return fmt.Errorf("workflow archive contains unsupported entry %q", header.Name)
		}
		kind := strings.TrimSuffix(parts[0], "s")
		identity := kind + ":" + parts[1]
		file, ok := expected[identity]
		if !ok || seen[identity] || header.Size != file.Size || ((header.Mode&0o111) != 0) != file.Executable {
			return fmt.Errorf("workflow archive entry %q does not match manifest", header.Name)
		}
		data := make([]byte, header.Size)
		if _, err := io.ReadFull(tarReader, data); err != nil {
			return fmt.Errorf("read workflow archive entry %q: %w", header.Name, err)
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != file.SHA256 || (kind == "prompt" && !utf8.Valid(data)) {
			return fmt.Errorf("workflow archive entry %q failed integrity validation", header.Name)
		}
		destination := filepath.Join(stageRoot, parts[0], filepath.FromSlash(parts[1]))
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return fmt.Errorf("create workflow archive parent: %w", err)
		}
		mode := os.FileMode(0o444)
		if file.Executable {
			mode = 0o555
		}
		if err := os.WriteFile(destination, data, mode); err != nil {
			return fmt.Errorf("write workflow archive entry: %w", err)
		}
		seen[identity] = true
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("workflow archive is missing manifest files")
	}
	return nil
}

func installWorkflowBundle(workspace, stageRoot string) error {
	for _, target := range []string{"prompts", "scripts"} {
		if err := os.RemoveAll(filepath.Join(workspace, target)); err != nil {
			return fmt.Errorf("clear workspace %s: %w", target, err)
		}
		if err := os.Rename(filepath.Join(stageRoot, target), filepath.Join(workspace, target)); err != nil {
			return fmt.Errorf("install workspace %s: %w", target, err)
		}
	}
	agentsRoot := filepath.Join(workspace, ".agents")
	if info, err := os.Lstat(agentsRoot); err == nil && !info.IsDir() {
		if err := os.RemoveAll(agentsRoot); err != nil {
			return fmt.Errorf("replace .agents: %w", err)
		}
	}
	if err := os.MkdirAll(agentsRoot, 0o755); err != nil {
		return fmt.Errorf("create .agents: %w", err)
	}
	if err := os.RemoveAll(filepath.Join(agentsRoot, "skills")); err != nil {
		return fmt.Errorf("clear skills: %w", err)
	}
	if err := os.Rename(filepath.Join(stageRoot, "skills"), filepath.Join(agentsRoot, "skills")); err != nil {
		return fmt.Errorf("install skills: %w", err)
	}
	claude := filepath.Join(workspace, ".claude")
	if err := os.RemoveAll(claude); err != nil {
		return fmt.Errorf("clear .claude: %w", err)
	}
	if err := os.Symlink(".agents", claude); err != nil {
		return fmt.Errorf("link .claude: %w", err)
	}
	output := filepath.Join(workspace, "output")
	if err := os.RemoveAll(output); err != nil {
		return fmt.Errorf("clear output: %w", err)
	}
	if err := os.MkdirAll(output, 0o755); err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	return os.RemoveAll(stageRoot)
}

func (supervisor *Supervisor) handleResult(response http.ResponseWriter, _ *http.Request) {
	supervisor.mu.Lock()
	completion, progress := supervisor.completion, supervisor.progress
	supervisor.mu.Unlock()
	if completion != nil {
		writeJSON(response, http.StatusOK, completion)
		return
	}
	if progress != nil {
		writeJSON(response, http.StatusAccepted, progress)
		return
	}
	writeJSON(response, http.StatusAccepted, map[string]any{"status": "starting"})
}

func (supervisor *Supervisor) handleRun(response http.ResponseWriter, request *http.Request) {
	if !isJSON(request) {
		writeJSON(response, http.StatusUnsupportedMediaType, map[string]any{"error": "content-type must be application/json"})
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 64*1024)
	var input RunRequest
	if err := decodeOne(request.Body, &input); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	callbackURL, callbackError := url.Parse(input.CallbackURL)
	callbackIsLocal := callbackURL != nil && (callbackURL.Hostname() == "localhost" || callbackURL.Hostname() == "127.0.0.1")
	callbackIsValid := input.CallbackURL == "" || (callbackError == nil && callbackURL.Host != "" && (callbackURL.Scheme == "https" || (callbackIsLocal && callbackURL.Scheme == "http")))
	interactionURL, interactionError := url.Parse(input.InteractionURL)
	interactionIsLocal := interactionURL != nil && (interactionURL.Hostname() == "localhost" || interactionURL.Hostname() == "127.0.0.1")
	interactionIsValid := input.InteractionURL == "" || (interactionError == nil && interactionURL.Host != "" && (interactionURL.Scheme == "https" || (interactionIsLocal && interactionURL.Scheme == "http")))
	budgetURL, budgetError := url.Parse(input.BudgetURL)
	budgetIsLocal := budgetURL != nil && (budgetURL.Hostname() == "localhost" || budgetURL.Hostname() == "127.0.0.1")
	budgetIsValid := input.BudgetURL == "" || (budgetError == nil && budgetURL.Host != "" && (budgetURL.Scheme == "https" || (budgetIsLocal && budgetURL.Scheme == "http")))
	deadline, deadlineError := time.Parse(time.RFC3339Nano, input.DeadlineAt)
	invalidField := ""
	switch {
	case input.JobID == "":
		invalidField = "job_id is required"
	case len(input.CallbackToken) < 32:
		invalidField = "callback_token must be at least 32 characters"
	case !callbackIsValid:
		invalidField = "callback_url must use HTTPS, except for localhost HTTP"
	case !interactionIsValid:
		invalidField = "interaction_url must use HTTPS, except for localhost HTTP"
	case !budgetIsValid:
		invalidField = "budget_url must use HTTPS, except for localhost HTTP"
	case supervisor.config.Interactions.Provider == "callback" && input.InteractionURL == "":
		invalidField = "interaction_url is required for callback interactions"
	case supervisor.config.Workflow.TokenBudget != nil && input.BudgetURL == "":
		invalidField = "budget_url is required for budgeted workflows"
	case input.CallbackURL != "" && (input.RunCreatedAt == "" || input.RunStartedAt == ""):
		invalidField = "run timestamps are required with callback_url"
	case input.CallbackURL != "" && (!validTraceID(input.TraceID) || !validSpanID(input.RunSpanID)):
		invalidField = "valid trace_id and run_span_id are required with callback_url"
	case input.CallbackURL != "" && input.RunHarness != flowRunHarness:
		invalidField = "run_harness must be workflow with callback_url"
	case deadlineError != nil || !deadline.After(time.Now()):
		invalidField = "deadline_at must be a future RFC3339 timestamp"
	case input.Resume != nil && (input.Resume.StepIndex < 0 || input.Resume.StepIndex >= len(supervisor.config.Workflow.Steps) || len(input.Resume.Steps) != len(supervisor.config.Workflow.Steps)):
		invalidField = "resume state is invalid"
	case input.Resume != nil && input.Resume.Kind == "budget" && (input.Resume.SessionID != "" || input.Resume.Response != nil):
		invalidField = "budget resume state is invalid"
	case input.Resume != nil && input.Resume.Kind != "budget" && (input.Resume.SessionID == "" || input.Resume.RemainingStepMS < 1 || input.Resume.Response == nil):
		invalidField = "interaction resume state is invalid"
	case input.Resume != nil && input.Resume.Response != nil && input.Resume.Response.Action != InteractionAccept && input.Resume.Response.Action != InteractionDecline && input.Resume.Response.Action != InteractionCancel:
		invalidField = "resume response action is invalid"
	}
	if invalidField != "" {
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": invalidField})
		return
	}

	supervisor.mu.Lock()
	if !supervisor.staged {
		supervisor.mu.Unlock()
		writeJSON(response, http.StatusConflict, map[string]any{"error": "workspace prompts are not staged"})
		return
	}
	if supervisor.config.Workflow.MemoryEnabled && !supervisor.memoryReady {
		supervisor.mu.Unlock()
		writeJSON(response, http.StatusConflict, map[string]any{"error": "workflow memory is not staged"})
		return
	}
	if supervisor.started {
		supervisor.mu.Unlock()
		writeJSON(response, http.StatusConflict, map[string]any{"error": "this container already accepted a job"})
		return
	}
	supervisor.started = true
	ctx, cancel := context.WithCancel(context.Background())
	supervisor.cancel = cancel
	supervisor.mu.Unlock()

	started := make(chan error, 1)
	go supervisor.run(ctx, input, started)
	select {
	case err := <-started:
		if err != nil {
			writeJSON(response, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(response, http.StatusAccepted, map[string]any{"accepted": true, "job_id": input.JobID})
	case <-time.After(30 * time.Second):
		cancel()
		writeJSON(response, http.StatusGatewayTimeout, map[string]any{"error": "timed out starting harness process"})
	}
}

func (supervisor *Supervisor) handleCancel(response http.ResponseWriter, _ *http.Request) {
	supervisor.Stop()
	writeJSON(response, http.StatusAccepted, map[string]any{"cancelling": true})
}

func (supervisor *Supervisor) run(ctx context.Context, input RunRequest, started chan<- error) {
	defer close(supervisor.done)
	completion := executeWorkflow(ctx, supervisor.config, input, supervisor.out, started, func(progress WorkflowCompletion) {
		supervisor.mu.Lock()
		copy := cloneWorkflowCompletion(progress)
		if copy.Status == "succeeded" || copy.Status == "failed" || copy.Status == "partial" || copy.Status == "timed_out" || copy.Status == "cancelled" {
			copy.Status = "running"
		}
		supervisor.progress = &copy
		supervisor.mu.Unlock()
	})
	artifacts, artifactError := scanArtifacts(supervisor.config, envOr("RUNNER_WORKSPACE", "/workspace"))
	scopedToken := os.Getenv("RUNNER_EGRESS_TOKEN")
	for index := range artifacts {
		artifacts[index].Path = redactText(artifacts[index].Path, scopedToken)
	}
	if artifactError != nil {
		artifacts = []Artifact{}
		completion.ArtifactError = &JobError{Code: "artifact_collection_failed", Message: redactText(artifactError.Error(), scopedToken), Retryable: false}
		if completion.Status == "succeeded" {
			completion.Status = "failed"
			completion.Error = completion.ArtifactError
		}
	}
	var memorySnapshot *MemorySnapshot
	if supervisor.config.Workflow.MemoryEnabled {
		var memoryError error
		memorySnapshot, memoryError = createMemorySnapshot(supervisor.config)
		if memoryError != nil {
			completion.MemoryError = &JobError{Code: "memory_snapshot_failed", Message: redactText(memoryError.Error(), scopedToken), Retryable: false}
			if completion.Status == "succeeded" || completion.Status == "partial" {
				completion.Status = "failed"
				completion.Error = completion.MemoryError
			}
		}
	}
	supervisor.mu.Lock()
	supervisor.artifacts = artifacts
	supervisor.memorySnapshot = memorySnapshot
	supervisor.completion = &completion
	supervisor.progress = &completion
	supervisor.mu.Unlock()
}

/** Produces a standalone checked snapshot so WAL state is included in one canonical object. */
func createMemorySnapshot(config Config) (*MemorySnapshot, error) {
	source := memoryDatabasePath()
	info, err := os.Lstat(source)
	if err != nil {
		return nil, fmt.Errorf("inspect memory database: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("memory database is not a regular file")
	}
	if err := validateSQLiteDatabase(config, source); err != nil {
		return nil, fmt.Errorf("validate memory database: %w", err)
	}
	workspace := envOr("RUNNER_WORKSPACE", "/workspace")
	temporary, err := os.CreateTemp(workspace, ".memory-snapshot-")
	if err != nil {
		return nil, fmt.Errorf("create memory snapshot: %w", err)
	}
	snapshotPath := temporary.Name()
	if closeError := temporary.Close(); closeError != nil {
		_ = os.Remove(snapshotPath)
		return nil, fmt.Errorf("close memory snapshot: %w", closeError)
	}
	if removeError := os.Remove(snapshotPath); removeError != nil {
		return nil, fmt.Errorf("prepare memory snapshot: %w", removeError)
	}
	escapedPath := strings.ReplaceAll(snapshotPath, "'", "''")
	if _, err = runSQLite(config, source, ".timeout 5000", ".backup '"+escapedPath+"'"); err != nil {
		_ = os.Remove(snapshotPath)
		return nil, fmt.Errorf("snapshot memory database: %w", err)
	}
	if err = validateSQLiteDatabase(config, snapshotPath); err != nil {
		_ = os.Remove(snapshotPath)
		return nil, fmt.Errorf("validate memory snapshot: %w", err)
	}
	file, err := os.Open(snapshotPath)
	if err != nil {
		_ = os.Remove(snapshotPath)
		return nil, fmt.Errorf("open memory snapshot: %w", err)
	}
	hash := sha256.New()
	_, copyError := io.Copy(hash, file)
	closeError := file.Close()
	if copyError != nil || closeError != nil {
		_ = os.Remove(snapshotPath)
		return nil, fmt.Errorf("hash memory snapshot")
	}
	snapshotInfo, err := os.Stat(snapshotPath)
	if err != nil {
		_ = os.Remove(snapshotPath)
		return nil, fmt.Errorf("inspect memory snapshot: %w", err)
	}
	return &MemorySnapshot{Size: snapshotInfo.Size(), SHA256: hex.EncodeToString(hash.Sum(nil)), absolute: snapshotPath}, nil
}

/** Returns a progress snapshot whose mutable slices and aggregate usage cannot change underneath an HTTP encoder. */
func cloneWorkflowCompletion(value WorkflowCompletion) WorkflowCompletion {
	clone := value
	clone.Steps = append([]StepCompletion(nil), value.Steps...)
	if value.Usage != nil {
		usage := *value.Usage
		usage.TotalTokens = copyIntPointer(value.Usage.TotalTokens)
		usage.TotalInputTokens = copyIntPointer(value.Usage.TotalInputTokens)
		usage.InputTokens = copyIntPointer(value.Usage.InputTokens)
		usage.OutputTokens = copyIntPointer(value.Usage.OutputTokens)
		usage.ThoughtTokens = copyIntPointer(value.Usage.ThoughtTokens)
		usage.CachedReadTokens = copyIntPointer(value.Usage.CachedReadTokens)
		usage.CachedWriteTokens = copyIntPointer(value.Usage.CachedWriteTokens)
		usage.Used = copyIntPointer(value.Usage.Used)
		usage.Size = copyIntPointer(value.Usage.Size)
		clone.Usage = &usage
	}
	return clone
}

func scanArtifacts(config Config, workspace string) ([]Artifact, error) {
	root := filepath.Join(workspace, "output")
	artifacts := []Artifact{}
	var total int64
	err := filepath.WalkDir(root, func(filePath string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filePath == root || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("output contains unsupported file %s", filePath)
		}
		if len(artifacts) >= config.MaxArtifactFiles || info.Size() > config.MaxArtifactFileBytes {
			return fmt.Errorf("output artifact limits exceeded")
		}
		total += info.Size()
		if total > config.MaxArtifactTotalBytes {
			return fmt.Errorf("output artifact total limit exceeded")
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		relative, _ := filepath.Rel(root, filePath)
		contentType := mime.TypeByExtension(filepath.Ext(filePath))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		artifacts = append(artifacts, Artifact{Path: filepath.ToSlash(relative), Size: info.Size(), SHA256: hex.EncodeToString(digest[:]), ContentType: contentType, absolute: filePath})
		return nil
	})
	if os.IsNotExist(err) {
		return artifacts, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Path < artifacts[j].Path })
	for index := range artifacts {
		artifacts[index].Index = index
	}
	return artifacts, nil
}

func (supervisor *Supervisor) handleArtifacts(response http.ResponseWriter, _ *http.Request) {
	supervisor.mu.Lock()
	completion, artifacts := supervisor.completion, append([]Artifact(nil), supervisor.artifacts...)
	supervisor.mu.Unlock()
	if completion == nil {
		writeJSON(response, http.StatusAccepted, map[string]any{"status": "running"})
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"artifacts": artifacts})
}

func (supervisor *Supervisor) handleArtifact(response http.ResponseWriter, request *http.Request) {
	index, err := strconv.Atoi(request.PathValue("index"))
	if err != nil {
		http.NotFound(response, request)
		return
	}
	supervisor.mu.Lock()
	artifacts := supervisor.artifacts
	supervisor.mu.Unlock()
	if index < 0 || index >= len(artifacts) {
		http.NotFound(response, request)
		return
	}
	artifact := artifacts[index]
	file, err := os.Open(artifact.absolute)
	if err != nil {
		http.NotFound(response, request)
		return
	}
	defer file.Close()
	response.Header().Set("content-type", artifact.ContentType)
	response.Header().Set("content-length", strconv.FormatInt(artifact.Size, 10))
	response.Header().Set("x-content-sha256", artifact.SHA256)
	_, _ = io.Copy(response, io.LimitReader(file, artifact.Size+1))
}

func isJSON(request *http.Request) bool {
	return strings.HasPrefix(strings.ToLower(request.Header.Get("content-type")), "application/json")
}

func decodeOne(reader io.Reader, target any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid request JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("request must contain one JSON object")
	}
	return nil
}

func (supervisor *Supervisor) Stop() {
	supervisor.mu.Lock()
	if supervisor.cancel != nil {
		supervisor.cancel()
	}
	supervisor.mu.Unlock()
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("content-type", "application/json; charset=utf-8")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
