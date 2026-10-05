package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type countingWriter struct {
	w     io.Writer
	total int64
	limit int64
}

func (writer *countingWriter) Write(value []byte) (int, error) {
	if writer.total+int64(len(value)) > writer.limit {
		return 0, fmt.Errorf("checkpoint exceeds configured total limit")
	}
	n, err := writer.w.Write(value)
	writer.total += int64(n)
	return n, err
}

func checkpointRoots() map[string]string {
	return map[string]string{
		"workspace":     envOr("RUNNER_WORKSPACE", "/workspace"),
		"home":          "/home/runner",
		"codex-home":    envOr("CODEX_HOME", "/tmp/codex-home"),
		"grok-home":     envOr("GROK_HOME", "/tmp/grok-home"),
		"claude-home":   envOr("CLAUDE_CONFIG_DIR", "/tmp/claude-home"),
		"opencode-home": envOr("OPENCODE_HOME", "/tmp/opencode-home"),
		"pi-home":       envOr("PI_CODING_AGENT_DIR", "/tmp/pi-home"),
	}
}

func workflowMemoryCheckpointPath(root, relative string) bool {
	return root == "workspace" && (relative == "memory.sqlite3" || strings.HasPrefix(relative, "memory.sqlite3-"))
}

/** Identifies workspace content recreated from the pinned bundle on every start. */
func rematerializedCheckpointPath(root, relative string) bool {
	if root != "workspace" {
		return false
	}
	return relative == "prompts" || strings.HasPrefix(relative, "prompts/") ||
		relative == "scripts" || strings.HasPrefix(relative, "scripts/") ||
		relative == ".agents/skills" || strings.HasPrefix(relative, ".agents/skills/") ||
		relative == ".claude" || strings.HasPrefix(relative, ".claude/") ||
		strings.HasPrefix(relative, ".workflow-bundle-") ||
		strings.HasPrefix(relative, ".memory-snapshot-")
}

/** Streams a bounded archive of the workspace and harness-owned state. */
func writeCheckpoint(config Config, target io.Writer) error {
	return writeCheckpointRoots(config, target, checkpointRoots())
}

/** Streams a checkpoint from the supplied roots, allowing isolated policy tests. */
func writeCheckpointRoots(config Config, target io.Writer, roots map[string]string) error {
	gzipWriter := gzip.NewWriter(target)
	tarWriter := tar.NewWriter(gzipWriter)
	counted := &countingWriter{w: tarWriter, limit: config.Interactions.CheckpointMaxTotalBytes}
	files := 0
	for archiveRoot, sourceRoot := range roots {
		err := filepath.Walk(sourceRoot, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			relative, err := filepath.Rel(sourceRoot, path)
			if err != nil || relative == "." {
				return err
			}
			archiveRelative := filepath.ToSlash(relative)
			if workflowMemoryCheckpointPath(archiveRoot, archiveRelative) || rematerializedCheckpointPath(archiveRoot, archiveRelative) {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return nil
			}
			if !info.IsDir() && !info.Mode().IsRegular() {
				return nil
			}
			if info.Mode().IsRegular() {
				files++
				if files > config.Interactions.CheckpointMaxFiles || info.Size() > config.Interactions.CheckpointMaxFileBytes {
					return fmt.Errorf("checkpoint file limits exceeded")
				}
			}
			header, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}
			header.Name = filepath.ToSlash(filepath.Join(archiveRoot, relative))
			if err := tarWriter.WriteHeader(header); err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(counted, file)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			return closeErr
		})
		if err != nil {
			return err
		}
	}
	if err := tarWriter.Close(); err != nil {
		return err
	}
	return gzipWriter.Close()
}

/** Restores a bounded checkpoint while rejecting links and path traversal. */
func readCheckpoint(config Config, source io.Reader) error {
	gzipReader, err := gzip.NewReader(source)
	if err != nil {
		return fmt.Errorf("open checkpoint: %w", err)
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	files := 0
	var total int64
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read checkpoint: %w", err)
		}
		clean := filepath.ToSlash(filepath.Clean(header.Name))
		parts := strings.Split(clean, "/")
		root, ok := checkpointRoots()[parts[0]]
		if !ok || len(parts) < 2 || clean != header.Name || strings.Contains(clean, "../") {
			return fmt.Errorf("checkpoint path is unsafe")
		}
		relative := strings.Join(parts[1:], "/")
		if workflowMemoryCheckpointPath(parts[0], relative) {
			return fmt.Errorf("checkpoint contains workflow memory")
		}
		rematerialized := rematerializedCheckpointPath(parts[0], relative)
		target := filepath.Join(root, filepath.FromSlash(relative))
		if header.Typeflag == tar.TypeDir {
			// Older checkpoints included immutable bundle directories. Ignore them so
			// the staged bundle remains authoritative.
			if rematerialized {
				continue
			}
			if err := os.MkdirAll(target, os.FileMode(header.Mode)&0o755); err != nil {
				return err
			}
			continue
		}
		if header.Typeflag != tar.TypeReg {
			return fmt.Errorf("checkpoint contains an unsupported entry")
		}
		files++
		total += header.Size
		if files > config.Interactions.CheckpointMaxFiles || header.Size < 0 || header.Size > config.Interactions.CheckpointMaxFileBytes || total > config.Interactions.CheckpointMaxTotalBytes {
			return fmt.Errorf("checkpoint limits exceeded")
		}
		// Ignore legacy immutable files only after validating their type and size.
		if rematerialized {
			if _, err := io.CopyN(io.Discard, tarReader, header.Size); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(header.Mode)&0o755)
		if err != nil {
			return err
		}
		_, copyErr := io.CopyN(file, tarReader, header.Size)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
}

func (supervisor *Supervisor) handleCheckpointExport(response http.ResponseWriter, _ *http.Request) {
	supervisor.mu.Lock()
	ready := supervisor.completion != nil && (supervisor.completion.Status == "waiting_for_input" || supervisor.completion.Status == "budget_suspended")
	supervisor.mu.Unlock()
	if !ready {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "workflow is not paused at a checkpoint boundary"})
		return
	}
	temporary, err := os.CreateTemp("/tmp/agent-runner", ".checkpoint-*.tgz")
	if err != nil {
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "create checkpoint"})
		return
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := writeCheckpoint(supervisor.config, temporary); err != nil {
		temporary.Close()
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if _, err := temporary.Seek(0, io.SeekStart); err != nil {
		temporary.Close()
		writeJSON(response, http.StatusInternalServerError, map[string]any{"error": "read checkpoint"})
		return
	}
	response.Header().Set("content-type", "application/gzip")
	_, _ = io.Copy(response, temporary)
	_ = temporary.Close()
}

func (supervisor *Supervisor) handleCheckpointImport(response http.ResponseWriter, request *http.Request) {
	supervisor.mu.Lock()
	allowed := !supervisor.started
	supervisor.mu.Unlock()
	if !allowed {
		writeJSON(response, http.StatusConflict, map[string]any{"error": "workflow already started"})
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, supervisor.config.Interactions.CheckpointMaxTotalBytes)
	if err := readCheckpoint(supervisor.config, request.Body); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"restored": true})
}
