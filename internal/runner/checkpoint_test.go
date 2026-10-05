package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

type checkpointEntry struct {
	name    string
	content string
	mode    int64
}

func checkpointEntriesFixture(t *testing.T, entries []checkpointEntry) []byte {
	t.Helper()
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, entry := range entries {
		header := &tar.Header{Name: entry.name, Mode: entry.mode, Size: int64(len(entry.content)), Typeflag: tar.TypeReg}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write([]byte(entry.content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func checkpointFixture(t *testing.T, header *tar.Header, content string) []byte {
	t.Helper()
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	if err := tarWriter.WriteHeader(header); err != nil {
		t.Fatal(err)
	}
	if content != "" {
		if _, err := tarWriter.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestCheckpointRestoreRejectsTraversalAndLinks(t *testing.T) {
	config := testConfig()
	config.Interactions = InteractionsConfig{Provider: "callback", CheckpointMaxFiles: 10, CheckpointMaxFileBytes: 1024, CheckpointMaxTotalBytes: 4096}
	traversal := checkpointFixture(t, &tar.Header{Name: "workspace/../home/escape", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg}, "x")
	if err := readCheckpoint(config, bytes.NewReader(traversal)); err == nil {
		t.Fatal("expected traversal to be rejected")
	}
	link := checkpointFixture(t, &tar.Header{Name: "workspace/link", Linkname: "/etc/passwd", Typeflag: tar.TypeSymlink}, "")
	if err := readCheckpoint(config, bytes.NewReader(link)); err == nil {
		t.Fatal("expected symbolic link to be rejected")
	}
}

func TestCheckpointRestoreEnforcesTotalSize(t *testing.T) {
	config := testConfig()
	config.Interactions = InteractionsConfig{Provider: "callback", CheckpointMaxFiles: 10, CheckpointMaxFileBytes: 1024, CheckpointMaxTotalBytes: 2}
	archive := checkpointFixture(t, &tar.Header{Name: "workspace/large", Mode: 0o600, Size: 3, Typeflag: tar.TypeReg}, "abc")
	if err := readCheckpoint(config, bytes.NewReader(archive)); err == nil {
		t.Fatal("expected oversized checkpoint to be rejected")
	}
}

func TestCheckpointRestoreRejectsWorkflowMemory(t *testing.T) {
	config := testConfig()
	config.Interactions = InteractionsConfig{Provider: "callback", CheckpointMaxFiles: 10, CheckpointMaxFileBytes: 1024, CheckpointMaxTotalBytes: 4096}
	archive := checkpointFixture(t, &tar.Header{Name: "workspace/memory.sqlite3-wal", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg}, "x")
	if err := readCheckpoint(config, bytes.NewReader(archive)); err == nil {
		t.Fatal("expected workflow memory to be rejected")
	}
}

func TestCheckpointOmitsRematerializedWorkspaceFiles(t *testing.T) {
	workspace := t.TempDir()
	for name, content := range map[string]string{
		"prompts/prompt.md":             "prompt",
		"scripts/run.sh":                "script",
		".agents/skills/demo/SKILL.md":  "skill",
		".memory-snapshot-temporary.db": "snapshot",
		"memory.sqlite3":                "memory",
		"output/result.txt":             "result",
		"notes.txt":                     "notes",
	} {
		path := filepath.Join(workspace, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	config := testConfig()
	config.Interactions = InteractionsConfig{Provider: "callback", CheckpointMaxFiles: 10, CheckpointMaxFileBytes: 1024, CheckpointMaxTotalBytes: 4096}
	var archive bytes.Buffer
	if err := writeCheckpointRoots(config, &archive, map[string]string{"workspace": workspace}); err != nil {
		t.Fatal(err)
	}
	gzipReader, err := gzip.NewReader(bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	tarReader := tar.NewReader(gzipReader)
	var names []string
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg {
			names = append(names, header.Name)
		}
	}
	slices.Sort(names)
	want := []string{"workspace/notes.txt", "workspace/output/result.txt"}
	if !slices.Equal(names, want) {
		t.Fatalf("checkpoint files = %v, want %v", names, want)
	}
}

func TestCheckpointRestoreIgnoresLegacyRematerializedFiles(t *testing.T) {
	workspace := t.TempDir()
	t.Setenv("RUNNER_WORKSPACE", workspace)
	promptPath := filepath.Join(workspace, "prompts", "ask-user.md")
	if err := os.MkdirAll(filepath.Dir(promptPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(promptPath, []byte("pinned"), 0o444); err != nil {
		t.Fatal(err)
	}
	archive := checkpointEntriesFixture(t, []checkpointEntry{
		{name: "workspace/prompts/ask-user.md", content: "legacy", mode: 0o644},
		{name: "workspace/.agents/skills/demo/SKILL.md", content: "legacy skill", mode: 0o644},
		{name: "workspace/.memory-snapshot-old.db", content: "legacy snapshot", mode: 0o600},
		{name: "workspace/output/result.txt", content: "restored", mode: 0o644},
	})
	config := testConfig()
	config.Interactions = InteractionsConfig{Provider: "callback", CheckpointMaxFiles: 10, CheckpointMaxFileBytes: 1024, CheckpointMaxTotalBytes: 4096}
	if err := readCheckpoint(config, bytes.NewReader(archive)); err != nil {
		t.Fatal(err)
	}
	prompt, err := os.ReadFile(promptPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(prompt) != "pinned" {
		t.Fatalf("prompt = %q, want pinned", prompt)
	}
	restored, err := os.ReadFile(filepath.Join(workspace, "output", "result.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(restored) != "restored" {
		t.Fatalf("restored file = %q", restored)
	}
	if _, err := os.Stat(filepath.Join(workspace, ".memory-snapshot-old.db")); !os.IsNotExist(err) {
		t.Fatalf("temporary memory snapshot was restored: %v", err)
	}
}
