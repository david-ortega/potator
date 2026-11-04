package sync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/david-ortega/potator/internal/hashcache"
	"github.com/david-ortega/potator/internal/manifest"
)

// setup creates a destination dir with one unchanged file.
func setup(t *testing.T) (string, *manifest.Manifest) {
	t.Helper()

	dir := t.TempDir()
	content := []byte("hello world")
	if err := os.WriteFile(filepath.Join(dir, "same.txt"), content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	sameHash, err := manifest.CalculateSHA256(filepath.Join(dir, "same.txt"))
	if err != nil {
		t.Fatalf("hash: %v", err)
	}

	remote := &manifest.Manifest{
		Version: 1,
		Files: map[string]manifest.FileInfo{
			"same.txt":    {Size: int64(len(content)), SHA256: sameHash},
			"missing.txt": {Size: 3, SHA256: "abc"},
			"changed.txt": {Size: 999, SHA256: "def"},
		},
	}
	// A locally existing file whose size differs from the remote one.
	if err := os.WriteFile(filepath.Join(dir, "changed.txt"), []byte("short"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	return dir, remote
}

func TestCompareManifestsActions(t *testing.T) {
	dir, remote := setup(t)

	tasks, err := CompareManifestsWithOptions(remote, dir, "always", Options{})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}

	byPath := map[string]FileTask{}
	for _, task := range tasks {
		byPath[task.RelPath] = task
	}

	if got := byPath["same.txt"].Action; got != ActionSkip {
		t.Errorf("same.txt action = %s, want SKIP", got)
	}
	if got := byPath["missing.txt"].Action; got != ActionDownload {
		t.Errorf("missing.txt action = %s, want DOWNLOAD", got)
	}
	if got := byPath["changed.txt"].Action; got != ActionDownload {
		t.Errorf("changed.txt action = %s, want DOWNLOAD", got)
	}

	// Tasks carry the metadata the transfer engine needs.
	if byPath["missing.txt"].Size != 3 || byPath["missing.txt"].SHA256 != "abc" {
		t.Errorf("missing.txt metadata = %+v", byPath["missing.txt"])
	}
}

func TestCompareManifestsUsesCache(t *testing.T) {
	dir, remote := setup(t)
	cache := hashcache.Open(filepath.Join(t.TempDir(), "cache.json"))

	// First run fills the cache.
	if _, err := CompareManifestsWithOptions(remote, dir, "always", Options{Cache: cache}); err != nil {
		t.Fatalf("first compare: %v", err)
	}
	if cache.Len() == 0 {
		t.Fatal("cache was not populated")
	}

	// Delete the local file content but keep the cache valid: with a matching
	// mtime and size the comparison must not need to re-hash, so the task stays
	// SKIP even though the bytes on disk were swapped.
	path := filepath.Join(dir, "same.txt")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.WriteFile(path, []byte("HELLO WORLD"), info.Mode()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	tasks, err := CompareManifestsWithOptions(remote, dir, "always", Options{Cache: cache})
	if err != nil {
		t.Fatalf("second compare: %v", err)
	}
	for _, task := range tasks {
		if task.RelPath == "same.txt" && task.Action != ActionSkip {
			t.Errorf("cached file was re-hashed: action = %s", task.Action)
		}
	}
}

func TestCompareManifestsOverwritePolicies(t *testing.T) {
	dir := t.TempDir()
	content := []byte("local version")
	path := filepath.Join(dir, "f.txt")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	remote := &manifest.Manifest{Files: map[string]manifest.FileInfo{
		"f.txt": {Size: int64(len(content)), SHA256: "different"},
	}}

	for _, mode := range []string{"skip", "always"} {
		tasks, err := CompareManifestsWithOptions(remote, dir, mode, Options{})
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if len(tasks) != 1 {
			t.Fatalf("%s: got %d tasks", mode, len(tasks))
		}
		want := ActionDownload
		if mode == "skip" {
			want = ActionSkip
		}
		if tasks[0].Action != want {
			t.Errorf("%s: action = %s, want %s", mode, tasks[0].Action, want)
		}
	}
}
