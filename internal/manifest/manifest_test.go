package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFile creates a file with the given content below dir.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestGenerateWithProgressReportsBytes(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "hello")
	writeFile(t, dir, "sub/b.txt", "world!!")

	var (
		calls    int
		lastDone int64
		totals   = map[int64]bool{}
	)

	m, err := GenerateWithProgress(dir, func(done, total int64) {
		calls++
		lastDone = done
		totals[total] = true
		if done > total {
			t.Errorf("done (%d) exceeded total (%d)", done, total)
		}
	})
	if err != nil {
		t.Fatalf("GenerateWithProgress: %v", err)
	}

	// 5 + 7 bytes.
	const wantTotal = 12
	if lastDone != wantTotal {
		t.Errorf("final done = %d, want %d", lastDone, wantTotal)
	}
	if !totals[wantTotal] {
		t.Errorf("total %d never reported, saw %v", wantTotal, totals)
	}
	if calls == 0 {
		t.Error("progress callback was never called")
	}
	if m.TotalSize() != wantTotal {
		t.Errorf("TotalSize() = %d, want %d", m.TotalSize(), wantTotal)
	}
	if len(m.Files) != 2 {
		t.Errorf("files = %d, want 2", len(m.Files))
	}
	if _, ok := m.Files["sub/b.txt"]; !ok {
		t.Errorf("nested path with forward slashes missing: %v", m.Files)
	}
}

func TestGenerateSkipsManifestAndHiddenFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "keep.txt", "data")
	writeFile(t, dir, ManifestName, `{"version":1}`)
	writeFile(t, dir, ".hidden", "secret")

	m, err := Generate(dir)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if len(m.Files) != 1 {
		t.Fatalf("files = %v, want only keep.txt", m.Files)
	}
	if _, ok := m.Files["keep.txt"]; !ok {
		t.Errorf("keep.txt missing: %v", m.Files)
	}
}

func TestGenerateWithoutProgressDoesNotPanic(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.bin", "abc")

	if _, err := Generate(dir); err != nil {
		t.Fatalf("Generate: %v", err)
	}
}

func TestGenerateSkipsHiddenFolders(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "keep.txt", "data")
	writeFile(t, dir, ".git/config", "[core]")
	writeFile(t, dir, ".ssh/id_rsa", "private key")

	m, err := Generate(dir)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if len(m.Files) != 1 {
		t.Fatalf("files = %v, want only keep.txt", m.Files)
	}
	for _, leaked := range []string{".git/config", ".ssh/id_rsa"} {
		if _, ok := m.Files[leaked]; ok {
			t.Errorf("%s from a hidden folder was published", leaked)
		}
	}
}

func TestCalculateSHA256MatchesKnownDigest(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "abc")

	got, err := CalculateSHA256(filepath.Join(dir, "a.txt"))
	if err != nil {
		t.Fatalf("CalculateSHA256: %v", err)
	}
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Errorf("digest = %s, want %s", got, want)
	}
}
