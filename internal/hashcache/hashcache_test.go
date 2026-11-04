package hashcache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const testDigest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

func TestLookupHitAndMiss(t *testing.T) {
	cache := Open(filepath.Join(t.TempDir(), "cache.json"))
	modTime := time.Unix(1700000000, 0)

	// Unknown path.
	if _, ok := cache.Lookup("/a", 1, modTime); ok {
		t.Fatal("expected a miss for an unknown path")
	}

	cache.Store("/a", 1, modTime, testDigest)

	if sha, ok := cache.Lookup("/a", 1, modTime); !ok || sha != testDigest {
		t.Fatalf("Lookup = %q, %v; want %q, true", sha, ok, testDigest)
	}

	// Size changed.
	if _, ok := cache.Lookup("/a", 2, modTime); ok {
		t.Error("expected a miss when the size differs")
	}

	// Modification time changed.
	if _, ok := cache.Lookup("/a", 1, modTime.Add(time.Second)); ok {
		t.Error("expected a miss when the mod time differs")
	}
}

func TestSaveAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	modTime := time.Unix(1700000000, 123456789)

	cache := Open(path)
	cache.Store("/b", 42, modTime, testDigest)
	if err := cache.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reopened := Open(path)
	if reopened.Len() != 1 {
		t.Fatalf("Len = %d, want 1", reopened.Len())
	}
	if sha, ok := reopened.Lookup("/b", 42, modTime); !ok || sha != testDigest {
		t.Fatalf("Lookup after reopen = %q, %v", sha, ok)
	}
}

func TestOpenMissingFileIsEmpty(t *testing.T) {
	cache := Open(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if cache.Len() != 0 {
		t.Errorf("Len = %d, want 0", cache.Len())
	}
}

func TestOpenCorruptFileIsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	cache := Open(path)
	if cache.Len() != 0 {
		t.Errorf("corrupt cache must be ignored, Len = %d", cache.Len())
	}
}

func TestNilCacheIsSafe(t *testing.T) {
	var cache *Cache

	if _, ok := cache.Lookup("/x", 1, time.Now()); ok {
		t.Error("nil cache must never hit")
	}
	cache.Store("/x", 1, time.Now(), testDigest)
	if cache.Len() != 0 {
		t.Error("nil cache must stay empty")
	}
	if err := cache.Save(); err != nil {
		t.Errorf("Save on nil cache = %v", err)
	}
}

func TestSaveWithoutChangesIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")

	cache := Open(path)
	if err := cache.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("an unchanged cache must not be written")
	}
}
