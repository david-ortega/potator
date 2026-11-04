// Package hashcache remembers SHA256 digests keyed by path, size and
// modification time, so unchanged files are never hashed twice.
//
// Hashing is the slowest part of a sync on both sides: the publisher has to
// hash the whole folder and the subscriber has to hash everything it already
// has. With the cache, a repeat run only hashes files that actually changed.
package hashcache

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry is a single cached digest.
type Entry struct {
	Size    int64  `json:"size"`
	ModTime int64  `json:"mtime"` // UnixNano
	SHA256  string `json:"sha256"`
}

// Cache is a concurrency-safe digest cache persisted as JSON.
type Cache struct {
	mu      sync.Mutex
	path    string
	entries map[string]Entry
	dirty   bool
}

// DefaultPath returns the cache location inside the user cache directory.
func DefaultPath() string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "potator", "hashcache.json")
}

// Open loads the cache stored at path. A missing or unreadable file yields an
// empty cache instead of an error: the cache is only ever an optimisation.
func Open(path string) *Cache {
	cache := &Cache{path: path, entries: map[string]Entry{}}

	data, err := os.ReadFile(path)
	if err != nil {
		return cache
	}
	var file struct {
		Entries map[string]Entry `json:"entries"`
	}
	if err := json.Unmarshal(data, &file); err != nil || file.Entries == nil {
		return cache
	}
	cache.entries = file.Entries
	return cache
}

// OpenDefault opens the cache at DefaultPath.
func OpenDefault() *Cache {
	return Open(DefaultPath())
}

// Lookup returns the cached digest for path when size and mod time still match.
func (c *Cache) Lookup(path string, size int64, modTime time.Time) (string, bool) {
	if c == nil {
		return "", false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[path]
	if !ok || entry.Size != size || entry.ModTime != modTime.UnixNano() {
		return "", false
	}
	return entry.SHA256, true
}

// Store records a digest for path.
func (c *Cache) Store(path string, size int64, modTime time.Time, sha string) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry := Entry{Size: size, ModTime: modTime.UnixNano(), SHA256: sha}
	if existing, ok := c.entries[path]; ok && existing == entry {
		return
	}
	c.entries[path] = entry
	c.dirty = true
}

// Len returns the number of cached entries.
func (c *Cache) Len() int {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Save writes the cache to disk atomically.
func (c *Cache) Save() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.dirty {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return err
	}

	data, err := json.Marshal(struct {
		Entries map[string]Entry `json:"entries"`
	}{Entries: c.entries})
	if err != nil {
		return err
	}

	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, c.path); err != nil {
		os.Remove(tmp)
		return err
	}
	c.dirty = false
	return nil
}
