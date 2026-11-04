package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/david-ortega/potator/internal/progress"
)

// digest returns the hex SHA256 of b.
func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// newFileServer serves content with Range support (like http.FileServer does).
func newFileServer(content []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(content))
	}))
}

func testContent(size int) []byte {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i * 31)
	}
	return content
}

func TestRunChunkedDownload(t *testing.T) {
	content := testContent(3 << 20) // 3 MiB
	server := newFileServer(content)
	defer server.Close()

	dest := filepath.Join(t.TempDir(), "nested", "file.bin")
	bar := progress.NewWriter(io.Discard, int64(len(content)), "test")

	clients := []*http.Client{server.Client(), server.Client(), server.Client()}
	err := Run(context.Background(), Options{
		Clients:   clients,
		Parallel:  3,
		ChunkSize: 512 << 10,
		Progress:  bar,
		Verify:    true,
	}, []Request{{
		URL:      server.URL + "/file.bin",
		RelPath:  "file.bin",
		DestPath: dest,
		Size:     int64(len(content)),
		SHA256:   digest(content),
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(content))
	}
}

func TestRunSmallFileUsesSingleStream(t *testing.T) {
	content := testContent(1024)
	server := newFileServer(content)
	defer server.Close()

	dest := filepath.Join(t.TempDir(), "small.bin")
	err := Run(context.Background(), Options{
		Clients:   []*http.Client{server.Client()},
		Parallel:  4,
		ChunkSize: 4 << 20,
		Verify:    true,
	}, []Request{{
		URL: server.URL + "/small.bin", RelPath: "small.bin", DestPath: dest,
		Size: int64(len(content)), SHA256: digest(content),
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := os.ReadFile(dest)
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch")
	}
}

func TestRunVerifiesChecksum(t *testing.T) {
	announced := testContent(2048)
	// Same length, different bytes than the manifest announces.
	corrupted := append([]byte(nil), announced...)
	corrupted[0] ^= 0xFF
	server := newFileServer(corrupted)
	defer server.Close()

	dest := filepath.Join(t.TempDir(), "corrupt.bin")
	err := Run(context.Background(), Options{
		Clients:  []*http.Client{server.Client()},
		Parallel: 1,
		Verify:   true,
	}, []Request{{
		URL: server.URL + "/corrupt.bin", RelPath: "corrupt.bin", DestPath: dest,
		Size: int64(len(announced)), SHA256: digest(announced),
	}})
	if err == nil {
		t.Fatal("expected a checksum error")
	}

	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Errorf("corrupted file was not removed")
	}
}

func TestRunSkipsVerificationWhenDisabled(t *testing.T) {
	content := testContent(1024)
	server := newFileServer(content)
	defer server.Close()

	dest := filepath.Join(t.TempDir(), "unverified.bin")
	err := Run(context.Background(), Options{
		Clients:  []*http.Client{server.Client()},
		Parallel: 1,
		Verify:   false,
	}, []Request{{
		URL: server.URL + "/f.bin", RelPath: "f.bin", DestPath: dest,
		Size: int64(len(content)), SHA256: "deadbeef",
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("file missing: %v", err)
	}
}

func TestRunDownloadsSeveralFiles(t *testing.T) {
	first := testContent(700 << 10)
	second := testContent(1500 << 10)

	mux := http.NewServeMux()
	mux.HandleFunc("/a.bin", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "a.bin", time.Time{}, bytes.NewReader(first))
	})
	mux.HandleFunc("/b.bin", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "b.bin", time.Time{}, bytes.NewReader(second))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := t.TempDir()
	requests := []Request{
		{URL: server.URL + "/a.bin", RelPath: "a.bin", DestPath: filepath.Join(dir, "a.bin"), Size: int64(len(first)), SHA256: digest(first)},
		{URL: server.URL + "/b.bin", RelPath: "b.bin", DestPath: filepath.Join(dir, "sub", "b.bin"), Size: int64(len(second)), SHA256: digest(second)},
	}

	if err := Run(context.Background(), Options{
		Clients:   []*http.Client{server.Client(), server.Client()},
		Parallel:  2,
		ChunkSize: 256 << 10,
		Verify:    true,
	}, requests); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for i, want := range [][]byte{first, second} {
		got, err := os.ReadFile(requests[i].DestPath)
		if err != nil {
			t.Fatalf("read %s: %v", requests[i].RelPath, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s content mismatch", requests[i].RelPath)
		}
	}
}

func TestRunRespectsCancelledContext(t *testing.T) {
	content := testContent(1024)
	server := newFileServer(content)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Run(ctx, Options{
		Clients:  []*http.Client{server.Client()},
		Parallel: 1,
	}, []Request{{
		URL: server.URL + "/f.bin", RelPath: "f.bin",
		DestPath: filepath.Join(t.TempDir(), "f.bin"),
		Size:     int64(len(content)), SHA256: digest(content),
	}})
	if err == nil {
		t.Fatal("expected an error for a cancelled context")
	}
}

func TestRunWithoutClients(t *testing.T) {
	if err := Run(context.Background(), Options{}, []Request{{RelPath: "x"}}); err == nil {
		t.Fatal("expected an error when no clients are configured")
	}
}

func TestShortName(t *testing.T) {
	if got := shortName("a/b.txt"); got != "a/b.txt" {
		t.Errorf("shortName must keep short paths unchanged, got %q", got)
	}
	long := strings.Repeat("x", 100)
	if got := shortName(long, 20); len(got) != 20 || !strings.HasPrefix(got, "...") {
		t.Errorf("shortName(%d chars, 20) = %q", len(long), got)
	}
}
