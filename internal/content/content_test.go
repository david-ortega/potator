package content

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFolder creates a temporary folder holding the given files.
func writeFolder(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	for name, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", name, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

func TestNodeRejectsNonFolder(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := NewNode(file, nil); err == nil {
		t.Fatal("NewNode accepted a file as content folder")
	}
	if _, err := NewNode(filepath.Join(t.TempDir(), "missing"), nil); err == nil {
		t.Fatal("NewNode accepted a missing folder")
	}
}

func TestHandlerServesFiles(t *testing.T) {
	node, err := NewNode(writeFolder(t, map[string]string{
		"a.txt":     "hello",
		"sub/b.bin": "world",
	}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	server := httptest.NewServer(node.Handler())
	defer server.Close()

	for _, tc := range []struct{ path, want string }{
		{"/a.txt", "hello"},
		{"/sub/b.bin", "world"},
	} {
		response, err := http.Get(server.URL + tc.path)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("read %s: %v", tc.path, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", tc.path, response.StatusCode)
		}
		if string(body) != tc.want {
			t.Errorf("GET %s body = %q, want %q", tc.path, body, tc.want)
		}
	}
}

// TestHandlerSupportsRange guards the feature transfer relies on: large files
// are fetched as several parallel Range requests.
func TestHandlerSupportsRange(t *testing.T) {
	node, err := NewNode(writeFolder(t, map[string]string{"data.bin": "0123456789"}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	server := httptest.NewServer(node.Handler())
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+"/data.bin", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Range", "bytes=2-5")

	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("GET with range: %v", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if response.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want 206", response.StatusCode)
	}
	if string(body) != "2345" {
		t.Errorf("body = %q, want %q", body, "2345")
	}
}

// TestHandlerHidesMissingFiles makes sure a probe cannot distinguish "absent"
// from "outside the shared folder".
func TestHandlerHidesMissingFiles(t *testing.T) {
	node, err := NewNode(writeFolder(t, map[string]string{"a.txt": "hello"}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	server := httptest.NewServer(node.Handler())
	defer server.Close()

	for _, path := range []string{"/missing.txt", "/sub/", "/nope/"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want 404", path, response.StatusCode)
		}
	}
}

func TestResolveStaysInsideRoot(t *testing.T) {
	root := t.TempDir()
	node, err := NewNode(root, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	h, ok := node.Handler().(*handler)
	if !ok {
		t.Fatal("Handler() did not return a *handler")
	}

	// Every one of these must either be rejected or land inside root; none may
	// reach a file above it.
	for _, raw := range []string{
		"/../secret.txt",
		"/../../../../etc/passwd",
		"/a/../../secret.txt",
		"/sub/../../../secret.txt",
		"/./../secret.txt",
	} {
		resolved, err := h.resolve(raw)
		if err != nil {
			continue // refused outright, which is equally safe
		}
		if resolved != root && !strings.HasPrefix(resolved, root+string(os.PathSeparator)) {
			t.Errorf("resolve(%q) = %q, escapes root %q", raw, resolved, root)
		}
	}
}

func TestClientNormalizesAddress(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"127.0.0.1:8080", "http://127.0.0.1:8080"},
		{"http://127.0.0.1:8080", "http://127.0.0.1:8080"},
		{"http://127.0.0.1:8080/", "http://127.0.0.1:8080"},
		{"  example.local  ", "http://example.local"},
	} {
		client, err := NewClient(tc.in)
		if err != nil {
			t.Fatalf("NewClient(%q): %v", tc.in, err)
		}
		if client.BaseURL() != tc.want {
			t.Errorf("NewClient(%q).BaseURL() = %q, want %q", tc.in, client.BaseURL(), tc.want)
		}
	}

	for _, bad := range []string{"", "   ", "://no-host"} {
		if _, err := NewClient(bad); err == nil {
			t.Errorf("NewClient(%q) was accepted", bad)
		}
	}
}

func TestClientURL(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:8080")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	for _, tc := range []struct{ in, want string }{
		{"a.txt", "http://127.0.0.1:8080/a.txt"},
		{"/a.txt", "http://127.0.0.1:8080/a.txt"},
		{"sub/b.bin", "http://127.0.0.1:8080/sub/b.bin"},
	} {
		if got := client.URL(tc.in); got != tc.want {
			t.Errorf("URL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestClientGet(t *testing.T) {
	node, err := NewNode(writeFolder(t, map[string]string{"a.txt": "hello"}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	server := httptest.NewServer(node.Handler())
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	response, err := client.Get(context.Background(), "a.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != "hello" {
		t.Errorf("Get = %d %q, want 200 %q", response.StatusCode, body, "hello")
	}

	missing, err := client.Get(context.Background(), "missing.txt")
	if err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Errorf("Get missing status = %d, want 404", missing.StatusCode)
	}
}

// TestClientUsesInjectedDialer proves the injected dialer replaces the operating
// system's network stack: the URL host does not resolve, so a request can only
// succeed if the dialer is really used.
func TestClientUsesInjectedDialer(t *testing.T) {
	node, err := NewNode(writeFolder(t, map[string]string{"a.txt": "hello"}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	server := httptest.NewServer(node.Handler())
	defer server.Close()

	realAddr := strings.TrimPrefix(server.URL, "http://")
	dials := 0

	client, err := NewClientWithDialer("http://potator.invalid:9999", func(stream int) (DialerFunc, error) {
		if stream != 0 {
			t.Errorf("primary client used stream %d, want 0", stream)
		}
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials++
			if addr == realAddr {
				t.Error("dialer received the real address: the transport bypassed it")
			}
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", realAddr)
		}, nil
	})
	if err != nil {
		t.Fatalf("NewClientWithDialer: %v", err)
	}

	response, err := client.Get(context.Background(), "a.txt")
	if err != nil {
		t.Fatalf("Get through dialer: %v", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "hello" {
		t.Errorf("body = %q, want %q", body, "hello")
	}
	if dials == 0 {
		t.Error("the injected dialer was never called")
	}
}

// TestClientStreamsUseFactoryIndexes checks that stream i gets dialer i, which is
// what puts each transfer stream on its own Tor circuit.
func TestClientStreamsUseFactoryIndexes(t *testing.T) {
	var requested []int

	client, err := NewClientWithDialer("http://potator.invalid:9999", func(stream int) (DialerFunc, error) {
		requested = append(requested, stream)
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			return nil, context.DeadlineExceeded
		}, nil
	})
	if err != nil {
		t.Fatalf("NewClientWithDialer: %v", err)
	}
	requested = nil // ignore the primary client built by the constructor

	streams, err := client.Streams(3)
	if err != nil {
		t.Fatalf("Streams: %v", err)
	}
	if len(streams) != 3 {
		t.Fatalf("Streams(3) returned %d clients", len(streams))
	}
	for i, stream := range requested {
		if stream != i {
			t.Errorf("factory call %d got index %d", i, stream)
		}
	}
	if len(requested) != 3 {
		t.Fatalf("factory called %d times, want 3", len(requested))
	}
}

func TestNewClientWithDialerRejectsNil(t *testing.T) {
	if _, err := NewClientWithDialer("http://127.0.0.1:8080", nil); err == nil {
		t.Fatal("NewClientWithDialer accepted a nil factory")
	}
}

// TestStartOnRejectsNilListener keeps the transport contract honest.
func TestStartOnRejectsNilListener(t *testing.T) {
	node, err := NewNode(writeFolder(t, map[string]string{"a.txt": "hello"}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.StartOn(context.Background(), nil); err == nil {
		t.Fatal("StartOn accepted a nil listener")
	}
}

// TestClientStreamsAreIndependent checks that --parallel gets separate clients
// rather than the same one repeated.
func TestClientStreamsAreIndependent(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:8080")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	streams, err := client.Streams(3)
	if err != nil {
		t.Fatalf("Streams(3): %v", err)
	}
	if len(streams) != 3 {
		t.Fatalf("Streams(3) returned %d clients", len(streams))
	}
	seen := map[*http.Client]bool{}
	for _, stream := range streams {
		if seen[stream] {
			t.Error("Streams returned the same client twice")
		}
		seen[stream] = true
	}

	single, err := client.Streams(0)
	if err != nil {
		t.Fatalf("Streams(0): %v", err)
	}
	if len(single) != 1 {
		t.Errorf("Streams(0) returned %d clients, want 1", len(single))
	}
}

func TestNodeStartServesAndStops(t *testing.T) {
	node, err := NewNode(writeFolder(t, map[string]string{"a.txt": "hello"}), &Config{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := node.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer node.Stop()

	if strings.HasSuffix(node.Addr(), ":0") {
		t.Fatalf("Addr() = %q, want the resolved port", node.Addr())
	}

	client, err := NewClient(node.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	response, err := client.Get(context.Background(), "a.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", response.StatusCode)
	}

	node.Stop()
	node.Stop() // must not panic
}
