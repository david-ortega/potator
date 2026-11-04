package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/david-ortega/potator/internal/manifest"
)

func TestParseSize(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"4MiB", 4 << 20},
		{"512kb", 512 << 10},
		{"1g", 1 << 30},
		{"2 mib", 2 << 20},
	} {
		got, err := parseSize(tc.in)
		if err != nil {
			t.Fatalf("parseSize(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("parseSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}

	for _, bad := range []string{"", "abc", "0", "-5", "MiB"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q) was accepted", bad)
		}
	}
}

func TestPublishedURL(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"127.0.0.1:8080", "http://127.0.0.1:8080"},
		{"0.0.0.0:9000", "http://127.0.0.1:9000"},
		{":9000", "http://127.0.0.1:9000"},
		{"[::]:9000", "http://127.0.0.1:9000"},
	} {
		if got := publishedURL(tc.in); got != tc.want {
			t.Errorf("publishedURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// setListenFlag points the publisher at a test address for the duration of a
// test. The flag is package state, so tests must not run in parallel.
func setListenFlag(t *testing.T, addr string) {
	t.Helper()
	previous := listenFlag
	listenFlag = addr
	t.Cleanup(func() { listenFlag = previous })
}

// setTransport forces a transport choice for the duration of a test.
func setTransport(t *testing.T, value string) {
	t.Helper()
	previous := transportFlag
	transportFlag = value
	t.Cleanup(func() { transportFlag = previous })
}

func TestResolveTransport(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want transportKind
	}{
		{"", transportAuto},
		{"auto", transportAuto},
		{"tor", transportOnion},
		{"onion", transportOnion},
		{"TOR", transportOnion},
		{"lan", transportLan},
		{"local", transportLan},
		{" http ", transportLan},
	} {
		got, err := resolveTransport(tc.in)
		if err != nil {
			t.Fatalf("resolveTransport(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("resolveTransport(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}

	if _, err := resolveTransport("carrier-pigeon"); err == nil {
		t.Error("resolveTransport accepted an unknown transport")
	}
}

// TestOpenClientRefusesOnionOverLan is the leak guard: a .onion address must
// never be fetched with the direct transport.
func TestOpenClientRefusesOnionOverLan(t *testing.T) {
	setTransport(t, "lan")

	const onionURL = "http://" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + ".onion" // 56 base32 chars
	client, closeClient, err := openClient(context.Background(), onionURL)
	if err == nil {
		closeClient()
		_ = client
		t.Fatal("openClient accepted a .onion address over the lan transport")
	}
}

// TestOpenClientRejectsOnionFlagForClearnet covers the mirror-image mistake.
func TestOpenClientRejectsOnionFlagForClearnet(t *testing.T) {
	setTransport(t, "tor")

	client, closeClient, err := openClient(context.Background(), "http://127.0.0.1:8080")
	if err == nil {
		closeClient()
		_ = client
		t.Fatal("--transport=tor accepted a clearnet address")
	}
}

// waitForFile blocks until path exists, so tests never race the publisher.
func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

// TestRunPubCleansUp is the contract described in the README: a publisher that
// shuts down leaves no manifest.json behind in the shared folder.
func TestRunPubCleansUp(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	setListenFlag(t, "127.0.0.1:0")
	setTransport(t, "lan")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runPub(ctx, src) }()

	manifestPath := filepath.Join(src, manifest.ManifestName)
	waitForFile(t, manifestPath)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runPub: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("runPub did not stop after cancellation")
	}

	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Errorf("manifest.json survived shutdown (stat err = %v)", err)
	}
}

// TestRunPubCleansUpOnStartFailure covers the early-error path: if the listener
// cannot bind, the generated manifest must still be removed.
func TestRunPubCleansUpOnStartFailure(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	setListenFlag(t, "127.0.0.1:70000") // port out of range
	setTransport(t, "lan")

	if err := runPub(context.Background(), src); err == nil {
		t.Fatal("runPub accepted an unbindable address")
	}

	manifestPath := filepath.Join(src, manifest.ManifestName)
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Errorf("manifest.json survived a failed start (stat err = %v)", err)
	}
}

func TestRunPubRejectsBadFolder(t *testing.T) {
	if err := runPub(context.Background(), filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("runPub accepted a missing folder")
	}

	file := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := runPub(context.Background(), file); err == nil {
		t.Error("runPub accepted a file")
	}
}

func TestRunSubRejectsBadOverwritePolicy(t *testing.T) {
	previous := overwriteFlag
	overwriteFlag = "yolo"
	t.Cleanup(func() { overwriteFlag = previous })

	if err := runSub(context.Background(), "127.0.0.1:1", t.TempDir()); err == nil {
		t.Error("runSub accepted an invalid --overwrite policy")
	}
}
