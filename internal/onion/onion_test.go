package onion

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIsOnionURL(t *testing.T) {
	// A v3 address is 56 base32 characters.
	v3 := strings.Repeat("abc234", 9) + "ab" // 56 characters
	if len(v3) != 56 {
		t.Fatalf("test address is %d characters, want 56", len(v3))
	}

	onionCases := []string{
		"http://" + v3 + ".onion",
		v3 + ".onion",
		"http://" + v3 + ".onion:8080/manifest.json",
		"https://" + strings.ToUpper(v3) + ".onion",
		"  " + v3 + ".onion  ",
	}
	for _, addr := range onionCases {
		if !IsOnionURL(addr) {
			t.Errorf("IsOnionURL(%q) = false, want true", addr)
		}
	}

	clearnetCases := []string{
		"",
		"http://127.0.0.1:8080",
		"127.0.0.1:8080",
		"http://example.com",
		"http://onion.example.com",
		"http://.onion",
		"http://" + strings.Repeat("a", 55) + ".onion",  // one character short
		"http://" + strings.Repeat("a", 57) + ".onion",  // one character long
		"http://" + strings.Repeat("a", 55) + "1.onion", // '1' is not base32
		"http://" + strings.Repeat("a", 55) + "0.onion", // '0' is not base32
	}
	for _, addr := range clearnetCases {
		if IsOnionURL(addr) {
			t.Errorf("IsOnionURL(%q) = true, want false", addr)
		}
	}
}

func TestConfigRemotePort(t *testing.T) {
	for _, tc := range []struct {
		in   int
		want int
	}{
		{0, DefaultRemotePort},
		{-1, DefaultRemotePort},
		{70000, DefaultRemotePort},
		{8080, 8080},
		{1, 1},
		{65535, 65535},
	} {
		if got := (Config{RemotePort: tc.in}).remotePort(); got != tc.want {
			t.Errorf("remotePort(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestDefaultDataDirIsUsable(t *testing.T) {
	if got := DefaultDataDir(); got == "" {
		t.Fatal("DefaultDataDir returned an empty path")
	}
}

// TestServiceKeyPersists is what makes the onion address stable across runs.
func TestServiceKeyPersists(t *testing.T) {
	dataDir := t.TempDir()

	first, err := serviceKey(dataDir)
	if err != nil {
		t.Fatalf("serviceKey: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, onionKeyName)); err != nil {
		t.Fatalf("key file was not written: %v", err)
	}

	second, err := serviceKey(dataDir)
	if err != nil {
		t.Fatalf("serviceKey (reload): %v", err)
	}
	if string(first.PublicKey()) != string(second.PublicKey()) {
		t.Error("reloaded key has a different public key: the address would change")
	}
}

func TestServiceKeyRejectsCorruptFile(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, onionKeyName), []byte("too short"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := serviceKey(dataDir); err == nil {
		t.Fatal("serviceKey accepted a corrupt key file")
	}
}

// TestServiceKeyEphemeral checks that no data directory means no persistence.
func TestServiceKeyEphemeral(t *testing.T) {
	first, err := serviceKey("")
	if err != nil {
		t.Fatalf("serviceKey: %v", err)
	}
	second, err := serviceKey("")
	if err != nil {
		t.Fatalf("serviceKey: %v", err)
	}
	if string(first.PublicKey()) == string(second.PublicKey()) {
		t.Error("ephemeral keys are identical: the address would never change")
	}
}

func TestDialerWithoutRuntime(t *testing.T) {
	runtime := &Runtime{}
	if _, err := runtime.Dialer(0); err == nil {
		t.Fatal("Dialer succeeded without a SOCKS listener")
	}
}

// fakeSocks is a minimal SOCKS5 server that records the credentials of every
// handshake, so tests can prove that each stream gets its own circuit.
type fakeSocks struct {
	listener net.Listener
	mu       sync.Mutex
	users    []string
	passes   []string
}

func newFakeSocks(t *testing.T) *fakeSocks {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := &fakeSocks{listener: listener}
	go server.serve()
	t.Cleanup(func() { listener.Close() })
	return server
}

func (f *fakeSocks) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeSocks) credentials() ([]string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.users...), append([]string(nil), f.passes...)
}

func (f *fakeSocks) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Greeting: VER, NMETHODS, METHODS.
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil || greeting[0] != 5 {
		return
	}
	methods := make([]byte, int(greeting[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{5, 2}); err != nil { // username/password only
		return
	}

	// RFC 1929: VER, ULEN, UNAME, PLEN, PASSWD.
	authHead := make([]byte, 2)
	if _, err := io.ReadFull(conn, authHead); err != nil || authHead[0] != 1 {
		return
	}
	user := make([]byte, int(authHead[1]))
	if _, err := io.ReadFull(conn, user); err != nil {
		return
	}
	passLen := make([]byte, 1)
	if _, err := io.ReadFull(conn, passLen); err != nil {
		return
	}
	pass := make([]byte, int(passLen[0]))
	if _, err := io.ReadFull(conn, pass); err != nil {
		return
	}

	f.mu.Lock()
	f.users = append(f.users, string(user))
	f.passes = append(f.passes, string(pass))
	f.mu.Unlock()

	if _, err := conn.Write([]byte{1, 0}); err != nil {
		return
	}

	// CONNECT request: VER, CMD, RSV, ATYP, ADDR, PORT.
	request := make([]byte, 4)
	if _, err := io.ReadFull(conn, request); err != nil {
		return
	}
	switch request[3] {
	case 1: // IPv4
		if _, err := io.ReadFull(conn, make([]byte, 4)); err != nil {
			return
		}
	case 3: // domain name
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil {
			return
		}
		if _, err := io.ReadFull(conn, make([]byte, int(length[0]))); err != nil {
			return
		}
	case 4: // IPv6
		if _, err := io.ReadFull(conn, make([]byte, 16)); err != nil {
			return
		}
	default:
		return
	}
	if _, err := io.ReadFull(conn, make([]byte, 2)); err != nil { // port
		return
	}

	// Success reply, then let the caller close.
	_, _ = conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
}

// TestDialerUsesDistinctCredentialsPerStream is the circuit-isolation guarantee:
// Tor puts a stream on its own circuit only when the SOCKS credentials differ.
func TestDialerUsesDistinctCredentialsPerStream(t *testing.T) {
	server := newFakeSocks(t)

	runtime, err := Connect(context.Background(), Config{SocksAddr: server.listener.Addr().String()})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer runtime.Close()

	const streams = 3
	for stream := 0; stream < streams; stream++ {
		dial, err := runtime.Dialer(stream)
		if err != nil {
			t.Fatalf("Dialer(%d): %v", stream, err)
		}
		conn, err := dial(context.Background(), "tcp", "example.invalid:80")
		if err != nil {
			t.Fatalf("dial stream %d: %v", stream, err)
		}
		conn.Close()
	}

	users, passwords := server.credentials()
	if len(users) != streams {
		t.Fatalf("SOCKS saw %d handshakes, want %d", len(users), streams)
	}

	seen := map[string]bool{}
	for i, user := range users {
		if user == "" {
			t.Errorf("stream %d authenticated with an empty username", i)
		}
		if seen[user] {
			t.Errorf("username %q was reused, so those streams would share a circuit", user)
		}
		seen[user] = true
		if passwords[i] == "" {
			t.Errorf("stream %d sent an empty password", i)
		}
	}
}
