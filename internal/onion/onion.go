// Package onion publishes and reaches PotaTor folders through Tor v3 onion
// services.
//
// Two modes share one implementation:
//
//	Publish  starts Tor, creates a v3 onion service forwarded to a local
//	         listener, and reports the .onion address subscribers should use.
//	Connect  starts Tor only, and hands out per-stream dialers for fetching
//	         from somebody else's onion service.
//
// Dialers use distinct SOCKS credentials per stream, so Tor's IsolateSOCKSAuth
// places each transfer stream on its own circuit. That is what makes
// "sub --parallel" multiply throughput instead of queueing behind a single
// circuit. Tor's default SocksPort enables IsolateSOCKSAuth.
package onion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/cretz/bine/tor"
	"github.com/cretz/bine/torutil/ed25519"
	"golang.org/x/net/proxy"
)

const (
	// DefaultRemotePort is the port subscribers connect to on the onion address.
	DefaultRemotePort = 80

	// onionKeyName is the file holding the ed25519 service key, so a publisher
	// keeps the same address across restarts.
	onionKeyName = "onion.key"

	// torDataName is the subdirectory holding Tor's own state (consensus,
	// descriptors), which makes restarts much faster.
	torDataName = "tor"

	// socksPassword is paired with the per-stream SOCKS username. Tor only
	// needs the username to differ, but RFC 1929 carries both fields.
	socksPassword = "potator"
)

// Config configures a Tor runtime.
type Config struct {
	// TorPath is the tor executable; empty means "tor" on PATH.
	TorPath string

	// DataDir keeps Tor state and the onion service key so the address survives
	// a restart. Empty means everything is temporary.
	DataDir string

	// RemotePort is the port the onion service exposes. Zero means
	// DefaultRemotePort.
	RemotePort int

	// Fast disables Tor's connection padding. Slightly faster, slightly less
	// metadata protection: opt-in only.
	Fast bool

	// Debug, when set, receives Tor's own log output.
	Debug io.Writer

	// SocksAddr points at an already running Tor SOCKS port instead of starting
	// one. Only used by Connect.
	SocksAddr string
}

// Runtime owns a Tor instance and, for a publisher, one onion service.
type Runtime struct {
	cfg        Config
	instance   *tor.Tor
	service    *tor.OnionService
	socksNet   string
	socksAddr  string
	closeOnce  sync.Once
	closeError error
}

// Publish creates a v3 onion service forwarding to a local listener owned by the
// runtime. Serve it with content.Node.StartOn and shut it down with Close.
func Publish(ctx context.Context, cfg Config) (*Runtime, error) {
	runtime, err := start(ctx, cfg)
	if err != nil {
		return nil, err
	}

	key, err := serviceKey(cfg.DataDir)
	if err != nil {
		runtime.Close()
		return nil, err
	}

	service, err := runtime.instance.Listen(ctx, &tor.ListenConf{
		Key:         key,
		RemotePorts: []int{cfg.remotePort()},
	})
	if err != nil {
		runtime.Close()
		return nil, fmt.Errorf("onion: create onion service: %w", err)
	}
	runtime.service = service
	return runtime, nil
}

// Connect starts Tor (or reuses SocksAddr) and returns a runtime that only dials.
func Connect(ctx context.Context, cfg Config) (*Runtime, error) {
	return start(ctx, cfg)
}

// start brings up Tor and resolves the SOCKS address every dialer needs.
func start(ctx context.Context, cfg Config) (*Runtime, error) {
	runtime := &Runtime{cfg: cfg, socksNet: "tcp"}

	if addr := strings.TrimSpace(cfg.SocksAddr); addr != "" {
		runtime.socksAddr = addr
		return runtime, nil
	}

	startConf := &tor.StartConf{ExePath: cfg.TorPath, DebugWriter: cfg.Debug}
	if cfg.DataDir != "" {
		startConf.DataDir = filepath.Join(cfg.DataDir, torDataName)
		if err := os.MkdirAll(startConf.DataDir, 0o700); err != nil {
			return nil, fmt.Errorf("onion: prepare Tor data directory: %w", err)
		}
	}
	if cfg.Fast {
		startConf.ExtraArgs = []string{"--ConnectionPadding", "0"}
	}

	instance, err := tor.Start(ctx, startConf)
	if err != nil {
		return nil, fmt.Errorf("onion: start Tor: %w (install Tor, pass --tor-path, or use --transport=lan)", err)
	}
	runtime.instance = instance

	if err := runtime.resolveSocks(); err != nil {
		runtime.Close()
		return nil, err
	}
	return runtime, nil
}

// resolveSocks asks the controller where Tor actually listens for SOCKS.
func (r *Runtime) resolveSocks() error {
	const key = "net/listeners/socks"
	values, err := r.instance.Control.GetInfo(key)
	if err != nil {
		return fmt.Errorf("onion: query Tor SOCKS address: %w", err)
	}
	for _, kv := range values {
		if kv.Key != key || kv.Val == "" {
			continue
		}
		address := kv.Val
		if strings.HasPrefix(address, "unix:") {
			r.socksNet = "unix"
			address = strings.TrimPrefix(address, "unix:")
		}
		r.socksAddr = address
		return nil
	}
	return errors.New("onion: Tor reported no SOCKS listener")
}

// Dialer returns a dialer for one transfer stream. Each stream authenticates to
// Tor's SOCKS port with a distinct username, which puts it on its own circuit.
func (r *Runtime) Dialer(stream int) (func(ctx context.Context, network, addr string) (net.Conn, error), error) {
	if r.socksAddr == "" {
		return nil, errors.New("onion: runtime has no SOCKS listener (call Connect first)")
	}

	auth := &proxy.Auth{User: "potator-" + strconv.Itoa(stream), Password: socksPassword}
	dialer, err := proxy.SOCKS5(r.socksNet, r.socksAddr, auth, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("onion: build SOCKS dialer for stream %d: %w", stream, err)
	}
	if contextDialer, ok := dialer.(proxy.ContextDialer); ok {
		return contextDialer.DialContext, nil
	}

	// x/net/proxy falls back to the plain Dialer interface for some
	// implementations; keep cancellation working in that case.
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		type result struct {
			conn net.Conn
			err  error
		}
		done := make(chan result, 1)
		go func() {
			conn, dialErr := dialer.Dial(network, addr)
			done <- result{conn: conn, err: dialErr}
		}()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-done:
			if res.err != nil {
				return nil, res.err
			}
			if err := ctx.Err(); err != nil {
				res.conn.Close()
				return nil, err
			}
			return res.conn, nil
		}
	}, nil
}

// Listener returns the local listener the onion service forwards to. It is nil
// for a runtime created by Connect.
func (r *Runtime) Listener() net.Listener {
	if r.service == nil {
		return nil
	}
	return r.service
}

// ID returns the onion service identifier, without the .onion suffix.
func (r *Runtime) ID() string {
	if r.service == nil {
		return ""
	}
	return r.service.ID
}

// Address returns the URL subscribers should use.
func (r *Runtime) Address() string {
	if r.service == nil {
		return ""
	}
	return fmt.Sprintf("http://%s.onion", r.service.ID)
}

// Close removes the onion service and stops Tor. It is safe to call twice.
func (r *Runtime) Close() error {
	r.closeOnce.Do(func() {
		var problems []string
		if r.service != nil {
			if err := r.service.Close(); err != nil {
				problems = append(problems, err.Error())
			}
			r.service = nil
		}
		if r.instance != nil {
			if err := r.instance.Close(); err != nil {
				problems = append(problems, err.Error())
			}
			r.instance = nil
		}
		if len(problems) > 0 {
			r.closeError = fmt.Errorf("onion: shutdown: %s", strings.Join(problems, "; "))
		}
	})
	return r.closeError
}

// remotePort applies the configured port with its default and range check.
func (c Config) remotePort() int {
	if c.RemotePort < 1 || c.RemotePort > 65535 {
		return DefaultRemotePort
	}
	return c.RemotePort
}

// serviceKey loads the persisted ed25519 key, or generates and stores a new one.
// With no data directory the key is generated per run, so the address changes.
func serviceKey(dataDir string) (ed25519.KeyPair, error) {
	if dataDir == "" {
		return ed25519.GenerateKey(nil)
	}

	keyPath := filepath.Join(dataDir, onionKeyName)
	stored, err := os.ReadFile(keyPath)
	switch {
	case err == nil:
		if len(stored) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("onion: %s is corrupt (%d bytes, want %d)", keyPath, len(stored), ed25519.PrivateKeySize)
		}
		return ed25519.PrivateKey(stored).KeyPair(), nil
	case !errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("onion: read %s: %w", keyPath, err)
	}

	key, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("onion: generate service key: %w", err)
	}
	if err := writeKey(keyPath, key); err != nil {
		return nil, err
	}
	return key, nil
}

// writeKey persists a service key with owner-only permissions.
func writeKey(keyPath string, key ed25519.KeyPair) error {
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return fmt.Errorf("onion: create %s: %w", filepath.Dir(keyPath), err)
	}

	tmp := keyPath + ".tmp"
	if err := os.WriteFile(tmp, key.PrivateKey(), 0o600); err != nil {
		return fmt.Errorf("onion: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, keyPath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("onion: store %s: %w", keyPath, err)
	}
	return nil
}

// CheckAvailable reports whether the configured Tor executable can be found,
// without starting it, so callers can fail before doing expensive work.
func CheckAvailable(cfg Config) error {
	if strings.TrimSpace(cfg.SocksAddr) != "" {
		return nil
	}

	path := strings.TrimSpace(cfg.TorPath)
	if path == "" {
		if _, err := exec.LookPath("tor"); err != nil {
			return errors.New("onion: Tor not found on PATH: install Tor, pass --tor-path, or use --transport=lan")
		}
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("onion: --tor-path %q: %w", path, err)
	}
	return nil
}

// IsOnionURL reports whether addr points at a Tor v3 onion service. It accepts a
// bare host, host:port or a full URL, and requires the address to look like a v3
// service: 56 base32 characters followed by ".onion".
func IsOnionURL(addr string) bool {
	text := strings.TrimSpace(addr)
	if text == "" {
		return false
	}
	if !strings.Contains(text, "://") {
		text = "http://" + text
	}

	parsed, err := url.Parse(text)
	if err != nil {
		return false
	}

	host := strings.ToLower(parsed.Hostname())
	label, found := strings.CutSuffix(host, ".onion")
	if !found || len(label) != 56 {
		return false
	}
	for _, char := range label {
		// Base32 without padding: lower-case letters and digits 2-7.
		if (char < 'a' || char > 'z') && (char < '2' || char > '7') {
			return false
		}
	}
	return true
}

// DefaultDataDir is where the onion key and Tor state live by default.
func DefaultDataDir() string {
	base, err := os.UserConfigDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "potator", "onion")
}
