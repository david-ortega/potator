// Package content moves a shared folder over HTTP.
//
// A Node serves a folder — manifest.json plus the files — with Range support, so
// internal/transfer can split large files across several parallel streams. A
// Client reads individual paths back and hands out the extra clients that make
// --parallel mean something.
//
// This package is deliberately about content, not anonymity: it never dials or
// listens on its own terms. internal/onion supplies the listener a publisher
// serves on and the dialer a subscriber fetches through, which is why a Tor
// onion service can front the same Node without changing anything here.
package content

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DefaultListenAddr is where a publisher listens when no address is configured.
const DefaultListenAddr = "127.0.0.1:8080"

// shutdownTimeout bounds how long Stop waits for transfers still in flight.
const shutdownTimeout = 10 * time.Second

// Config holds the configuration of a node.
type Config struct {
	// ListenAddr is the TCP address the node binds, e.g. "127.0.0.1:8080".
	ListenAddr string
}

// Node serves a folder over HTTP.
type Node struct {
	root     string
	config   Config
	server   *http.Server
	listener net.Listener
	stopOnce sync.Once
}

// NewNode prepares a node that serves contentDir.
func NewNode(contentDir string, config *Config) (*Node, error) {
	root, err := filepath.Abs(contentDir)
	if err != nil {
		return nil, fmt.Errorf("content: resolve %q: %w", contentDir, err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("content: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("content: %s is not a folder", root)
	}

	cfg := Config{ListenAddr: DefaultListenAddr}
	if config != nil && config.ListenAddr != "" {
		cfg.ListenAddr = config.ListenAddr
	}
	return &Node{root: root, config: cfg}, nil
}

// Root returns the absolute folder being served.
func (n *Node) Root() string { return n.root }

// Handler serves the node's folder. It is exposed separately so tests can mount
// it in an httptest.Server.
func (n *Node) Handler() http.Handler { return &handler{root: n.root} }

// Start binds ListenAddr and serves in the background until ctx is cancelled.
func (n *Node) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", n.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("content: listen on %s: %w", n.config.ListenAddr, err)
	}
	return n.StartOn(ctx, listener)
}

// StartOn serves on a caller-supplied listener. This is how a Tor onion service
// fronts the node: the listener accepts connections Tor has already routed, so
// no clearnet port is ever opened and the node never learns the peer's address.
func (n *Node) StartOn(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("content: nil listener")
	}

	n.listener = listener
	n.server = &http.Server{Handler: n.Handler()}

	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = n.server.Serve(listener)
	}()
	go func() {
		select {
		case <-ctx.Done():
			n.Stop()
		case <-served:
		}
	}()
	return nil
}

// Addr returns the address the node is listening on, with the real port when
// ListenAddr asked for port 0.
func (n *Node) Addr() string {
	if n.listener != nil {
		return n.listener.Addr().String()
	}
	return n.config.ListenAddr
}

// Stop shuts the node down, giving transfers in flight ten seconds to finish.
// It is safe to call more than once.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		if n.server == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = n.server.Shutdown(ctx)
	})
}

// handler maps request paths onto files below root.
type handler struct {
	root string
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	filePath, err := h.resolve(r.URL.Path)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", contentTypeFor(filePath))
	// ServeContent provides Range handling (transfer splits large files with
	// it), Content-Length and If-Modified-Since. Copying the file by hand would
	// silently break every chunked download.
	http.ServeContent(w, r, info.Name(), info.ModTime(), file)
}

// resolve turns a URL path into a file path, refusing anything that escapes
// root. Without this, a request such as /../../secret.txt would serve a file
// from outside the shared folder.
func (h *handler) resolve(urlPath string) (string, error) {
	// path.Clean collapses ".." segments and a leading "/" keeps the result
	// rooted, so "../x" becomes "/x" rather than escaping.
	relPath := strings.TrimPrefix(path.Clean("/"+urlPath), "/")
	if relPath == "" {
		relPath = "index.html"
	}

	fullPath := filepath.Join(h.root, filepath.FromSlash(relPath))

	absRoot, err := filepath.Abs(h.root)
	if err != nil {
		return "", fmt.Errorf("content: resolve root: %w", err)
	}
	absFullPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", fmt.Errorf("content: resolve path: %w", err)
	}

	relToRoot, err := filepath.Rel(absRoot, absFullPath)
	if err != nil || relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("content: %q is outside the shared folder", urlPath)
	}
	return absFullPath, nil
}

// contentTypeFor guesses a MIME type from the file extension.
func contentTypeFor(filePath string) string {
	if contentType := mime.TypeByExtension(strings.ToLower(filepath.Ext(filePath))); contentType != "" {
		return contentType
	}
	return "application/octet-stream"
}

// DialerFunc opens one connection. Callers that fetch over an anonymity network
// pass a per-stream dialer so that each stream leaves through its own circuit.
type DialerFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// DialerFactory builds the dialer for one stream index.
type DialerFactory func(stream int) (DialerFunc, error)

// Client fetches paths from a Node.
type Client struct {
	baseURL string
	dial    DialerFactory
	http    *http.Client
}

// NewClient accepts "http://host:port", "host:port" or a bare host name and
// normalises it into a base URL. Traffic uses the operating system's network
// stack, so this constructor offers no anonymity.
func NewClient(baseAddr string) (*Client, error) {
	return newClient(baseAddr, nil)
}

// NewClientWithDialer fetches through dial instead. The resulting transport
// never consults HTTP_PROXY: a proxy configured for the machine must not be able
// to carry this traffic outside the anonymity network.
func NewClientWithDialer(baseAddr string, dial DialerFactory) (*Client, error) {
	if dial == nil {
		return nil, errors.New("content: nil dialer factory")
	}
	return newClient(baseAddr, dial)
}

func newClient(baseAddr string, dial DialerFactory) (*Client, error) {
	baseURL, err := normalizeBaseURL(baseAddr)
	if err != nil {
		return nil, err
	}

	client := &Client{baseURL: baseURL, dial: dial}
	primary, err := client.stream(0)
	if err != nil {
		return nil, err
	}
	client.http = primary
	return client, nil
}

// BaseURL returns the normalised remote address, without a trailing slash.
func (c *Client) BaseURL() string { return c.baseURL }

// URL builds the absolute URL of relPath inside the remote folder.
func (c *Client) URL(relPath string) string {
	cleaned := strings.TrimPrefix(path.Clean("/"+filepath.ToSlash(relPath)), "/")
	return c.baseURL + "/" + cleaned
}

// Get fetches one path from the remote folder. The caller owns the response
// body and must close it.
func (c *Client) Get(ctx context.Context, relPath string) (*http.Response, error) {
	requestURL := c.URL(relPath)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("content: build request %s: %w", requestURL, err)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("content: GET %s: %w", requestURL, err)
	}
	return response, nil
}

// Streams returns n independent HTTP clients for the same remote. Each owns its
// own transport, so parallel transfers do not queue behind a single connection
// pool; with a dialer factory, stream i uses the dialer built for i (on Tor each
// one is a separate circuit). This is the knob behind "sub --parallel".
func (c *Client) Streams(n int) ([]*http.Client, error) {
	if n < 1 {
		n = 1
	}
	clients := make([]*http.Client, 0, n)
	for i := 0; i < n; i++ {
		client, err := c.stream(i)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	return clients, nil
}

// stream builds the client used by one transfer stream.
func (c *Client) stream(index int) (*http.Client, error) {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: time.Second,
		// Content is hashed byte for byte, so keep encoding transparent.
		DisableCompression: true,
	}

	if c.dial == nil {
		transport.Proxy = http.ProxyFromEnvironment
	} else {
		dialer, err := c.dial(index)
		if err != nil {
			return nil, err
		}
		transport.DialContext = dialer
		// Never inherit HTTP_PROXY here: a machine-wide proxy would carry the
		// request outside the anonymity network.
		transport.Proxy = nil
	}

	// No overall timeout: downloads are bounded by the request context, because
	// a large file over a slow link legitimately takes minutes.
	return &http.Client{Transport: transport}, nil
}

// normalizeBaseURL adds the scheme when it is missing and strips a trailing
// slash, so URL() can simply append "/<path>".
func normalizeBaseURL(baseAddr string) (string, error) {
	text := strings.TrimSpace(baseAddr)
	if text == "" {
		return "", errors.New("content: empty address")
	}
	if !strings.Contains(text, "://") {
		text = "http://" + text
	}

	parsed, err := url.Parse(text)
	if err != nil {
		return "", fmt.Errorf("content: invalid address %q: %w", baseAddr, err)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("content: address %q has no host", baseAddr)
	}

	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
