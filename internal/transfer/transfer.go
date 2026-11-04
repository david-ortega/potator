// Package transfer downloads files using several parallel streams.
//
// A single TCP connection is latency bound long before it saturates a link, and
// high-latency transports make that worse. Running N streams in parallel
// therefore multiplies throughput, and splitting big files into HTTP Range
// requests lets a single large file use those streams too.
package transfer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/david-ortega/potator/internal/manifest"
	"github.com/david-ortega/potator/internal/progress"
)

// copyBufferSize is used for every stream; large enough to keep the socket busy.
const copyBufferSize = 256 * 1024

// Request is a single file to download.
type Request struct {
	URL      string
	RelPath  string
	DestPath string
	Size     int64
	SHA256   string
}

// Options configures a transfer run.
type Options struct {
	// Clients are HTTP clients, one per stream group. Round-robin assigned.
	Clients []*http.Client
	// Parallel is the maximum number of concurrent streams.
	Parallel int
	// ChunkSize is the size of a Range request. Files larger than
	// 2*ChunkSize are split; smaller files use a single stream.
	ChunkSize int64
	// Progress is optional and shared by all streams.
	Progress *progress.Bar
	// Verify re-hashes every downloaded file against the manifest digest.
	Verify bool
	// Retries is the number of extra attempts per chunk.
	Retries int
	// Logf receives per-file progress messages (used when no bar is shown).
	Logf func(format string, args ...any)
}

// job is one range (or whole file) to fetch.
type job struct {
	state *reqState
	index int
	start int64
	end   int64 // inclusive, -1 for "whole file"
}

// reqState tracks a single requested file across its chunk jobs.
type reqState struct {
	req       Request
	remaining atomic.Int64
	failed    atomic.Bool

	mu      sync.Mutex
	file    *os.File
	openErr error
}

// open lazily creates (and pre-sizes) the destination file.
func (s *reqState) open() (*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		return s.file, nil
	}
	if s.openErr != nil {
		return nil, s.openErr
	}
	if err := os.MkdirAll(filepath.Dir(s.req.DestPath), 0o755); err != nil {
		s.openErr = err
		return nil, err
	}

	file, err := os.OpenFile(s.req.DestPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		s.openErr = err
		return nil, err
	}
	if s.req.Size > 0 {
		if err := file.Truncate(s.req.Size); err != nil {
			file.Close()
			s.openErr = err
			return nil, err
		}
	}
	s.file = file
	return file, nil
}

// close releases the destination file handle.
func (s *reqState) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		s.file.Close()
		s.file = nil
	}
}

// Run downloads every request, spreading chunks over the available streams.
func Run(ctx context.Context, opts Options, requests []Request) error {
	if len(requests) == 0 {
		return nil
	}
	if len(opts.Clients) == 0 {
		return fmt.Errorf("transfer: no HTTP clients configured")
	}

	parallel := opts.Parallel
	if parallel < 1 {
		parallel = 1
	}
	chunkSize := opts.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 4 << 20
	}
	retries := opts.Retries
	if retries < 0 {
		retries = 0
	}

	states := make([]*reqState, len(requests))
	var jobs []job

	for i, req := range requests {
		state := &reqState{req: req}
		states[i] = state

		first := len(jobs)
		if req.Size > 2*chunkSize && parallel > 1 {
			index := 0
			for start := int64(0); start < req.Size; start += chunkSize {
				end := start + chunkSize - 1
				if end > req.Size-1 {
					end = req.Size - 1
				}
				jobs = append(jobs, job{state: state, index: index, start: start, end: end})
				index++
			}
		} else {
			end := int64(-1)
			if req.Size > 0 {
				end = req.Size - 1
			}
			jobs = append(jobs, job{state: state, index: 0, start: 0, end: end})
		}
		state.remaining.Store(int64(len(jobs) - first))
	}

	engine := &engine{opts: opts, retries: retries}

	queue := make(chan job)
	errs := make(chan error, len(requests))
	var wg sync.WaitGroup

	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffer := make([]byte, copyBufferSize)
			for j := range queue {
				if !j.state.failed.Load() {
					if err := engine.fetch(ctx, j, buffer); err != nil {
						j.state.failed.Store(true)
					}
				}
				if j.state.remaining.Add(-1) == 0 {
					err := engine.finalize(j.state)
					select {
					case errs <- err:
					default:
					}
				}
			}
		}()
	}

	for _, j := range jobs {
		select {
		case <-ctx.Done():
			close(queue)
			wg.Wait()
			return ctx.Err()
		case queue <- j:
		}
	}
	close(queue)
	wg.Wait()

	var firstErr error
	for i := 0; i < len(requests); i++ {
		select {
		case err := <-errs:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		default:
		}
	}
	return firstErr
}

// engine performs the actual HTTP work.
type engine struct {
	opts    Options
	retries int
}

// fetch downloads one range (or a whole file) and writes it at the right
// offset, retrying a few times before giving up.
func (e *engine) fetch(ctx context.Context, j job, buffer []byte) error {
	var lastErr error

	for attempt := 0; attempt <= e.retries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := e.fetchOnce(ctx, j, buffer); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// fetchOnce performs a single attempt for a job.
func (e *engine) fetchOnce(ctx context.Context, j job, buffer []byte) error {
	client := e.opts.Clients[j.index%len(e.opts.Clients)]

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, j.state.req.URL, nil)
	if err != nil {
		return err
	}
	if j.end >= 0 {
		request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", j.start, j.end))
	}

	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusOK:
		// The server ignored the Range header; only valid for a whole file.
		if j.end >= 0 && j.state.req.Size > 0 && (j.start != 0 || j.end != 0) {
			if j.start != 0 {
				return fmt.Errorf("server ignored range request for %s", j.state.req.RelPath)
			}
		}
	case http.StatusPartialContent:
		// Expected for chunked transfers.
	default:
		return fmt.Errorf("unexpected status %s for %s", response.Status, j.state.req.RelPath)
	}

	file, err := j.state.open()
	if err != nil {
		return err
	}

	if e.opts.Progress != nil {
		e.opts.Progress.Describe("Syncing " + shortName(j.state.req.RelPath))
	}

	writer := io.NewOffsetWriter(file, j.start)
	written, err := io.CopyBuffer(writer, response.Body, buffer)
	if e.opts.Progress != nil && written > 0 {
		e.opts.Progress.Add(written)
	}
	if err != nil {
		return err
	}

	// Short read: the circuit died mid-stream, so retry this chunk.
	if j.end >= 0 && written != j.end-j.start+1 {
		return fmt.Errorf("incomplete chunk for %s (%d/%d bytes)",
			j.state.req.RelPath, written, j.end-j.start+1)
	}
	return nil
}

// finalize closes the file and verifies its digest when requested.
func (e *engine) finalize(state *reqState) error {
	state.close()

	if state.failed.Load() {
		return fmt.Errorf("failed to download %s", state.req.RelPath)
	}

	if e.opts.Verify && state.req.SHA256 != "" {
		digest, err := manifest.CalculateSHA256(state.req.DestPath)
		if err != nil {
			return err
		}
		if !strings.EqualFold(digest, state.req.SHA256) {
			os.Remove(state.req.DestPath)
			return fmt.Errorf("checksum mismatch for %s", state.req.RelPath)
		}
	}

	if e.opts.Logf != nil {
		e.opts.Logf("[+] Synced: %s", state.req.RelPath)
	}
	return nil
}

// shortName trims a path so it fits on a single bar line.
func shortName(path string, max ...int) string {
	limit := 44
	if len(max) > 0 {
		limit = max[0]
	}
	if len(path) <= limit {
		return path
	}
	return "..." + path[len(path)-(limit-3):]
}
