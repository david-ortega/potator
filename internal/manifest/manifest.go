package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/david-ortega/potator/internal/hashcache"
)

// FileInfo holds the metadata of a single file.
type FileInfo struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest is the root structure of manifest.json.
type Manifest struct {
	Version   int                 `json:"version"`
	Timestamp int64               `json:"timestamp"`
	Files     map[string]FileInfo `json:"files"`
}

// ManifestName is the generated file, skipped while scanning so a previous run
// never ends up hashing its own output.
const ManifestName = "manifest.json"

// hashBufferSize is the read size used while hashing. 1 MiB amortises the
// syscall overhead and is large enough for SHA-NI to run at full speed.
const hashBufferSize = 1 << 20

// Options tunes manifest generation.
type Options struct {
	// Jobs is the number of files hashed concurrently. Zero uses GOMAXPROCS.
	Jobs int
	// Cache, when set, avoids re-hashing unchanged files.
	Cache *hashcache.Cache
	// OnProgress is called with the bytes hashed so far and the total. Calls are
	// serialized and the reported values only grow, so the callback neither has
	// to be safe for concurrent use nor has to handle values going backwards.
	OnProgress func(done, total int64)
}

// entry is a file discovered during the scan phase.
type entry struct {
	absPath string
	relPath string
	size    int64
	modTime time.Time
}

// Generate walks rootDir recursively and builds the manifest.
func Generate(rootDir string) (*Manifest, error) {
	return GenerateWithOptions(rootDir, Options{})
}

// GenerateWithProgress is like Generate but reports hashing progress.
func GenerateWithProgress(rootDir string, onProgress func(done, total int64)) (*Manifest, error) {
	return GenerateWithOptions(rootDir, Options{OnProgress: onProgress})
}

// GenerateWithOptions walks rootDir, hashing files in parallel and reusing
// cached digests for files that did not change.
func GenerateWithOptions(rootDir string, opts Options) (*Manifest, error) {
	m := &Manifest{
		Version:   1,
		Timestamp: time.Now().Unix(),
		Files:     make(map[string]FileInfo),
	}

	// First pass: discover files and know the total size up front, so the
	// progress bar can show a percentage and an ETA while hashing.
	entries, totalBytes, err := scan(rootDir)
	if err != nil {
		return nil, err
	}

	hashes := make([]string, len(entries))
	var hashed atomic.Int64

	// One mutex serialises every progress callback. Without it the hashing
	// workers call OnProgress concurrently, which races with any callback that
	// is not thread-safe -- the first version of this shipped that bug and the
	// race detector caught it in CI. Reporting under the lock also keeps the
	// byte counts monotonic.
	var progressMu sync.Mutex
	report := func(delta int64) {
		if opts.OnProgress == nil {
			return
		}
		progressMu.Lock()
		defer progressMu.Unlock()
		opts.OnProgress(hashed.Add(delta), totalBytes)
	}

	jobs := opts.Jobs
	if jobs <= 0 {
		jobs = runtime.GOMAXPROCS(0)
	}
	if jobs > len(entries) {
		jobs = len(entries)
	}

	// Buffers are pooled: one per worker instead of one per file.
	buffers := sync.Pool{New: func() any {
		buffer := make([]byte, hashBufferSize)
		return &buffer
	}}

	indexes := make(chan int)
	errs := make(chan error, 1)
	var wg sync.WaitGroup

	for w := 0; w < jobs; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bufferPtr := buffers.Get().(*[]byte)
			defer buffers.Put(bufferPtr)

			for i := range indexes {
				e := entries[i]

				// Reuse a cached digest when the file is untouched.
				if sha, ok := opts.Cache.Lookup(e.absPath, e.size, e.modTime); ok {
					hashes[i] = sha
					report(e.size)
					continue
				}

				sha, err := hashFileBuffered(e.absPath, *bufferPtr, report)
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				hashes[i] = sha
				opts.Cache.Store(e.absPath, e.size, e.modTime, sha)
			}
		}()
	}

	var sendErr error
	for i := range entries {
		select {
		case <-errs:
			sendErr = errors.New("hashing aborted")
		case indexes <- i:
		}
		if sendErr != nil {
			break
		}
	}
	close(indexes)
	wg.Wait()

	select {
	case err := <-errs:
		return nil, err
	default:
	}

	for i, e := range entries {
		m.Files[e.relPath] = FileInfo{Size: e.size, SHA256: hashes[i]}
	}

	if opts.OnProgress != nil {
		opts.OnProgress(totalBytes, totalBytes)
	}
	return m, nil
}

// scan lists every file below rootDir together with its size and mod time.
func scan(rootDir string) ([]entry, int64, error) {
	var (
		entries []entry
		total   int64
	)

	err := filepath.WalkDir(rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Directories are skipped, they end up as path prefixes.
		if d.IsDir() {
			// Hidden folders (.git, .ssh, .config) are skipped entirely. Testing
			// only the file name would still publish .git/config and .ssh keys,
			// because their own names are not dotted.
			if path != rootDir && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}

		// Ignore hidden files.
		if strings.HasPrefix(d.Name(), ".") {
			return nil
		}

		relPath, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}

		// Normalize separators to Unix style.
		relPath = filepath.ToSlash(relPath)
		if relPath == ManifestName {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		entries = append(entries, entry{
			absPath: path,
			relPath: relPath,
			size:    info.Size(),
			modTime: info.ModTime(),
		})
		total += info.Size()
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return entries, total, nil
}

// CalculateSHA256 hashes a file using a pooled 1 MiB buffer, so memory use
// stays flat no matter how large the file is.
func CalculateSHA256(filePath string) (string, error) {
	return hashFileBuffered(filePath, make([]byte, hashBufferSize), nil)
}

// HashReader ranges over r and returns its SHA256 digest.
func HashReader(r io.Reader) (string, error) {
	hash := sha256.New()
	if _, err := io.CopyBuffer(hash, r, make([]byte, hashBufferSize)); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// hashFileBuffered hashes a file using the supplied buffer and reports the
// number of bytes consumed after every read, which drives the progress bar.
func hashFileBuffered(filePath string, buffer []byte, onChunk func(int64)) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	for {
		n, err := file.Read(buffer)
		if n > 0 {
			if _, writeErr := hash.Write(buffer[:n]); writeErr != nil {
				return "", writeErr
			}
			if onChunk != nil {
				onChunk(int64(n))
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// SaveToJson serializes the manifest to a JSON file on disk.
func (m *Manifest) SaveToJson(outputPath string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, data, 0644)
}

// TotalSize returns the sum of the sizes of every file in the manifest.
func (m *Manifest) TotalSize() int64 {
	var total int64
	for _, file := range m.Files {
		total += file.Size
	}
	return total
}
