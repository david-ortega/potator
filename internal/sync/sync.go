// Package sync compares a remote manifest with the local folder.
package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/david-ortega/potator/internal/hashcache"
	"github.com/david-ortega/potator/internal/manifest"
)

// DiffAction is the action to take for a single file.
type DiffAction string

const (
	ActionDownload DiffAction = "DOWNLOAD" // Create or update
	ActionSkip     DiffAction = "SKIP"     // Already present and identical
)

// FileTask describes what to do with one file.
type FileTask struct {
	RelPath string
	Action  DiffAction
	Reason  string
	Size    int64
	SHA256  string
}

// Options tunes the manifest comparison.
type Options struct {
	// Jobs is the number of files hashed concurrently. Zero uses GOMAXPROCS.
	Jobs int
	// Cache avoids re-hashing files that did not change since the last run.
	Cache *hashcache.Cache
	// OnProgress reports the number of files examined and the total.
	OnProgress func(done, total int64)
}

// compareItem is a remote file that also exists locally, so it must be hashed
// before a decision can be made.
type compareItem struct {
	relPath string
	absPath string
	size    int64
	modTime time.Time
	remote  manifest.FileInfo
}

// CompareManifests compares the remote manifest with the current state of the
// local folder.
func CompareManifests(remoteManifest *manifest.Manifest, localDir string, overwriteMode string) ([]FileTask, error) {
	return CompareManifestsWithOptions(remoteManifest, localDir, overwriteMode, Options{})
}

// CompareManifestsWithProgress is like CompareManifests but reports progress.
func CompareManifestsWithProgress(
	remoteManifest *manifest.Manifest,
	localDir string,
	overwriteMode string,
	onProgress func(done, total int64),
) ([]FileTask, error) {
	return CompareManifestsWithOptions(remoteManifest, localDir, overwriteMode, Options{OnProgress: onProgress})
}

// CompareManifestsWithOptions compares the remote manifest with the local
// folder, hashing existing local files in parallel and reusing cached digests.
func CompareManifestsWithOptions(
	remoteManifest *manifest.Manifest,
	localDir string,
	overwriteMode string,
	opts Options,
) ([]FileTask, error) {
	total := int64(len(remoteManifest.Files))
	var examined atomic.Int64

	var (
		tasks  []FileTask
		toHash []*compareItem
	)

	// First pass: only stat calls, no hashing yet.
	for relPath, remoteInfo := range remoteManifest.Files {
		if opts.OnProgress != nil {
			opts.OnProgress(examined.Add(1), total)
		}

		localFullPath := filepath.Join(localDir, relPath)
		localStat, err := os.Stat(localFullPath)
		if os.IsNotExist(err) {
			tasks = append(tasks, downloadTask(relPath, remoteInfo, "New file"))
			continue
		} else if err != nil {
			return nil, err
		}
		if localStat.IsDir() {
			continue
		}

		// A size mismatch settles it without reading the file at all.
		if localStat.Size() != remoteInfo.Size {
			tasks = append(tasks, downloadTask(relPath, remoteInfo, "Size changed"))
			continue
		}

		item := &compareItem{
			relPath: relPath,
			absPath: localFullPath,
			size:    localStat.Size(),
			modTime: localStat.ModTime(),
			remote:  remoteInfo,
		}

		if sha, ok := opts.Cache.Lookup(item.absPath, item.size, item.modTime); ok {
			tasks = append(tasks, decide(item, sha, overwriteMode))
			continue
		}
		toHash = append(toHash, item)
	}

	// Second pass: hash the remaining candidates in parallel.
	hashes := make([]string, len(toHash))
	jobs := opts.Jobs
	if jobs <= 0 {
		jobs = runtime.GOMAXPROCS(0)
	}
	if jobs > len(toHash) {
		jobs = len(toHash)
	}

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)

	indexes := make(chan int)
	for w := 0; w < jobs; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range indexes {
				sha, err := manifest.CalculateSHA256(toHash[i].absPath)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				hashes[i] = sha
			}
		}()
	}
	for i := range toHash {
		indexes <- i
	}
	close(indexes)
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	for i, item := range toHash {
		sha := hashes[i]
		opts.Cache.Store(item.absPath, item.size, item.modTime, sha)
		tasks = append(tasks, decide(item, sha, overwriteMode))
	}

	return tasks, nil
}

// downloadTask builds a task that will fetch the file from the publisher.
func downloadTask(relPath string, remote manifest.FileInfo, reason string) FileTask {
	return FileTask{
		RelPath: relPath,
		Action:  ActionDownload,
		Reason:  reason,
		Size:    remote.Size,
		SHA256:  remote.SHA256,
	}
}

// decide turns a hashed comparison into a task, applying the overwrite policy.
func decide(item *compareItem, localHash string, overwriteMode string) FileTask {
	if localHash == item.remote.SHA256 {
		return FileTask{
			RelPath: item.relPath,
			Action:  ActionSkip,
			Reason:  "Identical",
			Size:    item.remote.Size,
			SHA256:  item.remote.SHA256,
		}
	}

	switch overwriteMode {
	case "skip":
		return FileTask{
			RelPath: item.relPath,
			Action:  ActionSkip,
			Reason:  "Modified locally (skipped by policy)",
			Size:    item.remote.Size,
			SHA256:  item.remote.SHA256,
		}
	case "prompt":
		fmt.Printf("[?] %s changed locally. Overwrite? (y/n): ", item.relPath)
		var answer string
		fmt.Scanln(&answer)
		if answer == "y" || answer == "Y" {
			return FileTask{
				RelPath: item.relPath,
				Action:  ActionDownload,
				Reason:  "Modified locally (approved by user)",
				Size:    item.remote.Size,
				SHA256:  item.remote.SHA256,
			}
		}
		return FileTask{
			RelPath: item.relPath,
			Action:  ActionSkip,
			Reason:  "Modified locally (rejected by user)",
			Size:    item.remote.Size,
			SHA256:  item.remote.SHA256,
		}
	default:
		return FileTask{
			RelPath: item.relPath,
			Action:  ActionDownload,
			Reason:  "Modified locally (overwrite)",
			Size:    item.remote.Size,
			SHA256:  item.remote.SHA256,
		}
	}
}
