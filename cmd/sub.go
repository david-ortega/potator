package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/david-ortega/potator/internal/content"
	"github.com/david-ortega/potator/internal/hashcache"
	"github.com/david-ortega/potator/internal/manifest"
	"github.com/david-ortega/potator/internal/onion"
	"github.com/david-ortega/potator/internal/progress"
	"github.com/david-ortega/potator/internal/sync"
	"github.com/david-ortega/potator/internal/transfer"
	"github.com/spf13/cobra"
)

var (
	watchFlag     bool
	intervalFlag  string
	overwriteFlag string
	parallelFlag  int
	chunkSizeFlag string
	noVerifyFlag  bool
)

var subCmd = &cobra.Command{
	Use:   "sub [url] [dest-dir]",
	Short: "Sync a folder from a PotaTor publisher",
	Long: `Sync a folder from a publisher. An http://<id>.onion URL is fetched through
Tor, which also hides your address from the publisher; a plain http:// URL is
fetched directly with no anonymity.`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		if err := runSub(cmd.Context(), args[0], args[1]); err != nil {
			log.Fatalf("[-] %v", err)
		}
	},
}

// runSub performs the initial sync and, with --watch, keeps polling until the
// context is cancelled.
func runSub(ctx context.Context, remoteAddr, destDir string) error {
	switch overwriteFlag {
	case "always", "skip", "prompt":
	default:
		return fmt.Errorf("invalid --overwrite %q (use always, skip or prompt)", overwriteFlag)
	}

	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("could not create destination directory: %w", err)
	}

	client, closeClient, err := openClient(ctx, remoteAddr)
	if err != nil {
		return err
	}
	defer closeClient()

	chunkSize, err := parseSize(chunkSizeFlag)
	if err != nil {
		return fmt.Errorf("invalid chunk size: %w", err)
	}

	// A single cache holds both the digests of local files and the digests of
	// what this run downloads. It is flushed once, on exit.
	cache := hashcache.OpenDefault()
	defer func() {
		if err := cache.Save(); err != nil {
			log.Printf("[!] Could not save the hash cache: %v", err)
		}
	}()

	fmt.Printf("\n[+] PotaTor SUB active!\n")
	fmt.Printf("[*] Fetching from: %s\n", client.BaseURL())

	cycle := func(ctx context.Context) error {
		return syncCycle(ctx, client, destDir, cache, chunkSize)
	}

	if err := cycle(ctx); err != nil {
		if ctx.Err() != nil {
			fmt.Println("\n[+] PotaTor SUB stopped.")
			return nil
		}
		log.Printf("[-] %v", err)
	}

	if !watchFlag {
		return nil
	}

	interval, err := time.ParseDuration(intervalFlag)
	if err != nil || interval <= 0 {
		return fmt.Errorf("invalid --interval %q", intervalFlag)
	}

	fmt.Printf("[*] Watch mode activated. Checking for updates every %v...\n", interval)
	fmt.Println("[*] Press Ctrl+C to stop.")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n[+] PotaTor SUB stopped.")
			return nil
		case <-ticker.C:
			if err := cycle(ctx); err != nil && ctx.Err() == nil {
				log.Printf("[-] %v", err)
			}
		}
	}
}

// openClient builds the HTTP client for the remote. Onion addresses are always
// fetched through Tor, and are refused when the lan transport would leak them.
// The returned func releases any Tor instance that was started.
func openClient(ctx context.Context, remoteAddr string) (*content.Client, func(), error) {
	kind, err := resolveTransport(transportFlag)
	if err != nil {
		return nil, nil, err
	}

	if !onion.IsOnionURL(remoteAddr) {
		if kind == transportOnion {
			return nil, nil, errors.New("--transport=tor needs a .onion address")
		}
		client, err := content.NewClient(remoteAddr)
		if err != nil {
			return nil, nil, err
		}
		return client, func() {}, nil
	}

	if kind == transportLan {
		return nil, nil, errors.New("refusing to fetch a .onion address with --transport=lan: that would expose your IP")
	}

	runtime, err := onion.Connect(ctx, onionConfig())
	if err != nil {
		return nil, nil, err
	}
	client, err := content.NewClientWithDialer(remoteAddr, func(stream int) (content.DialerFunc, error) {
		return runtime.Dialer(stream)
	})
	if err != nil {
		runtime.Close()
		return nil, nil, err
	}
	return client, func() { runtime.Close() }, nil
}

// syncCycle fetches the remote manifest, compares it with the local folder and
// downloads whatever is missing or out of date.
func syncCycle(ctx context.Context, client *content.Client, destDir string, cache *hashcache.Cache, chunkSize int64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	fmt.Printf("[%s] [*] Fetching remote manifest...\n", time.Now().Format("15:04:05"))
	remoteManifest, err := fetchRemoteManifest(ctx, client)
	if err != nil {
		return err
	}

	fmt.Println("[*] Checking local state...")
	checkBar := progress.New(int64(len(remoteManifest.Files)), "Checking local files")
	tasks, err := sync.CompareManifestsWithOptions(remoteManifest, destDir, overwriteFlag, sync.Options{
		Cache: cache,
		OnProgress: func(done, total int64) {
			checkBar.SetTotal(total)
			checkBar.Set(done)
		},
	})
	checkBar.Finish()
	if err != nil {
		return fmt.Errorf("error comparing local files: %w", err)
	}

	// Everything the comparison marked for download becomes a transfer request.
	type pending struct {
		request transfer.Request
		reason  string
	}
	var (
		queue     []pending
		totalSize int64
	)
	for _, task := range tasks {
		if task.Action != sync.ActionDownload {
			continue
		}
		queue = append(queue, pending{
			request: transfer.Request{
				URL:      client.URL(task.RelPath),
				RelPath:  task.RelPath,
				DestPath: filepath.Join(destDir, filepath.FromSlash(task.RelPath)),
				Size:     task.Size,
				SHA256:   task.SHA256,
			},
			reason: task.Reason,
		})
		totalSize += task.Size
	}

	if len(queue) == 0 {
		fmt.Println("[+] Up to date! No new changes.")
		return nil
	}

	bar := progress.New(totalSize, "Syncing")
	bar.SetTotal(totalSize)
	// Without a terminal there is no bar, so announce every file instead.
	if !bar.Enabled() {
		for _, item := range queue {
			fmt.Printf("[+] Syncing: %s (%s)\n", item.request.RelPath, item.reason)
		}
	}

	requests := make([]transfer.Request, len(queue))
	for i, item := range queue {
		requests[i] = item.request
	}

	streams, err := client.Streams(parallelFlag)
	if err != nil {
		bar.Finish()
		return fmt.Errorf("prepare download streams: %w", err)
	}

	err = transfer.Run(ctx, transfer.Options{
		Clients:   streams,
		Parallel:  len(streams),
		ChunkSize: chunkSize,
		Progress:  bar,
		Verify:    !noVerifyFlag,
		Retries:   2,
	}, requests)
	bar.Finish()
	if err != nil {
		return fmt.Errorf("transfer error: %w", err)
	}

	fmt.Printf("[+] Synced %d file(s), %s.\n", len(requests), progress.Bytes(totalSize))
	return nil
}

// fetchRemoteManifest downloads and decodes the publisher's manifest.json.
func fetchRemoteManifest(ctx context.Context, client *content.Client) (*manifest.Manifest, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	response, err := client.Get(ctx, manifest.ManifestName)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Publisher: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the Publisher responded with %s", response.Status)
	}

	bar := progress.New(-1, "Fetching manifest")
	decoder := json.NewDecoder(io.TeeReader(response.Body, bar.Writer()))
	var remote manifest.Manifest
	err = decoder.Decode(&remote)
	bar.Finish()
	if err != nil {
		return nil, fmt.Errorf("error parsing the JSON manifest: %w", err)
	}
	if remote.Files == nil {
		remote.Files = map[string]manifest.FileInfo{}
	}
	return &remote, nil
}

func init() {
	rootCmd.AddCommand(subCmd)
	subCmd.Flags().StringVarP(&transportFlag, "transport", "t", "auto", "Transport: auto (Tor for .onion URLs), tor, or lan")
	subCmd.Flags().BoolVarP(&watchFlag, "watch", "w", false, "Run in background checking for updates")
	subCmd.Flags().StringVarP(&intervalFlag, "interval", "i", "15m", "Check interval in watch mode (e.g.: 30s, 10m, 1h)")
	subCmd.Flags().StringVarP(&overwriteFlag, "overwrite", "o", "always", "Overwrite policy for locally modified files (always, skip, prompt)")
	subCmd.Flags().IntVarP(&parallelFlag, "parallel", "j", 4, "Concurrent download streams")
	subCmd.Flags().StringVar(&chunkSizeFlag, "chunk-size", "4MiB", "Range size used to split large files (e.g. 1MiB, 4MiB, 16MiB)")
	subCmd.Flags().BoolVar(&noVerifyFlag, "no-verify", false, "Skip the SHA256 check of downloaded files")
}

// parseSize converts "4MiB", "512kb" or "1024" into a byte count.
func parseSize(value string) (int64, error) {
	text := value

	multipliers := []struct {
		suffix string
		factor int64
	}{
		{"gib", 1 << 30}, {"gb", 1 << 30}, {"g", 1 << 30},
		{"mib", 1 << 20}, {"mb", 1 << 20}, {"m", 1 << 20},
		{"kib", 1 << 10}, {"kb", 1 << 10}, {"k", 1 << 10},
		{"b", 1},
	}

	factor := int64(1)
	lowered := strings.ToLower(text)
	for _, candidate := range multipliers {
		if strings.HasSuffix(lowered, candidate.suffix) {
			factor = candidate.factor
			text = text[:len(text)-len(candidate.suffix)]
			break
		}
	}

	number, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", value)
	}
	if number <= 0 {
		return 0, fmt.Errorf("size must be positive: %q", value)
	}
	return int64(number * float64(factor)), nil
}
