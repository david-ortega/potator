package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/david-ortega/potator/internal/content"
	"github.com/david-ortega/potator/internal/hashcache"
	"github.com/david-ortega/potator/internal/manifest"
	"github.com/david-ortega/potator/internal/onion"
	"github.com/david-ortega/potator/internal/progress"
	"github.com/spf13/cobra"
)

// transportKind is the transport selected on the command line.
type transportKind int

const (
	// transportAuto means "Tor when it is needed": publishing always uses Tor,
	// subscribing decides from the URL.
	transportAuto transportKind = iota
	// transportOnion is a Tor v3 onion service.
	transportOnion
	// transportLan is plain HTTP on the local network, with no anonymity.
	transportLan
)

var (
	listenFlag    string
	transportFlag string
	dataDirFlag   string
	ephemeralFlag bool
	fastFlag      bool
	onionPortFlag int
)

var pubCmd = &cobra.Command{
	Use:   "pub [folder]",
	Short: "Publish a folder as a Tor onion service",
	Long: `Publish hashes a folder, writes manifest.json next to the content and serves
both through a Tor v3 onion service, so subscribers never learn your address.

Share the .onion URL it prints with "potator sub". Use --transport=lan to serve
over plain HTTP on the local network instead; that offers no anonymity and is
meant for mirrors you already trust.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if err := runPub(cmd.Context(), args[0]); err != nil {
			log.Fatalf("[-] %v", err)
		}
	},
}

// runPub generates the manifest, starts the transport and blocks until the
// context is cancelled. Everything is released through defers, so no path skips
// cleanup.
func runPub(ctx context.Context, folderPath string) error {
	absPath, err := filepath.Abs(folderPath)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("the folder does not exist: %s", absPath)
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("not a folder: %s", absPath)
	}

	transport, err := resolveTransport(transportFlag)
	if err != nil {
		return err
	}
	// Fail before hashing a huge folder if Tor cannot be started at all.
	if transport != transportLan {
		if err := onion.CheckAvailable(onionConfig()); err != nil {
			return err
		}
	}

	fmt.Println("[*] Analyzing folder and generating initial manifest...")
	cache := hashcache.OpenDefault()
	manifestBar := progress.New(-1, "Analyzing folder")
	m, err := manifest.GenerateWithOptions(absPath, manifest.Options{
		Cache: cache,
		OnProgress: func(done, total int64) {
			manifestBar.SetTotal(total)
			manifestBar.Set(done)
		},
	})
	manifestBar.Finish()
	if err != nil {
		return fmt.Errorf("error generating manifest: %w", err)
	}
	if err := cache.Save(); err != nil {
		log.Printf("[!] Could not save the hash cache: %v", err)
	}
	fmt.Printf("[+] Manifest ready: %d files, %s\n", len(m.Files), progress.Bytes(m.TotalSize()))

	manifestPath := filepath.Join(absPath, manifest.ManifestName)
	if err := m.SaveToJson(manifestPath); err != nil {
		return fmt.Errorf("error saving manifest.json: %w", err)
	}
	// manifest.json is generated per run: never leave it behind in the shared
	// folder once the publisher stops.
	defer func() {
		if err := os.Remove(manifestPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("[!] Could not delete %s: %v", manifestPath, err)
		}
	}()

	node, err := content.NewNode(absPath, &content.Config{ListenAddr: listenFlag})
	if err != nil {
		return err
	}

	if transport == transportLan {
		return serveLan(ctx, node)
	}
	return serveOnion(ctx, node)
}

// serveOnion publishes the node through a Tor v3 onion service.
func serveOnion(ctx context.Context, node *content.Node) error {
	runtime, err := onion.Publish(ctx, onionConfig())
	if err != nil {
		return err
	}
	defer runtime.Close()

	if err := node.StartOn(ctx, runtime.Listener()); err != nil {
		return err
	}
	defer node.Stop()

	fmt.Printf("\n[+] PotaTor PUB active!\n")
	fmt.Printf("[*] Serving content from: %s\n", node.Root())
	fmt.Printf("    => %s\n", runtime.Address())
	fmt.Println("[*] Share that .onion address. Press Ctrl+C to exit.")

	<-ctx.Done()
	fmt.Println("\n[+] PotaTor PUB stopped.")
	return nil
}

// serveLan serves plain HTTP on the local network. No anonymity.
func serveLan(ctx context.Context, node *content.Node) error {
	if err := node.Start(ctx); err != nil {
		return err
	}
	defer node.Stop()

	fmt.Printf("\n[+] PotaTor PUB active! (local network, no anonymity)\n")
	fmt.Printf("[*] Serving content from: %s\n", node.Root())
	fmt.Printf("    => %s\n", publishedURL(node.Addr()))
	fmt.Println("[*] Press Ctrl+C to exit.")

	<-ctx.Done()
	fmt.Println("\n[+] PotaTor PUB stopped.")
	return nil
}

// onionConfig builds the Tor settings shared by pub and sub.
func onionConfig() onion.Config {
	config := onion.Config{
		TorPath:    torPathFlag,
		RemotePort: onionPortFlag,
		Fast:       fastFlag,
	}
	// An empty data directory means ephemeral: the address changes every run.
	if !ephemeralFlag {
		config.DataDir = dataDirFlag
	}
	return config
}

// resolveTransport validates --transport.
func resolveTransport(value string) (transportKind, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return transportAuto, nil
	case "tor", "onion":
		return transportOnion, nil
	case "lan", "local", "http":
		return transportLan, nil
	default:
		return transportAuto, fmt.Errorf("invalid --transport %q (use tor or lan)", value)
	}
}

// publishedURL turns a listen address into the URL a subscriber should use.
func publishedURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	// A wildcard bind is reachable through the loopback address locally.
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func init() {
	rootCmd.AddCommand(pubCmd)
	pubCmd.Flags().StringVarP(&transportFlag, "transport", "t", "auto", "Transport: auto (Tor), tor, or lan")
	pubCmd.Flags().StringVarP(&listenFlag, "listen", "l", content.DefaultListenAddr, "Address to bind when serving on the local network")
	pubCmd.Flags().BoolVar(&ephemeralFlag, "ephemeral", false, "Use a throwaway onion address that changes on every run")
	pubCmd.Flags().BoolVar(&fastFlag, "fast", false, "Disable Tor connection padding (faster, slightly less metadata protection)")
	pubCmd.Flags().IntVar(&onionPortFlag, "onion-port", onion.DefaultRemotePort, "Port the onion service exposes to subscribers")
}
