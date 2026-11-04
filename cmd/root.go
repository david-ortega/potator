package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/david-ortega/potator/internal/onion"
	"github.com/spf13/cobra"
)

var (
	// torPathFlag points at the tor executable when it is not on PATH.
	torPathFlag string
)

// version is stamped at build time by the release workflow:
//
//	go build -ldflags "-X github.com/david-ortega/potator/cmd.version=v1.2.3"
var version = "dev"

var rootCmd = &cobra.Command{
	Use:     "potator",
	Version: version,
	Short:   "PotaTor: share and sync folders over Tor without re-sending unchanged bytes.",
	Long: `PotaTor publishes a folder as a Tor v3 onion service and syncs it to another
machine using an incremental SHA256 manifest, so only new or changed files are
transferred and neither side learns the other's address.`,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&torPathFlag, "tor-path", "", "Path to the tor executable (default: tor on PATH)")
	rootCmd.PersistentFlags().StringVar(&dataDirFlag, "data-dir", onion.DefaultDataDir(), "Where Tor state and the onion service key are kept")
}

// Execute runs the CLI. Ctrl+C (or SIGTERM) cancels the root context so
// commands can shut down cleanly: in-flight transfers get ten seconds to
// finish, the onion service is closed and the generated manifest.json is
// deleted.
func Execute() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// Restore default behaviour (immediate exit) after the first signal, so a
	// second Ctrl+C always works even if a shutdown hangs.
	defer stop()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
