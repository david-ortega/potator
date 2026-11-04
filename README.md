# PotaTor

[![ci](https://github.com/david-ortega/potator/actions/workflows/ci.yml/badge.svg)](https://github.com/david-ortega/potator/actions/workflows/ci.yml)
[![build](https://github.com/david-ortega/potator/actions/workflows/build.yml/badge.svg)](https://github.com/david-ortega/potator/actions/workflows/build.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Share and sync big folders — game builds, mod packs, datasets — over Tor, so the
person downloading never learns where you are and you never learn where they are.

```
potator pub ~/mods          → http://<56 chars>.onion
potator sub <that URL> ~/mods-copy
```

Only new and changed files travel: PotaTor compares a SHA256 manifest on both
sides, so a 40 GB folder with one new file transfers one file.

## How it works

```
publisher                                     subscriber
┌───────────────────────────┐                 ┌───────────────────────────┐
│ pub                       │                 │ sub                       │
│  hash folder → manifest   │                 │  fetch manifest.json      │
│  serve over HTTP          │                 │  diff against local copy  │
│  wrapped in Tor onion  ◄──┼── rendezvous ───┼─► parallel Range streams │
└───────────────────────────┘                 │  verify SHA256 per file   │
                                              └───────────────────────────┘
```

- **Manifest** — one SHA256 digest and size per file, written next to the
  content for the duration of the run and deleted on shutdown.
- **Diff** — files are compared by size first, then by digest; a local digest
  cache means unchanged files are never re-hashed.
- **Transfer** — large files are split into HTTP Range requests across N
  parallel streams. On Tor each stream uses its own SOCKS credentials, so Tor's
  `IsolateSOCKSAuth` gives it its own circuit: `--parallel` multiplies
  throughput instead of queueing behind one circuit.
- **Integrity** — every downloaded file is re-hashed and rejected on mismatch.

## Requirements

- Go 1.27 or newer to build.
- **Tor**, either on `PATH` or passed with `--tor-path`. PotaTor starts its own
  Tor process and does not need a running daemon, but it does need the binary.

Fetching from a `.onion` URL without Tor is refused rather than silently sent
over the clearnet.

## Install

```bash
# with a Go toolchain (requires Tor on PATH or --tor-path at run time)
go install github.com/david-ortega/potator@latest

# or from a checkout
go build -o potator .
```

Prebuilt binaries are not published; `go build` produces a single self-contained
executable that needs a Tor binary available at run time.

## Usage

```bash
# Publish a folder; prints the .onion address to share
potator pub ./my-mods

# Sync it somewhere else
potator sub http://<id>.onion ./my-mods-copy

# Keep a copy up to date
potator sub http://<id>.onion ./my-mods-copy --watch --interval 15m

# Serve on your LAN instead, with no anonymity
potator pub ./my-mods --transport=lan --listen 0.0.0.0:8080
```

### Flags

| Flag | Command | Description |
|------|---------|-------------|
| `-t, --transport` | both | `auto` (Tor; decided from the URL for `sub`), `tor`, or `lan` |
| `--tor-path` | both | Path to the `tor` executable |
| `--data-dir` | both | Where Tor state and the onion service key live |
| `-l, --listen` | `pub` | Address to bind for `--transport=lan` (default `127.0.0.1:8080`) |
| `--ephemeral` | `pub` | Throwaway onion address, different on every run |
| `--onion-port` | `pub` | Port the onion service exposes (default `80`) |
| `--fast` | `pub` | Disable Tor connection padding (faster, slightly less metadata protection) |
| `-w, --watch` | `sub` | Poll for updates in a loop |
| `-i, --interval` | `sub` | Poll interval (`30s`, `10m`, `1h`; default `15m`) |
| `-o, --overwrite` | `sub` | Policy for locally modified files: `always`, `skip`, `prompt` |
| `-j, --parallel` | `sub` | Concurrent transfer streams (default `4`) |
| `--chunk-size` | `sub` | Range size used to split large files (default `4MiB`) |
| `--no-verify` | `sub` | Skip the SHA256 check of downloaded files |

The onion address is stable across runs because the service key is stored in
`--data-dir` (`~/.config/potator/onion` on Linux, `%AppData%\potator\onion` on
Windows). Pass `--ephemeral` to avoid linking separate sessions to one address.

## Security model

**What Tor gives you here:** the publisher's address is hidden behind a
rendezvous point, the subscriber's address is hidden from the publisher, and
neither side opens a clearnet port. The onion address is an unguessable
capability: without it there is nothing to connect to.

**What it does not give you:**

- **The publisher sees which files you fetch, and when.** The manifest also
  lists every filename and size, so share the address only with people you would
  share the file list with.
- **A global passive adversary is out of scope.** Watching the whole internet
  can still correlate a large transfer; defeating that needs mixnets, which are
  orders of magnitude slower.
- **Deleted files are not deleted on subscribers.** Removals are not propagated.
- **`--watch` polling is a traffic fingerprint.** Use manual runs or a jittered
  interval if that matters to you.
- **`--transport=lan` is not anonymous.** It is plain HTTP on the local network.

Hidden files and folders inside the shared tree are never published, so
`.git/config` and `.ssh/id_rsa` stay private, and requests cannot escape the
shared folder (`/../../secret` is rejected).

## Performance

Onion services sustain roughly **3.8 MB/s median** (against ~8.6 MB/s for a
public site over Tor, per Tor Metrics), so bulk sync is practical but not free.

- Raise `--parallel` to 8 before doing anything else; it is the main lever.
- Re-runs are nearly free: the digest cache means unchanged files cost no
  network and almost no CPU.
- `--chunk-size 16MiB` reduces request overhead on very large files.

## Development

```
main.go                  entry point
cmd/                     cobra commands: root, pub, sub
internal/onion/          Tor runtime: onion service + per-stream SOCKS dialers
internal/content/         HTTP content layer: node, client, Range support
internal/manifest/       SHA256 manifest generation and JSON form
internal/sync/           manifest comparison and overwrite policies
internal/transfer/       parallel Range downloads with checksum verification
internal/hashcache/      digest cache keyed by path, size and mtime
internal/progress/       dependency-free single-line progress bar
```

```bash
go build ./...
go vet ./...
go test ./...        # no Tor needed: the onion tests run against a fake SOCKS server
```

`internal/onion` is the only package that knows Tor exists. Adding or swapping a
transport means implementing `Publish`/`Connect` there; the manifest, diff and
transfer layers do not change.

Pull requests run `gofmt`, `go vet`, the build and the full test suite on Linux,
macOS and Windows.

## License

MIT — see [LICENSE](LICENSE).
