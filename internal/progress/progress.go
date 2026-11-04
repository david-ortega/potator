// Package progress renders a lightweight single-line progress bar.
//
// It has no external dependencies and degrades gracefully: when the output is
// not a terminal (piped or redirected) nothing is printed at all, so logs stay
// clean in scripts and CI.
package progress

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	barWidth     = 26
	renderPeriod = 100 * time.Millisecond
	spinnerChars = `|/-\`
)

// Bar is a concurrency-safe progress bar. A negative total means "unknown",
// in which case the bar shows a spinner and the amount processed so far.
type Bar struct {
	mu       sync.Mutex
	out      io.Writer
	desc     string
	total    int64
	current  int64
	start    time.Time
	lastDraw time.Time
	lastLen  int
	spins    int
	enabled  bool
	finished bool
}

// New creates a bar that writes to standard output.
func New(total int64, desc string) *Bar {
	return NewWriter(os.Stdout, total, desc)
}

// NewWriter creates a bar that writes to out.
func NewWriter(out io.Writer, total int64, desc string) *Bar {
	return &Bar{
		out:     out,
		desc:    desc,
		total:   total,
		start:   time.Now(),
		enabled: isTerminal(out),
	}
}

// isTerminal reports whether w is an interactive character device.
func isTerminal(w io.Writer) bool {
	file, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// Enabled reports whether the bar will actually draw anything.
func (b *Bar) Enabled() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.enabled && !b.finished
}

// Describe replaces the label shown at the start of the line.
func (b *Bar) Describe(desc string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.desc = desc
	b.draw(true)
}

// SetTotal sets the total amount of work, revealing a percentage and ETA.
func (b *Bar) SetTotal(total int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.total = total
	b.draw(false)
}

// Set replaces the amount of work done so far.
func (b *Bar) Set(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.current = n
	b.draw(false)
}

// Add increments the amount of work done so far.
func (b *Bar) Add(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.current += n
	b.draw(false)
}

// Writer returns an io.Writer that advances the bar by however many bytes are
// written to it. Handy with io.Copy and io.MultiWriter.
func (b *Bar) Writer() io.Writer { return writer{b} }

type writer struct{ b *Bar }

func (w writer) Write(p []byte) (int, error) {
	w.b.Add(int64(len(p)))
	return len(p), nil
}

// Finish draws the final state and terminates the line.
func (b *Bar) Finish() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.enabled || b.finished {
		return
	}
	b.draw(true)
	b.finished = true
	fmt.Fprint(b.out, "\n")
}

// draw repaints the line. The caller must hold b.mu.
func (b *Bar) draw(force bool) {
	if !b.enabled || b.finished {
		return
	}

	now := time.Now()
	if !force && now.Sub(b.lastDraw) < renderPeriod {
		return
	}
	b.lastDraw = now

	line := b.line(now)
	padding := b.lastLen - len(line)
	if padding > 0 {
		line += strings.Repeat(" ", padding)
	}
	b.lastLen = len(line) - max(padding, 0)

	fmt.Fprint(b.out, "\r"+line)
}

// line builds the visible text of the bar. The caller must hold b.mu.
func (b *Bar) line(now time.Time) string {
	elapsed := now.Sub(b.start)

	rate := ""
	if elapsed > 0 && b.current > 0 {
		rate = Bytes(int64(float64(b.current)/elapsed.Seconds())) + "/s"
	}

	// Unknown total: spinner plus the amount processed so far.
	if b.total <= 0 {
		spin := string(spinnerChars[b.spins%len(spinnerChars)])
		b.spins++
		parts := []string{b.desc, spin, Bytes(b.current)}
		if rate != "" {
			parts = append(parts, rate)
		}
		return strings.Join(parts, "  ")
	}

	ratio := float64(b.current) / float64(b.total)
	if ratio > 1 {
		ratio = 1
	} else if ratio < 0 {
		ratio = 0
	}

	filled := int(ratio * barWidth)
	bar := strings.Repeat("=", filled)
	if filled < barWidth {
		bar += ">" + strings.Repeat(" ", barWidth-filled-1)
	}

	text := fmt.Sprintf("%s  [%s] %3.0f%%  %s/%s",
		b.desc, bar, ratio*100, Bytes(b.current), Bytes(b.total))
	if rate != "" {
		text += "  " + rate
	}
	if remaining := b.total - b.current; remaining > 0 && b.current > 0 && elapsed > 0 {
		perByte := elapsed.Seconds() / float64(b.current)
		text += "  ETA " + formatDuration(time.Duration(perByte*float64(remaining))*time.Second)
	}
	return text
}

// Bytes formats a byte count using binary units (KB, MB, GB...).
func Bytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	value := float64(n)
	unit := -1
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	return fmt.Sprintf("%.1f %s", value, units[unit])
}

// formatDuration renders an ETA as mm:ss or h:mm:ss.
func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	total := int64(d / time.Second)
	hours := total / 3600
	minutes := (total % 3600) / 60
	seconds := total % 60
	if hours > 0 {
		return fmt.Sprintf("%d:%02d:%02d", hours, minutes, seconds)
	}
	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}
