package progress

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestBytes(t *testing.T) {
	cases := map[int64]string{
		0:          "0 B",
		512:        "512 B",
		1024:       "1.0 KB",
		1536:       "1.5 KB",
		1048576:    "1.0 MB",
		1073741824: "1.0 GB",
	}
	for in, want := range cases {
		if got := Bytes(in); got != want {
			t.Errorf("Bytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	cases := map[time.Duration]string{
		0:                    "00:00",
		65 * time.Second:     "01:05",
		3661 * time.Second:   "1:01:01",
		-5 * time.Second:     "00:00",
		9 * time.Millisecond: "00:00",
	}
	for in, want := range cases {
		if got := formatDuration(in); got != want {
			t.Errorf("formatDuration(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestSilentWhenNotATerminal(t *testing.T) {
	var buf bytes.Buffer
	bar := NewWriter(&buf, 100, "test")

	if bar.Enabled() {
		t.Fatal("bar must be disabled when the writer is not a terminal")
	}

	bar.Set(50)
	bar.Describe("other")
	bar.Finish()

	if buf.Len() != 0 {
		t.Errorf("expected no output, got %q", buf.String())
	}
}

func TestWriterAdvancesBar(t *testing.T) {
	var buf bytes.Buffer
	bar := NewWriter(&buf, 10, "test")

	if _, err := bar.Writer().Write([]byte("hello")); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if bar.current != 5 {
		t.Errorf("current = %d, want 5", bar.current)
	}
}

func TestLineShowsPercentageAndBytes(t *testing.T) {
	var buf bytes.Buffer
	bar := NewWriter(&buf, 200, "Hashing files")
	bar.enabled = true
	bar.current = 50

	line := bar.line(time.Now())
	for _, want := range []string{"Hashing files", "25%", "50 B", "200 B", "["} {
		if !strings.Contains(line, want) {
			t.Errorf("line %q does not contain %q", line, want)
		}
	}
}

func TestLineSpinnerWhenTotalUnknown(t *testing.T) {
	var buf bytes.Buffer
	bar := NewWriter(&buf, -1, "Fetching manifest")
	bar.enabled = true
	bar.current = 2048

	line := bar.line(time.Now())
	if !strings.Contains(line, "Fetching manifest") || !strings.Contains(line, "2.0 KB") {
		t.Errorf("unexpected line for unknown total: %q", line)
	}
	if strings.Contains(line, "%") {
		t.Errorf("unknown total must not show a percentage: %q", line)
	}
}

func TestFinishWritesOnce(t *testing.T) {
	var buf bytes.Buffer
	bar := NewWriter(&buf, 10, "test")
	bar.enabled = true
	bar.current = 10

	bar.Finish()
	first := buf.String()
	bar.Finish()

	if first != buf.String() {
		t.Error("Finish must not draw twice")
	}
	if !strings.HasSuffix(first, "\n") {
		t.Errorf("Finish must end the line, got %q", first)
	}
}
