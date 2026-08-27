package main

// Watching the writes to the DVR. Every instrument so far rides the read
// side: the pace watcher proves the bytes leave lateEncoder on the wall's
// clock, and the stall queue reports how deep it stands. The one stretch
// nobody has ever measured is the last one — the write into the DVR's socket.
// The kernel's send buffer can hold megabytes, a megabyte is seconds of
// video, and bytes standing there are downstream of every flush and drop this
// program has: nothing inside ah4c can shed them. A write only blocks when
// that buffer is full, so the time spent blocked in Write is the one number
// that says whether the DVR is draining ah4c or damming it. Zero means the
// lag the viewer sees lives past the DVR's ingest, where no byte-stream
// change here can reach it; whole seconds mean the DVR itself reads slower
// than the encoder sends, and now there is a log line saying which instead of
// an argument either way.

import (
	"time"
)

// writeStallEvery is how often the blocked time is reported: the pace
// watcher's cadence, so the two lines land side by side in the log.
const writeStallEvery = 15 * time.Second

// stallWatchedWriter times every write on its way to the DVR and reports the
// time spent blocked. It is used from a single copy loop, so plain fields and
// no lock.
type stallWatchedWriter struct {
	dst     flushWriter
	label   string
	last    time.Time     // when the last report was made
	blocked time.Duration // time blocked in Write and Flush since then
	worst   time.Duration // the single slowest write since then
	total   time.Duration // time blocked over the whole tune
}

// watchWriteStalls wraps the DVR-facing writer with the report.
func watchWriteStalls(dst flushWriter, label string) flushWriter {
	return &stallWatchedWriter{dst: dst, label: label, last: time.Now()}
}

func (w *stallWatchedWriter) Write(p []byte) (int, error) {
	t0 := time.Now()
	n, err := w.dst.Write(p)
	w.note(time.Since(t0))
	if t0.Sub(w.last) >= writeStallEvery {
		logger("[HOLD] %s writes to the DVR blocked %dms of the last %v (worst single write %dms; %v blocked in all this tune)",
			w.label, w.blocked.Milliseconds(), t0.Sub(w.last).Round(time.Second),
			w.worst.Milliseconds(), w.total.Round(time.Millisecond))
		w.last, w.blocked, w.worst = t0, 0, 0
	}
	return n, err
}

// Flush is timed too: the hint path writes through a bufio whose tail leaves
// in the flush, so blocking there is the same backpressure by another door.
func (w *stallWatchedWriter) Flush() {
	t0 := time.Now()
	w.dst.Flush()
	w.note(time.Since(t0))
}

func (w *stallWatchedWriter) note(d time.Duration) {
	w.blocked += d
	w.total += d
	if d > w.worst {
		w.worst = d
	}
}
