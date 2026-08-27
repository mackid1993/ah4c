package main

// PLAYBACK_DELAY: hold a tune, opening the encoder when the delay is up so the
// DVR gets a session that starts when the viewer does. The hold itself, the
// 1xx window that fronts it, and the discontinuity marker that ends it.

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// holdDelay is the playback delay as parsed at startup; zero when unset.
var holdDelay time.Duration

// holdMost is as long as a tune is held, whatever PLAYBACK_DELAY asks for.
// Forty-five seconds is the longest hold watched land at the live edge; sixty
// and ninety come in behind the guide and stay there. Why is not yet settled
// — see holdRate — so this is lifted to ten minutes only to test longer holds,
// and is not a claim that they work. It is a guard against a typo, not a
// measured limit. Put it back to forty-five if a real answer does not arrive.
const holdMost = 10 * time.Minute

const (
	// The wait's byte diet: volume through the DVR's detection window, then
	// a keepalive. Every byte here is one the DVR stores ahead of the show.
	nullPace  = 100 * time.Millisecond
	nullBurst = 2 * tsPacketSize
	// Volume while the DVR decides the body is a stream, a keepalive after:
	// a trickle from the first byte starves it, and it gives up.
	nullDetect = 6 * time.Second
	nullIdle   = 500 * time.Millisecond
	// A DVR decides the body is a stream, and then keeps deciding. The
	// keepalive after nullDetect is three kilobits a second, and a DVR will
	// sit through about twenty seconds of that before concluding the stream
	// has died — which is why a forty-five second hold worked and a sixty
	// second one tuned again part way through. So the volume comes back for
	// nullBeatFor every nullBeat, and the thin stretch never runs longer than
	// four seconds however long the hold is. Four rather than eleven because
	// twenty-one seconds is one measurement on one box, and a DVR with less
	// patience than that one should hold too.
	nullBeat    = 5 * time.Second
	nullBeatFor = 1 * time.Second
	// flushBeforeArm is how far ahead of the gate's arming the stall queue is
	// emptied, so the gate hunts its keyframe through fresh bytes rather than
	// through whatever has been stored up behind it.
	flushBeforeArm = 300 * time.Millisecond
	// How long the encoder's clock must stop outrunning the wall, and the
	// most that may be spent or thrown away deciding.
	liveEdgeSettle = 250 * time.Millisecond
	liveEdgeBudget = 2 * time.Second
	liveEdgeMost   = 4 << 20
)

// maybeWrapNullFrameInsertion wraps body when NULL_FRAME_INSERTION is TRUE, so
// stalls are filled and the encoder at url is reconnected when it drops.
func maybeWrapNullFrameInsertion(body io.ReadCloser, url, label string) io.ReadCloser {
	if !strings.EqualFold(os.Getenv("NULL_FRAME_INSERTION"), "TRUE") {
		return body
	}
	return newStallTolerantReader(body, func() (io.ReadCloser, error) {
		r, e := http.Get(url)
		if e != nil {
			return nil, e
		}
		if r.StatusCode != 200 {
			r.Body.Close()
			return nil, fmt.Errorf("status %s", r.Status)
		}
		return r.Body, nil
	}, label)
}

// liveEdge drops whatever the encoder had queued when it was opened, and
// returns the last PCR it read at the live edge (33-bit, 90 kHz base) so the
// wait's clock can be re-anchored to the encoder's true live rather than to a
// probe taken tens of seconds earlier.
func liveEdge(body io.ReadCloser, label string) (uint64, bool) {
	buf := make([]byte, 64*1024)
	var start, last uint64
	var have bool
	var dropped int64
	var ahead float64
	t0, settled := time.Now(), time.Now()
	for time.Since(t0) < liveEdgeBudget && dropped < liveEdgeMost {
		n, err := body.Read(buf)
		if err != nil {
			return last, have
		}
		dropped += int64(n)
		for i := 0; i+tsPacketSize <= n; i += tsPacketSize {
			p := buf[i : i+tsPacketSize]
			if p[0] != 0x47 || p[3]>>4&2 == 0 || p[4] < 7 || p[5]&0x10 == 0 {
				continue
			}
			last = uint64(p[6])<<25 | uint64(p[7])<<17 | uint64(p[8])<<9 | uint64(p[9])<<1 | uint64(p[10])>>7
			// The clock free-runs and wraps, so anything that is not a
			// forward step of a sane size starts the measurement over.
			if !have || last < start || last-start > 60*90000 {
				start, have, ahead = last, true, 0
				settled = time.Now()
			}
		}
		if have {
			if by := float64(last-start)/90000 - time.Since(t0).Seconds(); by > ahead+0.05 {
				ahead, settled = by, time.Now()
			}
		}
		if time.Since(settled) >= liveEdgeSettle {
			break
		}
	}
	logger("[HOLD] %s dropped %s the encoder had queued (%.1fs ahead)", label, byteCount(dropped), ahead)
	return last, have
}

// tuneHoldStartup parses the delay and prepares the pre-roll, before the listener binds.
func tuneHoldStartup() {
	holdDelay = 0
	if s := os.Getenv("PLAYBACK_DELAY"); strings.TrimSpace(s) != "" {
		d, err := parseHoldDuration(s)
		if err != nil {
			logger("[HOLD] PLAYBACK_DELAY %q %v; tunes are not being held", s, err)
		} else {
			holdDelay = d
			if holdDelay > holdMost {
				logger("[HOLD] PLAYBACK_DELAY %s is longer than %s, which is as long as this build will hold a tune; holding for %s",
					holdWords(holdDelay), holdWords(holdMost), holdWords(holdMost))
				holdDelay = holdMost
			}
		}
	}
	prerollStartup()
	detect := strings.EqualFold(os.Getenv("PLAYBACK_DETECTION"), "TRUE")
	if holdDelay > 0 && detect {
		logger("[HOLD] PLAYBACK_DELAY is set, so PLAYBACK_DETECTION does not run: the delay decides when the program starts")
	}
	switch {
	case holdDelay > 0 && prerollTS != "":
		logger("[HOLD] hold %v with the pre-roll", holdDelay)
	case holdDelay > 0:
		logger("[HOLD] hold %v; the encoder is reopened %s into the program", holdDelay, holdWords(refreshAfterHold(holdDelay)))
	case detect && prerollTS != "":
		logger("[HOLD] pre-roll shows while playback detection holds a tune")
	case prerollTS != "":
		logger("[PREROLL] mounted, but nothing holds a tune: set PLAYBACK_DELAY or PLAYBACK_DETECTION to see it at tune time. It still covers stalls under NULL_FRAME_INSERTION")
	}
}

// holdWords says a hold's length the way a person reads it.
func holdWords(d time.Duration) string {
	d = d.Round(time.Second)
	m, sec := int(d/time.Minute), int((d%time.Minute)/time.Second)
	unit := func(n int, word string) string {
		if n == 1 {
			return "1 " + word
		}
		return fmt.Sprintf("%d %ss", n, word)
	}
	switch {
	case m > 0 && sec > 0:
		return unit(m, "minute") + " " + unit(sec, "second")
	case m > 0:
		return unit(m, "minute")
	default:
		return unit(sec, "second")
	}
}

// holdUnit reads spelled-out units: "1 hour", "90 seconds", "2 mins".
var holdUnit = regexp.MustCompile(`(?i)(\d+(?:\.\d+)?)\s*(hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)\b`)

// parseHoldDuration reads the delay: bare seconds, or 45s / 1m30s / 1h.
func parseHoldDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("is negative")
		}
		return time.Duration(n * float64(time.Second)), nil
	}
	norm := holdUnit.ReplaceAllStringFunc(s, func(m string) string {
		parts := holdUnit.FindStringSubmatch(m)
		return parts[1] + strings.ToLower(parts[2][:1])
	})
	norm = strings.ReplaceAll(norm, " ", "")
	d, err := time.ParseDuration(norm)
	if err != nil {
		return 0, fmt.Errorf("is not a number of seconds or a duration like 45s, 2m or 1h")
	}
	if d < 0 {
		return 0, fmt.Errorf("is negative")
	}
	return d, nil
}

// lateEncoder is the hold as a source: filler, then an encoder opened late.
type lateEncoder struct {
	url     string
	label   string
	t0      time.Time
	until   time.Time
	preroll *prerollPlayer
	pend    []byte
	// tuner and name are what captions are wrapped with, applied to the
	// encoder's own stream rather than to the hold in front of it.
	tuner int
	name  string

	// clock keeps a program and PCR alive through a long wait, so the player
	// does not re-acquire at the hand-off. nil for short waits and pre-rolls.
	clock *holdClock
	// opening and handoff drive the clock hand-off: the encoder is opened and
	// gated to a keyframe on a goroutine while the clock keeps emitting PCR, so
	// the keyframe wait does not stall the player's clock.
	opening atomic.Bool
	handoff chan *handoffResult
	// refresh is the encoder's one reopen. Its clock is started at the
	// hand-off, not at the open: the encoder is now opened at tune start, and
	// a shed spent during the wait is a shed the viewer never gets.
	refresh *refreshSource
	// drain is the gated chain drainEarly is reading, kept so Close can shut
	// the encoder even when the hand-off has not happened yet.
	drain io.ReadCloser

	mu     sync.Mutex
	body   io.ReadCloser
	closed bool
	nulls  int64
	// stall is the drained path's stall reader, kept so the hand-off and the
	// pace watcher can say how deep its queue stands instead of leaving the
	// depth to be inferred from drop events. nil on the pre-roll path, which
	// never drains. Written and read under mu.
	stall *stallTolerantReader
}

// handoffResult is the gated encoder, primed past its keyframe wait: first is
// the bytes the gate released (tables and the keyframe), body the rest.
type handoffResult struct {
	first []byte
	body  io.ReadCloser
}

// newLateEncoder holds from t0 until the delay is up, then opens url. early is
// a pre-roll already playing, which it takes over and shows for the wait.
func newLateEncoder(url, label string, t0 time.Time, early *prerollPlayer, tuner int, name string) *lateEncoder {
	l := &lateEncoder{url: url, label: label, t0: t0, until: t0.Add(holdDelay), preroll: early, tuner: tuner, name: name}
	if l.preroll != nil {
		l.preroll.adopted.Store(true)
	} else {
		l.preroll = startPreroll(label)
	}
	// A long wait runs the encoder's own program so the player keeps its
	// clock; a short one is already at the live edge on NULL packets, and a
	// pre-roll fills the wait itself.
	if l.preroll == nil && holdDelay > holdClockMinDelay {
		l.clock = startHoldClock(url, label, l.until)
	}
	// Every part of this program that works keeps the encoder open and reads
	// it for the whole time the DVR is waiting: the stall-tolerant reader
	// fills gaps in a stream it is still reading, and playback detection opens
	// the encoder at tune start and lets the gate drain it until the app is
	// seen playing. Only this hold left the encoder shut and opened it cold at
	// the end, and it is the only one with a ceiling. So it opens here now.
	// A pre-roll hold is left exactly as it was — it is a different feature
	// and it works.
	if l.preroll == nil {
		l.handoff = make(chan *handoffResult, 1)
		go l.drainEarly()
	}
	return l
}

// drainEarly opens the encoder at tune start and reads it for the whole wait.
// The gate discards everything it reads until the delay is up, so the DVR is
// never sent the wait's video — it gets NULL packets, exactly as before — and
// when the delay passes the gate releases a live keyframe. Nothing is opened
// cold at the end, and the connection the program arrives on has been up and
// flowing the entire time, which is the one thing this hold did not share with
// the features that work.
func (l *lateEncoder) drainEarly() {
	var body io.ReadCloser
	for {
		l.mu.Lock()
		closed := l.closed
		l.mu.Unlock()
		if closed {
			return
		}
		resp, err := http.Get(l.url)
		if err == nil && resp.StatusCode != 200 {
			resp.Body.Close()
			err = fmt.Errorf("status %s", resp.Status)
		}
		if err == nil {
			l.refresh = l.refreshing(resp.Body)
			// Captions wrap the encoder itself, inside the gate, rather than
			// the gate's output. Outside it they see their first frame at the
			// hand-off, and the log shows what that costs: Vulkan enumerated,
			// two backends dlopened and the devices registered, all in the
			// same second the program starts — the whole engine booting
			// between the release and the DVR's first frame. That is rule 13's
			// caption mistake and rule 6's "must be effectively free", and it
			// happened because until now there was no video to give them any
			// earlier. There is now: the encoder drains for the whole wait, so
			// the engine gets its first frame at the tune, warms up while
			// nothing is watching, and is already running at the hand-off.
			// The wait's own filler is never handed to them — this is the
			// encoder's stream, not the NULL packets in front of it.
			st := l.stallTolerant(l.refresh)
			l.mu.Lock()
			l.stall = st
			l.mu.Unlock()
			src := maybeWrapCaptions(st, l.tuner, l.name)
			// Timed: the gate arms itself when the delay is up and takes the
			// first keyframe after it. Until then it reads and throws away.
			body = markDiscontinuity(newGateReader(src, nil, true, l.until, nil))
			// Empty the queue just before the gate arms. The gate takes the
			// first keyframe after the delay is up, and a queue that is full at
			// that moment means it takes one out of two megabytes of stored-up
			// stream: the program starts seconds behind and stays there,
			// however carefully everything after the hand-off is arranged. The
			// log showed exactly that — "start on keyframe, 514ms from the
			// mark", "threw away 2.0 MB", "program starts", all in the same
			// second. Emptying it there is too late; the frames the gate chose
			// have already gone out. So it happens first, and what the gate
			// arms against is the live edge.
			go func(until time.Time) {
				if d := time.Until(until.Add(-flushBeforeArm)); d > 0 {
					time.Sleep(d)
				}
				st.flush(l.label)
			}(l.until)
			// Close has to be able to reach this. Until now it could not: it
			// only ever closed l.body, which is not set until the hand-off is
			// claimed, so a tune whose client left during the wait leaked the
			// encoder connection and the producer goroutine behind it — and
			// that producer keeps pulling the encoder at full rate for ever,
			// with nobody reading. Watched happening: a tuner with nothing
			// playing threw away three thousand chunks and climbing.
			l.mu.Lock()
			l.drain = body
			closed := l.closed
			l.mu.Unlock()
			if closed {
				body.Close()
				return
			}
			logger("[HOLD] %s encoder open and draining for the wait", l.label)
			break
		}
		logger("[HOLD] %s encoder would not open for the wait (%v); trying again", l.label, err)
		time.Sleep(time.Second)
	}
	// Read until the gate releases. It returns nothing while it is discarding
	// and while it hunts the keyframe, so a zero-byte read is not the end.
	buf := make([]byte, 64*1024)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			l.handoff <- &handoffResult{first: append([]byte(nil), buf[:n]...), body: body}
			return
		}
		if err != nil {
			logger("[HOLD] %s the drained encoder ended before the hand-off: %v", l.label, err)
			body.Close()
			l.handoff <- nil
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// dietFrom is when this tune's body opened, which is when the DVR starts
// deciding whether it has a stream: after the stretch held on 1xx when that
// is in play, and at the request itself when it is not.
func (l *lateEncoder) dietFrom() time.Time {
	if prerollTS == "" && hintsWork.Load() {
		return l.t0.Add(hintCeiling)
	}
	return l.t0
}

func (l *lateEncoder) Read(p []byte) (int, error) {
	l.mu.Lock()
	body, closed := l.body, l.closed
	l.mu.Unlock()
	if closed {
		return 0, io.EOF
	}
	// Pending bytes first, then the body: the clock hand-off leaves the gate's
	// first release (tables and the keyframe) in pend and sets body at once, so
	// draining pend before reading body keeps a large first chunk whole.
	if len(l.pend) > 0 {
		n := copy(p, l.pend)
		l.pend = l.pend[n:]
		return n, nil
	}
	if body != nil {
		return body.Read(p)
	}
	if l.preroll != nil {
		if d := time.Until(l.until); d > 0 {
			return l.showPreroll(p, d)
		}
		return l.open(p)
	}
	// The encoder has been open and draining since the tune, so the release
	// can arrive at any moment — including in the middle of a filler wait. A
	// look that does not block, followed by a sleep, meant the program sat in
	// the channel until the sleep ended: measured at 468ms of a 500ms
	// keepalive stretch, against a gate that had released a keyframe 0ms
	// behind the newest packet it had read. The picture was fresh; it was the
	// filler's own wait that made it stale, and it is the same 480ms whatever
	// else is changed, because nothing else touched it. So wait for the
	// release and the next burst of filler together, and let the release cut
	// the wait short.
	d, burst := l.nullPace(time.Until(l.until))
	wait := time.NewTimer(d)
	select {
	case r := <-l.handoff:
		wait.Stop()
		return l.takeHandoff(p, r)
	case <-wait.C:
	}
	return l.emitNulls(p, burst)
}

// takeHandoff swaps the filler for the released program and starts the
// reopen's clock, which must run from here and not from the encoder's open.
func (l *lateEncoder) takeHandoff(p []byte, r *handoffResult) (int, error) {
	if r == nil || r.body == nil {
		return 0, io.EOF
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		r.body.Close()
		return 0, io.EOF
	}
	// The pace watcher rides the drained hand-off too — it was only ever
	// wired on the paths that open the encoder late, so the one path the
	// user actually runs had gone silent.
	l.body, l.pend = l.watchEgress(r.body), r.first
	body, st := l.body, l.stall
	nulls := l.nulls
	l.mu.Unlock()
	if l.refresh != nil {
		l.refresh.arm(time.Now().Add(refreshAfterHold(holdDelay)))
	}
	logger("[HOLD] %s hold %v, %s sent, program starts", l.label, time.Since(l.t0).Round(time.Millisecond), byteCount(nulls))
	if st != nil {
		standing, deepest := st.queueGauge()
		logger("[HOLD] %s stall queue at the hand-off: %d of %d standing, deepest %d during the wait",
			l.label, standing, queueDepth, deepest)
	}
	if len(l.pend) > 0 {
		n := copy(p, l.pend)
		l.pend = l.pend[n:]
		return n, nil
	}
	return body.Read(p)
}

// clockHandoff opens and gates the encoder on a goroutine, and keeps the wait's
// clock running until it is ready, so the keyframe wait bridges on PCR rather
// than stalling the player's clock. The gate still starts the program on a
// keyframe, so the picture is clean; the clock is continuous, so there is no
// discontinuity to mark.
func (l *lateEncoder) clockHandoff(p []byte) (int, error) {
	if l.opening.CompareAndSwap(false, true) {
		l.handoff = make(chan *handoffResult, 1)
		go l.prepareHandoff()
	}
	select {
	case r := <-l.handoff:
		if r == nil || r.body == nil {
			return 0, io.EOF
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			r.body.Close()
			return 0, io.EOF
		}
		l.body = l.watchEgress(r.body)
		l.pend = r.first
		nulls := l.nulls
		l.mu.Unlock()
		if pts, ok := firstVideoPTS(r.first); ok {
			clockPTS := l.clock.pcrNow() / 300 // 27 MHz -> 90 kHz
			behindMs := (float64(clockPTS) - float64(pts)) / 90.0
			logger("[HOLD] %s seam: first picture is %.0fms from the clock's live edge", l.label, behindMs)
		} else {
			// The one instrument that compares the picture to a clock must not be
			// silent: say so, once per hand-off, with the size it had to work with.
			logger("[HOLD] %s seam: no video PTS found in the gate's first release (%s)", l.label, byteCount(int64(len(r.first))))
		}
		logger("[HOLD] %s hold %v, %s sent, program starts on the wait's clock",
			l.label, time.Since(l.t0).Round(time.Millisecond), byteCount(nulls))
		if len(l.pend) > 0 {
			n := copy(p, l.pend)
			l.pend = l.pend[n:]
			return n, nil
		}
		return r.body.Read(p)
	default:
		// Not ready: keep the clock alive so the player does not stall waiting.
		return l.serveHoldClock(p, 0)
	}
}

// prepareHandoff opens the encoder, gates it to a keyframe, and primes past
// the gate's keyframe wait here on its own goroutine — so that wait costs the
// player's clock nothing, because the main path keeps emitting PCR until this
// returns. It reports the gated body and the first bytes the gate released.
func (l *lateEncoder) prepareHandoff() {
	resp, err := http.Get(l.url)
	if err == nil && resp.StatusCode != 200 {
		resp.Body.Close()
		err = fmt.Errorf("status %s", resp.Status)
	}
	if err != nil {
		logger("[HOLD] %s encoder would not open after the hold: %v", l.label, err)
		l.handoff <- nil
		return
	}
	// Re-anchor the wait's clock to the encoder's true live PCR here, at the
	// hand-off, instead of leaving it on a probe taken tens of seconds ago —
	// liveEdge has just drained this fresh connection to the live edge and read
	// its PCR (33-bit, 90 kHz; the clock is 27 MHz, so ×300).
	if live, ok := liveEdge(resp.Body, l.label); ok {
		l.clock.reanchor(live * 300)
	}
	armed := make(chan struct{})
	close(armed)
	// The gate starts on a keyframe so the picture is clean, and is given the
	// clock so it releases only on a keyframe at the live edge — measured at
	// three milliseconds from it. The discontinuity indicator is then marked on
	// that keyframe: it is a clean entry point at the live edge, and the marker
	// is what tells the player to flush and re-anchor to it rather than stay a
	// buffer's length behind on the clock it was riding. It landed the player
	// behind when the keyframe was not yet at the live edge; now that it is,
	// the flush lands there.
	gate := newGateReader(l.stallTolerant(l.refreshing(resp.Body)), armed, true, time.Now(), nil)
	gate.bridge = l.clock
	body := markDiscontinuity(maybeWrapCaptions(gate, l.tuner, l.name))
	// Read until the gate releases the keyframe. A zero-byte read is not the
	// end — the gate returns nothing while it is still hunting the keyframe, so
	// it is retried, not treated as a dead encoder. Only a real error ends it.
	buf := make([]byte, 64*1024)
	var n int
	var rerr error
	for {
		n, rerr = body.Read(buf)
		if n > 0 || rerr != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n <= 0 {
		logger("[HOLD] %s encoder produced nothing after the hold: %v", l.label, rerr)
		body.Close()
		l.handoff <- nil
		return
	}
	l.handoff <- &handoffResult{first: append([]byte(nil), buf[:n]...), body: body}
}

// showPreroll passes the pre-roll on for what is left of the delay. The delay
// is what ends the wait, so a pre-roll longer than it is cut off, and one
// shorter is followed by NULL packets rather than starting the program early.
func (l *lateEncoder) showPreroll(p []byte, d time.Duration) (int, error) {
	gap := stallReadGap
	if d < gap {
		gap = d
	}
	select {
	case data, ok := <-l.preroll.out():
		if !ok {
			logger("[HOLD] %s pre-roll ended early; NULL packets for the rest of the wait", l.label)
			l.preroll = nil
			return l.serveNulls(p, d)
		}
		l.pend = append(l.pend, data...)
		n := copy(p, l.pend)
		l.pend = l.pend[n:]
		return n, nil
	case <-time.After(gap):
		if time.Until(l.until) <= 0 {
			return l.open(p)
		}
		return l.serveNulls(p, 0)
	}
}

// serveNulls sends NULL packets on the byte diet: volume through the DVR's
// detection window, a keepalive after. Every byte here is one the DVR stores
// ahead of the show.
func (l *lateEncoder) serveNulls(p []byte, d time.Duration) (int, error) {
	d, burst := l.nullPace(d)
	time.Sleep(d)
	return l.emitNulls(p, burst)
}

// nullPace is how long to wait before the next burst of filler and how big
// that burst may be, for a caller with d of the wait left to fill. Split out
// of serveNulls so a caller that has something better to do than sleep can
// wait on the diet's clock and on that other thing at once.
func (l *lateEncoder) nullPace(d time.Duration) (time.Duration, int) {
	pace, burst := holdRate(time.Since(l.dietFrom()))
	if d > pace || d <= 0 {
		d = pace
	}
	return d, burst
}

// emitNulls writes one burst of filler into p and counts what it sent.
func (l *lateEncoder) emitNulls(p []byte, burst int) (int, error) {
	if len(p) > burst {
		p = p[:burst]
	}
	n := nullPackets(p)
	l.mu.Lock()
	l.nulls += int64(n)
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return 0, io.EOF
	}
	return n, nil
}

func (l *lateEncoder) open(p []byte) (int, error) {
	var preroll int64
	if l.preroll != nil {
		preroll = l.preroll.stop()
		l.preroll = nil
	}
	// http.Get, not a client with a Timeout: that field covers reading the
	// body, so it breaks the stream by force at a moment nothing chose and
	// leaves nothing able to decline. The break is wanted — see refreshAfterHold —
	// but it is made deliberately below, and made so it can fail safely.
	resp, err := http.Get(l.url)
	if err == nil && resp.StatusCode != 200 {
		resp.Body.Close()
		err = fmt.Errorf("status %s", resp.Status)
	}
	if err != nil {
		logger("[HOLD] %s encoder would not open after the hold: %v", l.label, err)
		return 0, err
	}
	liveEdge(resp.Body, l.label)
	armed := make(chan struct{})
	close(armed)
	// Captions wrap the encoder's stream, not the hold in front of it.
	// Wrapping the hold hands the caption engine the pre-roll to work on and
	// lets it rewrite the pre-roll's own video packets on the way past.
	// A NULL hold carried no PCR, so the jump to the program's clock is a real
	// discontinuity to declare, and the gate starts it on a keyframe. A clock
	// hold does not come here — it hands off through clockHandoff.
	// Armed here and now: this path opens the encoder at the hand-off, so the
	// hand-off is this moment. refreshing hands back an unarmed source because
	// the drained path opens at the tune and must not spend its shed during
	// the wait — but nothing arms it here unless it is said, and an unarmed
	// source never sheds at all. Silent, and only on the pre-roll's path.
	rs := l.refreshing(resp.Body)
	rs.arm(time.Now().Add(refreshAfterHold(holdDelay)))
	body := markDiscontinuity(maybeWrapCaptions(
		newGateReader(l.stallTolerant(rs), armed, true, time.Now(), nil),
		l.tuner, l.name))
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		body.Close()
		return 0, io.EOF
	}
	l.body = l.watchEgress(body)
	nulls := l.nulls
	l.mu.Unlock()
	logger("[HOLD] %s hold %v, %s sent, program starts", l.label, time.Since(l.t0).Round(time.Millisecond), byteCount(nulls+preroll))
	return body.Read(p)
}

func (l *lateEncoder) Close() error {
	l.mu.Lock()
	body, drain := l.body, l.drain
	l.closed = true
	l.mu.Unlock()
	if l.preroll != nil {
		l.preroll.stop()
		l.preroll = nil
	}
	// A hand-off nobody claimed still holds an open encoder. drainEarly posts
	// the released body to l.handoff and moves on; if the client has already
	// gone, Read is never called again, so the body sits in the channel with
	// its producer goroutine reading the encoder for ever. Take it and close
	// it. Non-blocking: an empty channel means the hand-off never happened,
	// and the drain below is what holds the connection in that case.
	select {
	case r := <-l.handoff:
		if r != nil && r.body != nil {
			r.body.Close()
		}
	default:
	}
	if drain != nil {
		drain.Close()
	}
	if body != nil {
		return body.Close()
	}
	return nil
}

// --- Watching the stream's pace after the hand-off ---
// Every instrument on the hold answers at the hand-off, and the question that
// remains is what happens after it: a hand-off measured dead live can sit
// behind on the TV, and nothing here said anything while it did. The encoder's
// PCR advances with the wall, so the stream's own timestamps are the yardstick:
// a stream that leaves here behind the wall by more and more is ah4c falling
// behind; one that keeps pace while the TV sits behind puts the lag downstream
// of this program. The watcher only reads and reports — the bytes pass through
// as they arrived.

// egressDriftEvery is how often the pace is reported once the program starts.
const egressDriftEvery = 15 * time.Second

// egressDrift rides the program to the DVR and reports the stream's running
// gain or loss against the wall since the hand-off.
type egressDrift struct {
	io.ReadCloser
	label string
	// gauge, when set, is the stall queue's depth report, folded into the
	// pace line so the two numbers that matter — is the stream on pace, and
	// is anything standing between the encoder and the DVR — arrive together.
	gauge func() (int, int)

	mu   sync.Mutex
	have bool
	pcr0 uint64    // the first PCR seen, 27 MHz units
	t0   time.Time // when it was seen
	last time.Time // last report
}

// watchEgress dresses the program with the pace report. The DVR is handed the
// wrapper; the program inside it is untouched. Callers hold l.mu, which is
// what l.stall is read under.
func (l *lateEncoder) watchEgress(body io.ReadCloser) io.ReadCloser {
	e := &egressDrift{ReadCloser: body, label: l.label}
	if l.stall != nil {
		e.gauge = l.stall.queueGauge
	}
	return e
}

func (e *egressDrift) Read(p []byte) (int, error) {
	n, err := e.ReadCloser.Read(p)
	if n > 0 {
		e.watch(p[:n])
	}
	return n, err
}

// watch folds any PCR in b into the pace report. The newest PCR in the read is
// the one that counts: it is as close to the live edge as the byte stream gets.
func (e *egressDrift) watch(b []byte) {
	var pcr uint64
	var ok bool
	for i := 0; i+tsPacketSize <= len(b); i += tsPacketSize {
		pkt := b[i : i+tsPacketSize]
		if pkt[0] != 0x47 {
			continue
		}
		if v, yes := packetPCR(pkt); yes {
			pcr, ok = v, true
		}
	}
	if !ok {
		return
	}
	now := time.Now()
	e.mu.Lock()
	if !e.have {
		e.have, e.pcr0, e.t0, e.last = true, pcr, now, now
		e.mu.Unlock()
		logger("[HOLD] %s watching the stream's pace against the wall", e.label)
		return
	}
	if now.Sub(e.last) < egressDriftEvery {
		e.mu.Unlock()
		return
	}
	stream := time.Duration(pcr-e.pcr0) * time.Microsecond / 27
	wall := now.Sub(e.t0)
	// The clock free-runs and wraps; a wild step is a new base, not a drift.
	if d := (stream - wall).Abs(); d > time.Minute {
		e.pcr0, e.t0 = pcr, now
	}
	e.last = now
	e.mu.Unlock()
	if e.gauge != nil {
		standing, deepest := e.gauge()
		logger("[HOLD] %s stream pace %+.0fms against the wall since the hand-off; stall queue %d of %d standing, deepest %d since the last report",
			e.label, float64((stream - wall).Milliseconds()), standing, queueDepth, deepest)
		return
	}
	logger("[HOLD] %s stream pace %+.0fms against the wall since the hand-off", e.label, float64((stream - wall).Milliseconds()))
}

// --- The hand-off's discontinuity marker ---
// Telling the DVR the time base is new, so it does not read the jump from
// filler to program as corruption. ffmpeg spells this initial_discontinuity;
// there is no muxer in the path here, so it is set on the way past.

// firstDiscontinuity sets the discontinuity indicator on the first packet of
// each PID that carries an adaptation field, then steps aside.
type firstDiscontinuity struct {
	io.ReadCloser
	seen map[int]bool
	done bool
}

func markDiscontinuity(src io.ReadCloser) io.ReadCloser {
	return &firstDiscontinuity{ReadCloser: src, seen: map[int]bool{}}
}

func (f *firstDiscontinuity) Read(p []byte) (int, error) {
	n, err := f.ReadCloser.Read(p)
	if f.done || n <= 0 {
		return n, err
	}
	marked := 0
	for i := 0; i+tsPacketSize <= n; i += tsPacketSize {
		pkt := p[i : i+tsPacketSize]
		if pkt[0] != 0x47 || pkt[3]>>4&2 == 0 || pkt[4] == 0 {
			continue
		}
		pid := int(pkt[1]&0x1F)<<8 | int(pkt[2])
		if pid == 0x1FFF || f.seen[pid] {
			continue
		}
		f.seen[pid] = true
		pkt[5] |= 0x80
		marked++
	}
	if marked > 0 {
		f.done = true
	}
	return n, err
}

// --- The 1xx window ---
// A 1xx is protocol, not content: it puts nothing in the body, so the seconds
// spent on it cost the DVR no bytes. Bounded by the DVR's header clock, which
// no 1xx resets, so the hints stop short of it and the body carries the rest.

const (
	// hintEvery is how often a held request is kept alive.
	hintEvery = time.Second
	// hintCeiling is how long hints may run. The DVR's clock for the real
	// response headers runs from the request and no 1xx resets it: measured
	// at twenty-two seconds, so this keeps a wide margin under it.
	hintCeilingDefault = 18 * time.Second
	// hintProbe is how long to watch for a DVR that refuses 1xx outright.
	hintProbe = 750 * time.Millisecond
)

// hintCeiling is how long a hold may run on 1xx.
var hintCeiling = hintCeilingDefault

// hintsWork is cleared when a DVR refuses the hold outright, since taking the
// connection over cannot be undone.
var hintsWork atomic.Bool

func init() { hintsWork.Store(true) }

// hintHold is one request held on 1xx, with the real response written by hand.
type hintHold struct {
	conn  net.Conn
	rw    *bufio.ReadWriter
	sent  int
	began time.Time
	label string
}

// beginHintHold takes the connection over and probes the DVR with one hint,
// returning nil if it will not have them.
func beginHintHold(w http.ResponseWriter, label string) *hintHold {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		logger("[HOLD] %s the connection could not be taken over (%v); filling the body", label, err)
		return nil
	}
	h := &hintHold{conn: conn, rw: rw, began: time.Now(), label: label}
	if !h.hint() {
		h.refused(label, "would not take the first one")
		return nil
	}
	// A DVR that rejects informational responses closes at once; one that
	// accepts them says nothing, which is a read that times out.
	conn.SetReadDeadline(time.Now().Add(hintProbe))
	var b [1]byte
	if _, err := rw.Read(b[:]); err != nil {
		if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			h.refused(label, err.Error())
			return nil
		}
	}
	conn.SetReadDeadline(time.Time{})
	return h
}

// refused turns hints off for the process and drops the connection.
func (h *hintHold) refused(label, why string) {
	hintsWork.Store(false)
	logger("[HOLD] %s the DVR refused a held response (%s); holds from here fill the body", label, why)
	h.conn.Close()
}

// hint writes one informational response.
func (h *hintHold) hint() bool {
	if _, err := h.rw.WriteString("HTTP/1.1 103 Early Hints\r\n\r\n"); err != nil {
		return false
	}
	if err := h.rw.Flush(); err != nil {
		return false
	}
	h.sent++
	return true
}

// wait holds until until, and reports whether the DVR stayed.
func (h *hintHold) wait(until time.Time, label string) bool {
	for {
		left := time.Until(until)
		if left <= 0 {
			return true
		}
		if left > hintEvery {
			left = hintEvery
		}
		time.Sleep(left)
		if !h.hint() {
			hintsWork.Store(false)
			logger("[HOLD] %s the DVR gave up %v into the hold; holds from here fill the body", label, time.Since(h.began).Round(time.Millisecond))
			return false
		}
	}
}

// stream writes the real response and copies the program into it.
func (h *hintHold) stream(src io.Reader) (int64, error) {
	if _, err := h.rw.WriteString("HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\nConnection: close\r\n\r\n"); err != nil {
		return 0, err
	}
	if err := h.rw.Flush(); err != nil {
		return 0, err
	}
	// The write side gets the instrument here too; see writestall.go.
	return copyFlush(watchWriteStalls(bufWriter{h.rw}, h.label), src)
}

func (h *hintHold) Close() error { return h.conn.Close() }

// done ends a hop cleanly: the write side is shut first so the response is
// delivered rather than reset, which a bare close can do when anything is
// still unread on the connection.
func (h *hintHold) done() {
	if tc, ok := h.conn.(*net.TCPConn); ok {
		tc.CloseWrite()
		tc.SetReadDeadline(time.Now().Add(2 * time.Second))
		io.Copy(io.Discard, tc)
	}
	h.conn.Close()
}

// bufWriter flushes after every write, so no frame's tail waits on the next.
type bufWriter struct{ rw *bufio.ReadWriter }

func (b bufWriter) Write(p []byte) (int, error) { return b.rw.Write(p) }
func (b bufWriter) Flush()                      { b.rw.Flush() }

// holdOnHints holds a delayed tune on 1xx for as long as the DVR's clock
// allows, then hands back the connection to stream the rest. The second
// return says the connection has been taken over.
func holdOnHints(w http.ResponseWriter, src io.Reader, tuner, channel string) (*hintHold, bool) {
	if hintCeiling == 0 || holdDelay == 0 || prerollTS != "" || !hintsWork.Load() {
		return nil, false
	}
	label := "tuner=" + tuner + " channel=" + channel
	until := time.Now().Add(holdDelay)
	stop := time.Now().Add(hintCeiling)
	if until.Before(stop) {
		stop = until
	}
	h := beginHintHold(w, label)
	if h == nil {
		return nil, false
	}
	// The tune's scripts run off these reads; the filler itself is discarded.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		buf := make([]byte, 32*1024)
		for time.Until(stop) > time.Second {
			if _, err := src.Read(buf); err != nil {
				return
			}
		}
	}()
	ok := h.wait(stop, label)
	<-drained
	if !ok {
		h.Close()
		return nil, true
	}
	return h, true
}

// holdRate is how fast filler goes out, given how long the DVR has had a body
// to look at. Volume while it is deciding it has a stream, a keepalive after,
// and volume again for a moment every nullBeat so it goes on deciding that.
// Kept as its own function so the shape can be checked without a DVR.
func holdRate(since time.Duration) (time.Duration, int) {
	if since <= nullDetect {
		return nullPace, nullBurst
	}
	if (since-nullDetect)%nullBeat < nullBeatFor {
		return nullPace, nullBurst
	}
	return nullIdle, tsPacketSize
}

// stallTolerant wraps the encoder in the stall reader whether or not
// NULL_FRAME_INSERTION is switched on. A held tune is opened on a client that
// times out reading the body, so the hold is what breaks the stream every
// twenty seconds and the hold has to be what survives it. Without this, a
// container running the delay with NULL frame insertion off loses the stream
// twenty seconds after the program starts, every time.
func (l *lateEncoder) stallTolerant(body io.ReadCloser) *stallTolerantReader {
	return newStallTolerantReader(body, func() (io.ReadCloser, error) {
		r, e := http.Get(l.url)
		if e != nil {
			return nil, e
		}
		if r.StatusCode != 200 {
			r.Body.Close()
			return nil, fmt.Errorf("status %s", r.Status)
		}
		return r.Body, nil
	}, l.label)
}

// The reopen's timing is measured, not chosen: at a ninety second hold, five
// seconds into the program left the viewer five seconds behind; fifteen,
// twenty-seven and thirty all landed at the live edge. What twenty does at
// ninety was never on that list, and every build that went back to a flat
// twenty has come back behind — the shed has to fall outside whatever the
// player is still doing with the program it just acquired. refreshBase is what
// every hold up to refreshPer was watched with; past that the point scales
// with the hold, never above refreshMost, the longest point watched working.
const (
	refreshBase = 20 * time.Second
	refreshPer  = 45 * time.Second
	refreshMost = 30 * time.Second
)

// refreshAfterHold is how long after the program starts the encoder is
// reopened, for a hold of the given length. Its own function so the arithmetic
// can be checked without a DVR: twenty seconds at forty-five or under, thirty
// at ninety or longer.
func refreshAfterHold(hold time.Duration) time.Duration {
	if hold <= refreshPer {
		return refreshBase
	}
	// Through float64: a Duration is nanoseconds, and nanoseconds times
	// nanoseconds overflows int64 for any hold longer than nine seconds.
	d := time.Duration(float64(hold) * float64(refreshBase) / float64(refreshPer))
	if d > refreshMost {
		d = refreshMost
	}
	return d
}

// refreshing reopens the encoder once, shortly after the program starts, and
// only if the encoder will have it. The new connection is opened before the old
// one is closed, so an encoder that refuses a second reader — and some do,
// while a tuner owns the stream — costs nothing at all: the refresh is declined,
// said so once, and never tried again for this tune.
func (l *lateEncoder) refreshing(body io.ReadCloser) *refreshSource {
	return &refreshSource{ReadCloser: body, label: l.label, open: func() (io.ReadCloser, error) {
		r, e := http.Get(l.url)
		if e != nil {
			return nil, e
		}
		if r.StatusCode != 200 {
			r.Body.Close()
			return nil, fmt.Errorf("status %s", r.Status)
		}
		return r.Body, nil
	}}
}

type refreshSource struct {
	io.ReadCloser
	open func() (io.ReadCloser, error)
	// at is when the shed is due, as unix nanoseconds; zero until armed. It is
	// written by the DVR's goroutine at the hand-off and read by the drain's,
	// so it is atomic rather than a plain time.
	at    atomic.Int64
	done  bool
	label string
}

// arm starts the shed's clock. Until this is called there is no shed, so a
// connection opened at tune start does not spend it during the wait.
func (r *refreshSource) arm(at time.Time) { r.at.Store(at.UnixNano()) }

func (r *refreshSource) Read(p []byte) (int, error) {
	if at := r.at.Load(); !r.done && at != 0 && time.Now().UnixNano() >= at {
		r.done = true
		fresh, err := r.open()
		if err != nil {
			logger("[HOLD] %s the encoder would not open a second time (%v); leaving the stream as it is", r.label, err)
		} else {
			old := r.ReadCloser
			r.ReadCloser = fresh
			old.Close()
			logger("[HOLD] %s reopened the encoder, dropping what the DVR had stored ahead of the show", r.label)
		}
	}
	return r.ReadCloser.Read(p)
}
