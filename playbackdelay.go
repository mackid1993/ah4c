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
	// The wait's filler: null packets at a steady volume for the whole hold,
	// nullBurst every nullPace, about 3.7 KB/s. An earlier diet dropped to a
	// thin keepalive after a six second detection window and brought the
	// volume back for a second every five. That made the number of near-dead
	// stretches grow with the hold — about four at forty-five seconds, about
	// thirteen at ninety — which is the one thing in what the hold sends that
	// scales with its length, and forty-five is where holds stop landing at
	// the live edge. So the volume is now steady, to see whether those
	// stretches were what bounded the hold. Null packets carry no time, so
	// this is bytes and nothing else.
	nullPace  = 100 * time.Millisecond
	nullBurst = 2 * tsPacketSize
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

// liveEdge drops whatever the encoder had queued when it was opened.
func liveEdge(body io.ReadCloser, label string) {
	buf := make([]byte, 64*1024)
	var start, last uint64
	var have bool
	var dropped int64
	var ahead float64
	t0, settled := time.Now(), time.Now()
	for time.Since(t0) < liveEdgeBudget && dropped < liveEdgeMost {
		n, err := body.Read(buf)
		if err != nil {
			return
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
		logger("[HOLD] hold %v", holdDelay)
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

	mu     sync.Mutex
	body   io.ReadCloser
	closed bool
	nulls  int64
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
	return l
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
	if body != nil {
		return body.Read(p)
	}
	if len(l.pend) > 0 {
		n := copy(p, l.pend)
		l.pend = l.pend[n:]
		return n, nil
	}
	if d := time.Until(l.until); d > 0 {
		if l.preroll != nil {
			return l.showPreroll(p, d)
		}
		return l.serveNulls(p, d)
	}
	return l.open(p)
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
	pace, burst := holdRate(time.Since(l.dietFrom()))
	if d > pace || d <= 0 {
		d = pace
	}
	time.Sleep(d)
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
	// leaves nothing able to decline. The break is wanted — see refreshAfter —
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
	// Playback detection wraps it this way round, and its hand-off is clean.
	body := markDiscontinuity(maybeWrapCaptions(
		newGateReader(l.stallTolerant(l.refreshing(resp.Body)), armed, true, time.Now(), nil),
		l.tuner, l.name))
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		body.Close()
		return 0, io.EOF
	}
	l.body = body
	nulls := l.nulls
	l.mu.Unlock()
	logger("[HOLD] %s hold %v, %s sent, program starts", l.label, time.Since(l.t0).Round(time.Millisecond), byteCount(nulls+preroll))
	return body.Read(p)
}

func (l *lateEncoder) Close() error {
	l.mu.Lock()
	body := l.body
	l.closed = true
	l.mu.Unlock()
	if l.preroll != nil {
		l.preroll.stop()
		l.preroll = nil
	}
	if body != nil {
		return body.Close()
	}
	return nil
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
	h := &hintHold{conn: conn, rw: rw, began: time.Now()}
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
	return copyFlush(bufWriter{h.rw}, src)
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

// holdRate is how fast filler goes out: a steady volume for the whole hold,
// nullBurst every nullPace. It once tapered to a keepalive after a detection
// window, which made the stream go nearly dead in bursts whose count grew
// with the hold; that is being removed to see whether it was what bounded the
// hold at forty-five seconds. The argument is kept so the plumbing is there
// to taper again if steady volume turns out to matter. Its own function so
// the shape can be checked without a DVR.
func holdRate(time.Duration) (time.Duration, int) {
	return nullPace, nullBurst
}

// stallTolerant wraps the encoder in the stall reader whether or not
// NULL_FRAME_INSERTION is switched on. A held tune is opened on a client that
// times out reading the body, so the hold is what breaks the stream every
// twenty seconds and the hold has to be what survives it. Without this, a
// container running the delay with NULL frame insertion off loses the stream
// twenty seconds after the programme starts, every time.
func (l *lateEncoder) stallTolerant(body io.ReadCloser) io.ReadCloser {
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

// refreshAfter is how long after the program starts the encoder connection is
// reopened once. The break discards whatever the player has buffered ahead of
// the show and starts again at the encoder's live output, which is what pulls
// the viewer back to the live edge; without it the viewer stays wherever the
// hand-off left them. Twenty seconds is what every hold up to forty-five was
// watched with. It does not, on its own, fix a hold longer than forty-five.
const refreshAfter = 20 * time.Second

// refreshing reopens the encoder once, shortly after the programme starts, and
// only if the encoder will have it. The new connection is opened before the old
// one is closed, so an encoder that refuses a second reader — and some do,
// while a tuner owns the stream — costs nothing at all: the refresh is declined,
// said so once, and never tried again for this tune.
func (l *lateEncoder) refreshing(body io.ReadCloser) io.ReadCloser {
	return &refreshSource{ReadCloser: body, at: time.Now().Add(refreshAfter), label: l.label, open: func() (io.ReadCloser, error) {
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
	open  func() (io.ReadCloser, error)
	at    time.Time
	done  bool
	label string
	// The reopen is a fresh encoder session with its own time base, so its
	// first packets carry the discontinuity indicator — the same signal the
	// hand-off carries — to tell the player the clock is new and to reset to
	// it. Without it the reopen delivers fresh live content with no reset, and
	// the outer marker is already spent from the hand-off, so a player that
	// built a buffer over a long hold fits the new content behind that buffer
	// and stays there. This is what the reopen was meant to do all along and
	// silently was not.
	mark bool
	seen map[int]bool
}

func (r *refreshSource) Read(p []byte) (int, error) {
	if !r.done && !time.Now().Before(r.at) {
		r.done = true
		fresh, err := r.open()
		if err != nil {
			logger("[HOLD] %s the encoder would not open a second time (%v); leaving the stream as it is", r.label, err)
		} else {
			old := r.ReadCloser
			r.ReadCloser = fresh
			old.Close()
			r.mark, r.seen = true, map[int]bool{}
			logger("[HOLD] %s reopened the encoder, marked the new time base, dropping what the DVR had stored ahead of the show", r.label)
		}
	}
	n, err := r.ReadCloser.Read(p)
	if r.mark && n > 0 {
		r.markReopen(p[:n])
	}
	return n, err
}

// markReopen sets the discontinuity indicator on the first packet of each PID
// after a reopen, then steps aside once a read has marked any — the same rule
// the hand-off's marker follows.
func (r *refreshSource) markReopen(p []byte) {
	marked := 0
	for i := 0; i+tsPacketSize <= len(p); i += tsPacketSize {
		pkt := p[i : i+tsPacketSize]
		if pkt[0] != 0x47 || pkt[3]>>4&2 == 0 || pkt[4] == 0 {
			continue
		}
		pid := int(pkt[1]&0x1F)<<8 | int(pkt[2])
		if pid == 0x1FFF || r.seen[pid] {
			continue
		}
		r.seen[pid] = true
		pkt[5] |= 0x80
		marked++
	}
	if marked > 0 {
		r.mark = false
	}
}
