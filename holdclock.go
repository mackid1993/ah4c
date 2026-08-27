package main

// Keeping the player's clock alive through the hold.
//
// A pure NULL-packet hold carries no program and no PCR, so the player has no
// clock for the whole wait and acquires the program from scratch at the
// hand-off. That fresh acquisition is where it builds the playout buffer that
// leaves a long hold behind the live edge — measured present at the hand-off,
// before any reopen, and it grows with the wait. A forty-five second hold is
// short enough that the player lands at the live edge anyway; ninety is not.
//
// So the hold now runs a program. It probes the encoder once, early in the
// wait — the encoder reads fine before the hand-off opens it — for its real
// PAT and PMT and its current PCR, then emits those tables and a PCR that
// advances in real time from the value it read. The encoder's PCR free-runs
// at the wall's rate, so the PCR the hold emits stays continuous with what the
// encoder will be sending at the hand-off: the player's clock never stops and
// never jumps, and the program that arrives is the one it already acquired, on
// the same PIDs. Nothing here carries a picture or a sound — only the tables,
// the clock, and NULL fill. It is all in the transport stream.
//
// Everything is a best effort that falls back to NULL packets: a probe that
// fails, an encoder that will not be read early, a hold too short to need it —
// each leaves the wait exactly as it was.

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// holdClockMinDelay was the shortest hold that ran a synthesized program. The
// clock is now disengaged at every length: it is set past the longest hold this
// build allows, so the engagement condition is never true and nothing reaches
// startHoldClock. A 1m30s tune convicted it as the residual lag — the encoder
// handed over live (0.0s ahead), the gate released a keyframe -4ms from the
// clock, and the reopen dropped the DVR's buffer 19s after the hand-off, yet
// the picture still sat behind the guide. The only thing that path does that
// the working sub-45s hold does not is run the player on a streamless clock for
// the whole wait, so that is what is removed. The pure NULL hold — timeless
// filler, the encoder's untouched clock at the hand-off, the reopen shedding
// the buffer — is the detection mirror, and it carries a long hold the same way
// it carries a short one. holdclock.go stays for reference; set this back to
// 45 * time.Second to run the clock again.
const holdClockMinDelay = holdMost

// holdClock probes the encoder and then serves its program during the wait.
type holdClock struct {
	ready atomic.Bool
	dead  atomic.Bool
	pat   []byte // one whole PAT packet, verbatim from the encoder
	pmt   []byte // the wait's PMT: the encoder's program number and PCR PID,
	// but NO video/audio streams declared, so the player keeps its clock
	// without opening an A/V buffer it then waits a whole hold to fill. The
	// encoder's real PMT (with the streams) takes over at the hand-off, where
	// the picture is right there, and the player acquires it fresh like it does
	// on a NULL hold. This is what makes forty-five work, kept while the clock
	// removes the NULL hold's drift.
	pcrPID int
	base   uint64    // encoder PCR (27 MHz units) read at the probe
	anchor time.Time // wall time the base was read

	mu      sync.Mutex
	lastPSI time.Time
}

// crc32Mpeg is the MPEG-2 systems CRC (poly 0x04C11DB7, init all ones, MSB
// first, no final xor), used to close a PSI section built by hand.
func crc32Mpeg(b []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, x := range b {
		crc ^= uint32(x) << 24
		for i := 0; i < 8; i++ {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04C11DB7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// streamlessPMT builds a PMT packet on realPMT's PID that names the same
// program and PCR PID but declares no elementary streams, at a version one
// behind the encoder's so its real PMT reads as newer at the hand-off.
func streamlessPMT(realPMT []byte, pcrPID int) []byte {
	sec := psiPayload(realPMT)
	if len(sec) < 12 || sec[0] != 0x02 {
		return nil
	}
	prog := binary.BigEndian.Uint16(sec[3:5])
	ver := (sec[5] >> 1) & 0x1F
	minVer := (ver + 31) & 0x1F // one behind, so the encoder's PMT is newer
	// section: table_id..program_info_length, then CRC. section_length counts
	// from program_number through the CRC = 2+3+2+2+4 = 13.
	s := []byte{
		0x02,
		0xB0 | byte((13>>8)&0x0F), byte(13 & 0xFF),
		byte(prog >> 8), byte(prog),
		0xC0 | (minVer << 1) | 0x01,
		0x00, 0x00,
		0xE0 | byte((pcrPID>>8)&0x1F), byte(pcrPID),
		0xF0, 0x00,
	}
	crc := crc32Mpeg(s)
	s = append(s, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
	pmtPID := int(realPMT[1]&0x1F)<<8 | int(realPMT[2])
	pkt := make([]byte, tsPacketSize)
	pkt[0] = 0x47
	pkt[1] = 0x40 | byte((pmtPID>>8)&0x1F) // payload_unit_start
	pkt[2] = byte(pmtPID)
	pkt[3] = 0x10 // payload only, CC 0
	pkt[4] = 0x00 // pointer_field
	copy(pkt[5:], s)
	for i := 5 + len(s); i < tsPacketSize; i++ {
		pkt[i] = 0xFF
	}
	return pkt
}

// startHoldClock probes url on a goroutine, retrying until the encoder's
// program can be read or the wait is nearly over. The encoder is not always
// streaming a clean transport stream the instant the scripts finish — the app
// is still switching screens and the HDMI input renegotiating — so one probe
// at the top of the wait catches nothing. It is retried until the picture
// settles, which the lag captures showed happens some seconds in.
func startHoldClock(url, label string, until time.Time) *holdClock {
	c := &holdClock{}
	stop := until.Add(-10 * time.Second) // leave time to serve after a late probe
	go func() {
		var diag string
		for attempt := 1; time.Now().Before(stop); attempt++ {
			d, ok := c.probe(url)
			if ok {
				c.ready.Store(true)
				logger("[HOLD] %s running a streamless program through the wait (PCR on the encoder's own PID 0x%X, no video stream), so the player keeps its clock without a PCR-PID switch at the hand-off", label, c.pcrPID)
				return
			}
			diag = d
			time.Sleep(3 * time.Second)
		}
		c.dead.Store(true)
		logger("[HOLD] %s could not read the encoder's program for the wait (%s); the wait carries NULL packets", label, diag)
	}()
	return c
}

// probe reads the encoder briefly for its PAT, PMT and current PCR. It returns
// a description of what it saw and whether it found all three. The client has
// a timeout because this is a bounded probe, not the stream itself.
func (c *holdClock) probe(url string) (string, bool) {
	client := &http.Client{Timeout: 6 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "encoder would not open: " + err.Error(), false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "encoder returned " + resp.Status, false
	}
	buf := make([]byte, 0, 512*1024)
	tmp := make([]byte, 64*1024)
	deadline := time.Now().Add(5 * time.Second)
	seen := map[int]int{}
	var pat, pmt []byte
	pmtPID, pcrPID := -1, -1
	var sawPCR bool
	for time.Now().Before(deadline) {
		n, rerr := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		start := tsAlign(buf)
		var latest uint64
		var haveLatest bool
		for i := start; i+tsPacketSize <= len(buf); i += tsPacketSize {
			pkt := buf[i : i+tsPacketSize]
			if pkt[0] != 0x47 {
				break
			}
			pid := int(pkt[1]&0x1F)<<8 | int(pkt[2])
			seen[pid]++
			switch {
			case pid == 0 && pat == nil:
				if p := psiPayload(pkt); p != nil {
					if mp := patFirstPMT(p); mp >= 0 {
						pat, pmtPID = append([]byte(nil), pkt...), mp
					}
				}
			case pmtPID >= 0 && pid == pmtPID && pmt == nil:
				if p := psiPayload(pkt); p != nil {
					if cp := pmtPCRPID(p); cp >= 0 {
						pmt, pcrPID = append([]byte(nil), pkt...), cp
					}
				}
			}
			// Track the newest PCR in the buffer, not the first: the probe
			// spends some reads finding the tables, and the first PCR it saw is
			// that many seconds stale by the time it locks on. Anchoring to the
			// latest one starts the clock at the encoder's live edge.
			if pcrPID >= 0 && pid == pcrPID {
				if b, ok := packetPCR(pkt); ok {
					latest, haveLatest, sawPCR = b, true, true
				}
			}
		}
		if pat != nil && pmt != nil && haveLatest {
			// The wait runs a program with the encoder's number, its PCR on the
			// wait's own PID (off the video PID), and no streams, so the player
			// holds no A/V buffer and nothing lands on the video stream; the
			// encoder's real PMT takes over at the hand-off.
			min := streamlessPMT(pmt, pcrPID)
			if min == nil {
				min = pmt // fall back to the real PMT rather than run silent
			}
			c.pat, c.pmt, c.pcrPID = pat, min, pcrPID
			c.base, c.anchor = latest, time.Now()
			return "", true
		}
		if rerr != nil || len(buf) > 4<<20 {
			break
		}
	}
	return fmt.Sprintf("read %s, pids %v; pat=%v pmt(pid %d)=%v pcr(pid %d)=%v",
		byteCount(int64(len(buf))), topPIDs(seen), pat != nil, pmtPID, pmt != nil, pcrPID, sawPCR), false
}

// topPIDs lists the busiest PIDs seen, for a probe that could not parse.
func topPIDs(seen map[int]int) []string {
	type kv struct {
		pid, n int
	}
	var s []kv
	for p, n := range seen {
		s = append(s, kv{p, n})
	}
	sort.Slice(s, func(i, j int) bool { return s[i].n > s[j].n })
	var out []string
	for i := 0; i < len(s) && i < 5; i++ {
		out = append(out, fmt.Sprintf("0x%X:%d", s[i].pid, s[i].n))
	}
	return out
}

// pcrNow extrapolates the encoder's PCR (27 MHz) at the current wall time. The
// base and anchor are read under the lock as a pair: reanchor writes them as a
// pair, and a reader that split the two — a new base against the old anchor —
// would jump the clock by the whole wait and fail the gate's live-edge check
// on every keyframe.
func (c *holdClock) pcrNow() uint64 {
	c.mu.Lock()
	base, anchor := c.base, c.anchor
	c.mu.Unlock()
	return pcrAt(base, anchor)
}

// pcrAt extrapolates from a base/anchor pair already read under the lock.
func pcrAt(base uint64, anchor time.Time) uint64 {
	return base + uint64(time.Since(anchor).Nanoseconds())*27/1000
}

// reanchor resets the clock's base to a freshly measured encoder PCR (27 MHz),
// so it tracks the encoder's true live edge rather than an old probe. Called at
// the hand-off; the nudge is tens of milliseconds and lands under the trickle.
func (c *holdClock) reanchor(pcr27 uint64) {
	c.mu.Lock()
	c.base, c.anchor = pcr27, time.Now()
	c.mu.Unlock()
}

// The wait's PCR rides the encoder's own PCR PID, the same one the picture
// arrives on. It once rode a separate PID to keep the dense synthesized clock
// off the video stream, but that made the PCR PID switch at the hand-off and
// the player re-lock its clock — a fixed lag. Now that the wait is a bare
// trickle, it stays on the encoder's PID and the clock lock is unbroken across
// the seam, the way the encoder's own stream and PLAYBACK_DETECTION do it.

// clockPCREvery is how far apart the wait's PCRs are, in wall time. Every
// packet the wait injects is one the player then sits behind, and the measured
// lag fell as this rose, so it is as sparse as keeps the receiver's clock
// locked — past the spec ceiling, but far denser than the NULL hold that
// drifted with no PCR at all. serveHoldClock paces to it.
const clockPCREvery = 200 * time.Millisecond

// serve emits the wait's program as thinly as possible: the tables now and
// then, and one PCR on its own PID. No NULL fill — every extra packet is a
// packet the player ends up behind. Returns whole packets only.
func (c *holdClock) serve(p []byte) int {
	// Snapshot under the lock, then build: pcrPacket computes from the snapshot,
	// so the lock is never held across the extrapolation and never contends with
	// the re-anchor at the hand-off.
	c.mu.Lock()
	now := time.Now()
	psi := now.Sub(c.lastPSI) >= time.Second
	if psi {
		c.lastPSI = now
	}
	base, anchor := c.base, c.anchor
	c.mu.Unlock()
	var out []byte
	if psi {
		out = append(out, c.pat...)
		out = append(out, c.pmt...)
	}
	out = append(out, c.pcrPacket(pcrAt(base, anchor))...)
	if len(out) > len(p) {
		out = out[:len(p)/tsPacketSize*tsPacketSize]
	}
	return copy(p, out)
}

// pcrPacket builds an adaptation-only packet on the encoder's PCR PID —
// the same PID the picture arrives on — carrying pcr.
func (c *holdClock) pcrPacket(pcr uint64) []byte {
	base := pcr / 300
	ext := pcr % 300
	pkt := make([]byte, tsPacketSize)
	pkt[0] = 0x47
	pkt[1] = byte((c.pcrPID >> 8) & 0x1F)
	pkt[2] = byte(c.pcrPID & 0xFF)
	// adaptation field only, no payload; CC does not advance on such packets.
	pkt[3] = 0x20
	pkt[4] = 183  // adaptation_field_length: the rest of the packet
	pkt[5] = 0x10 // PCR_flag
	pkt[6] = byte(base >> 25)
	pkt[7] = byte(base >> 17)
	pkt[8] = byte(base >> 9)
	pkt[9] = byte(base >> 1)
	pkt[10] = byte(base<<7) | 0x7E | byte((ext>>8)&1)
	pkt[11] = byte(ext)
	for i := 12; i < tsPacketSize; i++ {
		pkt[i] = 0xFF
	}
	return pkt
}

// clockTrickle is how close to the hand-off the wait drops to a trickle, and
// how sparse that trickle is. The packets emitted right before the real video
// are the ones that sit next to it in the stream, so in the last few seconds
// the clock all but stops — just enough PCR to hold the lock — so almost
// nothing is between the filler and the picture when it arrives.
const (
	clockTrickleWithin = 6 * time.Second
	clockTricklePace   = 1500 * time.Millisecond
)

// serveHoldClock paces the wait's program and counts its bytes. untilHandoff is
// how long until the real video takes over; as that nears zero the pace drops
// to a trickle so the filler does not bleed into the feed. Returns io.EOF once
// the tune is closed.
func (l *lateEncoder) serveHoldClock(p []byte, untilHandoff time.Duration) (int, error) {
	pace := clockPCREvery
	if untilHandoff <= clockTrickleWithin {
		// The last stretch before the hand-off, and the keyframe wait after it
		// (untilHandoff <= 0), both trickle.
		pace = clockTricklePace
	}
	time.Sleep(pace)
	n := l.clock.serve(p)
	l.mu.Lock()
	l.nulls += int64(n)
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return 0, io.EOF
	}
	return n, nil
}

// --- transport-stream parsing helpers ---

// tsAlign returns the offset of the first byte of a run of aligned sync bytes.
func tsAlign(b []byte) int {
	for i := 0; i+3*tsPacketSize <= len(b); i++ {
		if b[i] == 0x47 && b[i+tsPacketSize] == 0x47 && b[i+2*tsPacketSize] == 0x47 {
			return i
		}
	}
	return 0
}

// psiPayload returns the section bytes of a PSI packet, past the pointer field.
func psiPayload(pkt []byte) []byte {
	if pkt[3]&0x10 == 0 { // no payload
		return nil
	}
	off := 4
	if pkt[3]&0x20 != 0 { // adaptation field present
		off += 1 + int(pkt[4])
	}
	if pkt[1]&0x40 != 0 { // payload_unit_start: a pointer field leads
		if off >= tsPacketSize {
			return nil
		}
		off += 1 + int(pkt[off])
	}
	if off >= tsPacketSize {
		return nil
	}
	return pkt[off:]
}

// patFirstPMT returns the PMT PID of the first real program in a PAT section.
func patFirstPMT(sec []byte) int {
	if len(sec) < 8 || sec[0] != 0x00 {
		return -1
	}
	slen := int(sec[1]&0x0F)<<8 | int(sec[2])
	end := 3 + slen - 4 // drop the 4-byte CRC
	for i := 8; i+4 <= end && i+4 <= len(sec); i += 4 {
		prog := binary.BigEndian.Uint16(sec[i : i+2])
		pid := int(sec[i+2]&0x1F)<<8 | int(sec[i+3])
		if prog != 0 {
			return pid
		}
	}
	return -1
}

// pmtPCRPID returns the PCR PID declared in a PMT section.
func pmtPCRPID(sec []byte) int {
	if len(sec) < 12 || sec[0] != 0x02 {
		return -1
	}
	return int(sec[8]&0x1F)<<8 | int(sec[9])
}

// packetPCR reads the PCR (27 MHz) from a packet's adaptation field.
func packetPCR(pkt []byte) (uint64, bool) {
	if pkt[3]&0x20 == 0 || pkt[4] < 7 || pkt[5]&0x10 == 0 {
		return 0, false
	}
	base := uint64(pkt[6])<<25 | uint64(pkt[7])<<17 | uint64(pkt[8])<<9 |
		uint64(pkt[9])<<1 | uint64(pkt[10])>>7
	ext := uint64(pkt[10]&0x01)<<8 | uint64(pkt[11])
	return base*300 + ext, true
}

// firstVideoPTS returns the first video PES presentation timestamp (90 kHz) in
// b — what the player actually displays. The video PID comes from the PMT
// carried in b rather than from guessing at stream IDs, and the PES header is
// reassembled across packets, because a keyframe packet often carries a large
// adaptation field that leaves it almost no payload of its own.
func firstVideoPTS(b []byte) (uint64, bool) {
	vid := map[int]bool{}
	var pes []byte
	have := false
	for i := 0; i+tsPacketSize <= len(b); i += tsPacketSize {
		pkt := b[i : i+tsPacketSize]
		if pkt[0] != 0x47 {
			continue
		}
		pid := int(pkt[1]&0x1F)<<8 | int(pkt[2])
		afc, pusi := pkt[3]>>4&3, pkt[1]&0x40 != 0
		if len(vid) == 0 {
			if sec := gatePSI(pkt, pusi, afc); len(sec) > 0 && sec[0] == 0x02 {
				if v := videoPIDs(sec); len(v) > 0 {
					vid = v
				}
			}
			continue
		}
		if !vid[pid] || afc&1 == 0 {
			continue
		}
		off := 4
		if afc >= 2 {
			off += 1 + int(pkt[4])
		}
		if off >= tsPacketSize {
			continue
		}
		pl := pkt[off:]
		if !have {
			// The header starts at a payload_unit_start: 00 00 01, stream_id.
			// Anything before that on this PID belongs to the previous PES.
			if !pusi || len(pl) < 4 || pl[0] != 0 || pl[1] != 0 || pl[2] != 1 {
				continue
			}
			have = true
		}
		pes = append(pes, pl...)
		if len(pes) >= 14 {
			break
		}
	}
	if len(pes) >= 14 && pes[7]&0x80 != 0 {
		pts := uint64(pes[9]&0x0E)<<29 | uint64(pes[10])<<22 | uint64(pes[11]&0xFE)<<14 |
			uint64(pes[12])<<7 | uint64(pes[13])>>1
		return pts, true
	}
	return 0, false
}
