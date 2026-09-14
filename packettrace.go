package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var packetTraceID atomic.Uint64
var packetTraceBudget atomic.Int64

type packetCapture struct {
	mu    sync.Mutex
	file  *os.File
	path  string
	total int64
	saved int64
	start time.Time
}

func newPacketCapture(label string) *packetCapture {
	id := packetTraceID.Add(1)
	p := &packetCapture{path: fmt.Sprintf("/tmp/ah4c-trace-%d-%s.ts", id, label), start: time.Now()}
	f, err := os.OpenFile(p.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	p.file = f
	logger("[PACKET TRACE] path=%s opened err=%v", p.path, err)
	return p
}

func (p *packetCapture) record(b []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.total += int64(len(b))
	if p.file == nil {
		return
	}
	n := min(len(b), int(16*1024*1024-p.saved))
	if n <= 0 {
		return
	}
	if packetTraceBudget.Add(int64(n)) > 128*1024*1024 {
		p.file.Close()
		p.file = nil
		logger("[PACKET TRACE] path=%s global capture limit reached", p.path)
		return
	}
	w, err := p.file.Write(b[:n])
	p.saved += int64(w)
	if p.saved == int64(w) || err != nil || p.saved == 16*1024*1024 {
		logger("[PACKET TRACE] path=%s total=%d saved=%d elapsed=%v err=%v", p.path, p.total, p.saved, time.Since(p.start), err)
	}
	if err != nil || p.saved == 16*1024*1024 {
		p.file.Close()
		p.file = nil
	}
}

func (p *packetCapture) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file != nil {
		p.file.Close()
		p.file = nil
	}
	logger("[PACKET TRACE] path=%s closed total=%d saved=%d elapsed=%v", p.path, p.total, p.saved, time.Since(p.start))
}

type packetTraceBody struct {
	io.ReadCloser
	capture *packetCapture
}

func (p *packetTraceBody) Read(b []byte) (int, error) {
	defer traceFunction("packetTraceBody.Read", p, "capture=%s", p.capture.path)()

	start := time.Now()
	n, err := p.ReadCloser.Read(b)
	p.capture.record(b[:n])
	if err != nil || time.Since(start) > 250*time.Millisecond {
		logger("[PACKET TRACE] path=%s read=%d err=%v blocked=%v", p.capture.path, n, err, time.Since(start))
	}
	return n, err
}
func (p *packetTraceBody) Close() error {
	defer traceFunction("packetTraceBody.Close", nil, "reader=%p capture=%s", p, p.capture.path)()

	err := p.ReadCloser.Close()
	p.capture.close()
	return err
}

func packetTraceGet(url string) (*http.Response, error) {
	defer traceFunction("packetTraceGet", nil, "url=%s", url)()

	started := time.Now()
	logger("[PACKET TRACE] GET begin url=%s at=%s", url, started.Format(time.RFC3339Nano))
	resp, err := http.Get(url)
	if err != nil {
		logger("[PACKET TRACE] GET failed elapsed=%v err=%v", time.Since(started), err)
		return resp, err
	}
	capture := newPacketCapture("source")
	logger("[PACKET TRACE] path=%s GET headers elapsed=%v status=%s headers=%v", capture.path, time.Since(started), resp.Status, resp.Header)
	resp.Body = &packetTraceBody{ReadCloser: resp.Body, capture: capture}
	return resp, nil
}
