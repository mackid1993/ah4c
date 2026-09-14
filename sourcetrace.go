package main

import (
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
)

var sourceTraceIDs atomic.Uint64

type sourceTraceBody struct {
	inner   io.ReadCloser
	id      uint64
	label   string
	started time.Time
	reads   int64
	bytes   int64
	first   bool
}

func traceSourceBody(inner io.ReadCloser, label string, resp *http.Response) io.ReadCloser {
	id := sourceTraceIDs.Add(1)
	logger("[SOURCE TRACE] id=%d %s opened status=%s length=%d transfer=%v body=%T", id, label, resp.Status, resp.ContentLength, resp.TransferEncoding, inner)
	return &sourceTraceBody{inner: inner, id: id, label: label, started: time.Now()}
}

func (s *sourceTraceBody) Read(p []byte) (int, error) {
	t0 := time.Now()
	n, err := s.inner.Read(p)
	s.reads++
	s.bytes += int64(n)
	if !s.first && n > 0 {
		s.first = true
		logger("[SOURCE TRACE] id=%d %s first bytes n=%d read=%d elapsed=%v", s.id, s.label, n, s.reads, time.Since(s.started))
	}
	if n == 0 || err != nil {
		logger("[SOURCE TRACE] id=%d %s read n=%d err=%v call=%v totalReads=%d totalBytes=%d elapsed=%v", s.id, s.label, n, err, time.Since(t0), s.reads, s.bytes, time.Since(s.started))
	}
	return n, err
}

func (s *sourceTraceBody) Close() error {
	buf := make([]byte, 8192)
	n := runtime.Stack(buf, false)
	logger("[SOURCE TRACE] id=%d %s local Close after %v reads=%d bytes=%d stack=%s", s.id, s.label, time.Since(s.started), s.reads, s.bytes, string(buf[:n]))
	return s.inner.Close()
}
