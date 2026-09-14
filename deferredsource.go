package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

type deferredSource struct {
	url       string
	ready     <-chan struct{}
	closed    chan struct{}
	once      sync.Once
	closeOnce sync.Once
	mu        sync.Mutex
	body      io.ReadCloser
	err       error
	seen      bool
	retried   bool
}

func newDeferredSource(url string, ready <-chan struct{}) io.ReadCloser {
	return &deferredSource{url: url, ready: ready, closed: make(chan struct{})}
}

func (s *deferredSource) open() {
	resp, err := http.Get(s.url)
	if err != nil {
		s.err = err
		return
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		s.err = fmt.Errorf("status %s", resp.Status)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
		resp.Body.Close()
		s.err = io.ErrClosedPipe
	default:
		s.body = resp.Body
		s.err = nil
	}
}

func (s *deferredSource) Read(p []byte) (int, error) {
	select {
	case <-s.ready:
	case <-s.closed:
		return 0, io.EOF
	}
	s.once.Do(s.open)
	if s.err != nil {
		return 0, s.err
	}
	s.mu.Lock()
	body := s.body
	s.mu.Unlock()
	n, err := body.Read(p)
	if n > 0 {
		s.seen = true
	}
	if n == 0 && err != nil && !s.seen && !s.retried {
		select {
		case <-s.closed:
			return 0, io.EOF
		default:
		}
		s.retried = true
		body.Close()
		s.mu.Lock()
		if s.body == body {
			s.body = nil
		}
		s.mu.Unlock()
		logger("[SOURCE] transitional response ended before delivering bytes; reopening once")
		s.open()
		if s.err != nil {
			return 0, s.err
		}
		return s.Read(p)
	}
	return n, err
}

func (s *deferredSource) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.body != nil {
		return s.body.Close()
	}
	return nil
}
