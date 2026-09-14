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
}

func newDeferredSource(url string, ready <-chan struct{}) io.ReadCloser {
	return &deferredSource{url: url, ready: ready, closed: make(chan struct{})}
}

func (s *deferredSource) Read(p []byte) (int, error) {
	select {
	case <-s.ready:
	case <-s.closed:
		return 0, io.ErrClosedPipe
	}
	s.once.Do(func() {
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
		select {
		case <-s.closed:
			resp.Body.Close()
			s.err = io.ErrClosedPipe
		default:
			s.body = resp.Body
		}
		s.mu.Unlock()
	})
	if s.err != nil {
		return 0, s.err
	}
	s.mu.Lock()
	body := s.body
	s.mu.Unlock()
	return body.Read(p)
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
