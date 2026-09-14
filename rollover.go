package main

import (
	"io"
	"sync"
	"time"
)

type rolloverReader struct {
	mu        sync.Mutex
	body      io.ReadCloser
	reopen    func() (io.ReadCloser, error)
	closed    chan struct{}
	closeOnce sync.Once
}

func newRolloverReader(body io.ReadCloser, reopen func() (io.ReadCloser, error)) io.ReadCloser {
	defer traceFunction("newRolloverReader", nil, "source=%T/%p", body, body)()

	return &rolloverReader{body: body, reopen: reopen, closed: make(chan struct{})}
}

func (r *rolloverReader) Read(p []byte) (int, error) {
	defer traceFunction("rolloverReader.Read", r, "")()

	for {
		r.mu.Lock()
		body := r.body
		r.mu.Unlock()

		if body == nil {
			select {
			case <-r.closed:
				return 0, io.EOF
			default:
			}
			next, err := r.reopen()
			if err != nil {
				select {
				case <-r.closed:
					return 0, io.EOF
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
			r.mu.Lock()
			select {
			case <-r.closed:
				r.mu.Unlock()
				next.Close()
				return 0, io.EOF
			default:
				r.body = next
				body = next
				r.mu.Unlock()
			}
		}

		n, err := body.Read(p)
		if err != nil {
			body.Close()
			r.mu.Lock()
			if r.body == body {
				r.body = nil
			}
			r.mu.Unlock()
		}
		if n > 0 {
			return n, nil
		}
		if err == nil {
			continue
		}
		select {
		case <-r.closed:
			return 0, io.EOF
		default:
		}
	}
}

func (r *rolloverReader) Close() error {
	defer traceFunction("rolloverReader.Close", nil, "reader=%p", r)()

	r.closeOnce.Do(func() { close(r.closed) })
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}
