package main

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var fnTraceID atomic.Uint64
var fnTraceReads sync.Map

func traceFunction(name string, object any, format string, args ...any) func() {
	if object != nil {
		key := fmt.Sprintf("%s/%p", name, object)
		value, _ := fnTraceReads.LoadOrStore(key, new(atomic.Uint64))
		if value.(*atomic.Uint64).Add(1) > 3 {
			return func() {}
		}
	}
	id := fnTraceID.Add(1)
	started := time.Now()
	pcs := make([]uintptr, 10)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	var callers []string
	for {
		frame, more := frames.Next()
		callers = append(callers, fmt.Sprintf("%s@%s:%d", frame.Function, filepath.Base(frame.File), frame.Line))
		if !more {
			break
		}
	}
	identity := "-"
	if object != nil {
		identity = fmt.Sprintf("%T/%p", object, object)
	}
	writeFunctionTrace("[FN TRACE] id=%d enter=%s self=%s %s callers=%s", id, name, identity, fmt.Sprintf(format, args...), strings.Join(callers, " <- "))
	return func() { writeFunctionTrace("[FN TRACE] id=%d exit=%s elapsed=%v", id, name, time.Since(started)) }
}

func writeFunctionTrace(format string, args ...any) {
	text := fmt.Sprintf(format, args...)
	fmt.Println(text)
	loggerhandle.Println(text)
}
