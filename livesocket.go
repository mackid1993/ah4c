package main

// The socket to the DVR is a reservoir too, and a bigger one than any queue in
// this program.
//
// The stall queue was capped at four chunks because two megabytes of it was
// holding the viewer seconds behind. But look at what a drop from that queue
// actually means: the producer could not hand a chunk on, which means the
// consumer was not taking one, which means the write to the DVR was blocking.
// A write blocks only when the kernel's send buffer for that socket is already
// full. So every one of those drop lines is also proof that a second reservoir,
// downstream of ours and outside this program, was full at the same moment.
//
// Linux autotunes a send buffer up into the megabytes on a connection that is
// written to hard, which is exactly this one. Those bytes are stale video: they
// were current when they were written and they are handed to the DVR whenever
// it gets round to reading. Nothing in ah4c can take them back — once a byte is
// in the send buffer it is committed — so the only way not to be behind by a
// buffer's worth is for the buffer not to be that big.
//
// Capping it does not lose anything. This is a live stream: if the DVR cannot
// keep up, the choice is to drop or to lag, and dropping is what keeps a viewer
// at the live edge. A small send buffer simply moves the decision back into
// this program, where the stall queue already drops what it cannot pass on,
// instead of leaving it to a kernel buffer that only ever hoards.

import (
	"net"

	"github.com/gin-gonic/gin"
)

// dvrSendBuffer is how many bytes the kernel may hold for the DVR. At a
// broadcast bitrate this is a couple of hundred milliseconds — enough that a
// write is not blocking on every packet, and far too little to hide a second
// of video in. The default is autotuned into the megabytes, which is seconds.
const dvrSendBuffer = 256 * 1024

// liveListener caps the send buffer on every connection it accepts, so no
// connection this program serves can bank more than dvrSendBuffer of stream.
type liveListener struct{ net.Listener }

func (l liveListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return c, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		// Best effort: a kernel that will not take the size still serves the
		// stream, it just keeps its own idea of how much to hold.
		_ = tc.SetWriteBuffer(dvrSendBuffer)
	}
	return c, nil
}

// serveLive runs the router on addr with the send buffer capped. It replaces
// r.Run, which builds its own listener and leaves the buffer to the kernel.
func serveLive(r *gin.Engine, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	logger("[START] serving on %s with the DVR's send buffer capped at %s, so the kernel cannot hold a stream back", addr, byteCount(dvrSendBuffer))
	return r.RunListener(liveListener{ln})
}
