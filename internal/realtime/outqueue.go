package realtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ably/ably-server/internal/protocol"
)

// errSlowConsumer is returned by outQueue.push when the queue stayed at
// its byte limit for the whole write timeout: the client is not reading
// fast enough and the connection is disconnected (DESIGN.md §5.2).
var errSlowConsumer = errors.New("realtime: slow consumer: outbound queue full for the write timeout")

// errQueueClosed is returned by outQueue.push once the connection is
// closing.
var errQueueClosed = errors.New("realtime: outbound queue closed")

// outFrame is one encoded frame waiting for the write loop.
type outFrame struct {
	data   []byte
	wsType int
	action protocol.Action
	// queued is when a sampled connection queued the frame (zero
	// otherwise), for ably_conn_write_wait_seconds (DESIGN.md §10).
	queued time.Time
}

// outQueue is a connection's bounded outbound queue (DESIGN.md §5.2). It
// holds encoded frames, so its bound is in bytes: a byte bound caps the
// memory a connection can pin, where a message count would not (frames
// range from a few bytes to the 64 KiB message limit). Pushers are the
// attachment goroutines, the publish worker (ACK/NACK) and the read loop;
// the single consumer is the write loop.
//
// A push that would take the queue past its limit waits for the writer
// to make room — backpressure, which absorbs bursts such as a resume
// replay — but for at most the write timeout. A queue that stays full
// that long means the client is not reading, and push returns
// errSlowConsumer. A frame is always admitted to an empty queue whatever
// its size, so a single frame larger than the limit cannot wedge a
// connection.
//
// An idle queue holds no backing array, so it costs a few words per
// connection.
type outQueue struct {
	max     int64
	timeout time.Duration

	mu     sync.Mutex
	frames []outFrame
	head   int
	bytes  int64
	closed bool
	// space, when non-nil, is closed by pop to wake pushers waiting for
	// room; it is allocated only when a push has to wait.
	space chan struct{}
	// ready signals the write loop that frames are waiting. Capacity 1:
	// the writer drains everything each time it wakes.
	ready chan struct{}
}

func newOutQueue(maxBytes int64, timeout time.Duration) *outQueue {
	return &outQueue{max: maxBytes, timeout: timeout, ready: make(chan struct{}, 1)}
}

// push appends f, waiting up to the write timeout for room when the
// queue is at its limit. It returns errSlowConsumer if no room appeared
// in time, ctx.Err() if ctx ended first, and errQueueClosed once the
// queue is closed.
func (q *outQueue) push(ctx context.Context, f outFrame) error {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return errQueueClosed
		}
		if q.head == len(q.frames) || q.bytes+int64(len(f.data)) <= q.max {
			q.frames = append(q.frames, f)
			q.bytes += int64(len(f.data))
			q.mu.Unlock()
			select {
			case q.ready <- struct{}{}:
			default:
			}
			return nil
		}
		if q.space == nil {
			q.space = make(chan struct{})
		}
		space := q.space
		q.mu.Unlock()

		if timer == nil {
			timer = time.NewTimer(q.timeout)
		}
		select {
		case <-space:
		case <-timer.C:
			return errSlowConsumer
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// tryPush appends f only if it fits now, without waiting. Used for the
// shutdown DISCONNECTED, which must not block the shutdown goroutine.
func (q *outQueue) tryPush(f outFrame) bool {
	q.mu.Lock()
	if q.closed || (q.head != len(q.frames) && q.bytes+int64(len(f.data)) > q.max) {
		q.mu.Unlock()
		return false
	}
	q.frames = append(q.frames, f)
	q.bytes += int64(len(f.data))
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
	return true
}

// replaceWith drops every queued frame, queues f as the last frame the
// connection will send, and closes the queue to further pushes. Used to
// disconnect a slow consumer: its backlog is discarded (the client
// resumes from the last channelSerial it received) so the DISCONNECTED
// goes out next.
func (q *outQueue) replaceWith(f outFrame) {
	q.mu.Lock()
	clear(q.frames)
	q.frames = append(q.frames[:0], f)
	q.head = 0
	q.bytes = int64(len(f.data))
	q.closed = true
	q.wakeLocked()
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// pop removes and returns the oldest frame, or false if the queue is
// empty.
func (q *outQueue) pop() (outFrame, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head == len(q.frames) {
		return outFrame{}, false
	}
	f := q.frames[q.head]
	q.frames[q.head] = outFrame{}
	q.head++
	q.bytes -= int64(len(f.data))
	if q.head == len(q.frames) {
		// Empty: rewind, and let a burst's backing array go.
		q.head = 0
		if cap(q.frames) > 32 {
			q.frames = nil
		} else {
			q.frames = q.frames[:0]
		}
	}
	q.wakeLocked()
	return f, true
}

// close drops every queued frame and fails current and future pushes.
func (q *outQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.frames = nil
	q.head = 0
	q.bytes = 0
	q.wakeLocked()
	q.mu.Unlock()
}

// wakeLocked wakes pushers waiting for room. Called with mu held.
func (q *outQueue) wakeLocked() {
	if q.space != nil {
		close(q.space)
		q.space = nil
	}
}

// queuedBytes reports the bytes currently queued.
func (q *outQueue) queuedBytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}
