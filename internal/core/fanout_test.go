package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStreamMemoBuildsOncePerEntry checks the per-entry memo the realtime
// layer keeps each cm's encoded frame on (DESIGN.md §5.1): every Stream on
// the channel gets the value the first build made for a key, a second key
// builds separately, and the next entry starts empty.
func TestStreamMemoBuildsOncePerEntry(t *testing.T) {
	c := newReadyChannel("test", "000")
	const streams = 50
	ss := make([]*Stream, streams)
	for i := range ss {
		s, err := c.Attach(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ss[i] = s
	}
	before := time.Now()
	c.Append(newCM("001", "m1"))
	c.Append(newCM("002", "m2"))

	var builds atomic.Int32
	build := func(v string) func() any {
		return func() any { builds.Add(1); return v }
	}
	var wg sync.WaitGroup
	for _, s := range ss {
		wg.Go(func() {
			if _, err := s.Next(context.Background()); err != nil {
				t.Error(err)
				return
			}
			if at := s.AppendedAt(); at.Before(before) {
				t.Errorf("AppendedAt = %v, before the append at %v", at, before)
			}
			if v := s.Memo("json", build("a")); v != "a" {
				t.Errorf("Memo(json) = %v, want a", v)
			}
			if v := s.Memo("msgpack", build("b")); v != "b" {
				t.Errorf("Memo(msgpack) = %v, want b", v)
			}
		})
	}
	wg.Wait()
	if n := builds.Load(); n != 2 {
		t.Fatalf("builds for the first entry = %d, want 2 (one per key)", n)
	}

	for _, s := range ss {
		if _, err := s.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
		if v := s.Memo("json", build("c")); v != "c" {
			t.Fatalf("Memo(json) on the second entry = %v, want c", v)
		}
	}
	if n := builds.Load(); n != 3 {
		t.Fatalf("builds after the second entry = %d, want 3", n)
	}
}

// TestAppendWakesStreamsOutsideLock checks that a stream woken by an
// Append can Attach and read the channel's state at once: the wake-ups
// run after Append releases mu (DESIGN.md §5.1), and every stream sees
// the cms in list order even when Appends race their wake-ups.
func TestAppendWakesStreamsOutsideLock(t *testing.T) {
	c := newReadyChannel("test", "000")
	const streams = 20
	const cms = 200
	var wg sync.WaitGroup
	for range streams {
		s, err := c.Attach(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			for i := 1; i <= cms; i++ {
				cm, err := s.Next(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if want := fmt.Sprintf("%06d", i); cm.ChannelSerial != want {
					t.Errorf("stream got %s, want %s", cm.ChannelSerial, want)
					return
				}
				// Attach takes mu: it must not wait for the wake-up of the
				// other streams.
				extra, err := c.Attach(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				extra.Close()
			}
		})
	}
	for i := 1; i <= cms; i++ {
		c.Append(newCM(fmt.Sprintf("%06d", i), "m"))
	}
	wg.Wait()
	if n := c.subs.Load(); n != streams {
		t.Fatalf("subs = %d, want %d (each extra Stream closed)", n, streams)
	}
}

// BenchmarkAppendFanout measures one Append on a channel with many
// parked Streams: the time Append itself takes (it readies every parked
// goroutine) and the time until every Stream has returned the cm.
func BenchmarkAppendFanout(b *testing.B) {
	for _, n := range []int{1000, 20000} {
		b.Run(fmt.Sprintf("streams=%d", n), func(b *testing.B) {
			c := newReadyChannel("bench", "000000")
			var woke sync.WaitGroup
			start := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for range n {
				s, err := c.Attach(ctx)
				if err != nil {
					b.Fatal(err)
				}
				go func() {
					<-start
					for {
						if _, err := s.Next(ctx); err != nil {
							return
						}
						woke.Done()
					}
				}()
			}
			close(start)
			time.Sleep(100 * time.Millisecond) // let every goroutine park
			var appendNs, allNs time.Duration
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				woke.Add(n)
				t0 := time.Now()
				c.Append(newCM(fmt.Sprintf("%06d", i+1), "m"))
				appendNs += time.Since(t0)
				woke.Wait()
				allNs += time.Since(t0)
			}
			b.ReportMetric(float64(appendNs.Microseconds())/float64(b.N), "append-µs/op")
			b.ReportMetric(float64(allNs.Microseconds())/float64(b.N), "all-woken-µs/op")
		})
	}
}

// TestEntryWakeSlots checks the wake slots (entry.wait, wakeAll): a
// waiter on any slot wakes when next is linked, and a slot first asked
// for after the wake-up is already closed, so a Stream that arrives late
// never parks.
func TestEntryWakeSlots(t *testing.T) {
	for _, streams := range []int64{0, 1, 100, 5000, 1 << 30} {
		e := newEntry(nil, streams)
		n := len(e.wake)
		if n < 1 || n > maxWakeSlots || n&(n-1) != 0 {
			t.Fatalf("streams=%d: %d slots, want a power of two in [1, %d]", streams, n, maxWakeSlots)
		}
		if streams <= maxWakeSlots*streamsPerWakeSlot && int64(n)*streamsPerWakeSlot < streams {
			t.Fatalf("streams=%d: %d slots hold fewer than %d each", streams, n, streamsPerWakeSlot)
		}
	}

	c := newChannel("test")
	c.tail = newEntry(nil, 16*streamsPerWakeSlot) // 16 slots
	head := c.tail
	var wg sync.WaitGroup
	for slot := range uint64(2 * len(head.wake)) {
		ch := head.wait(slot)
		wg.Go(func() { <-ch })
	}
	c.Append(newCM("001", "m1"))
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a waiter did not wake")
	}
	for slot := range uint64(len(head.wake)) {
		if !isClosed(head.wait(slot)) {
			t.Fatalf("slot %d: wait after the wake-up returned an open channel", slot)
		}
	}
	if isClosed(head.next.wait(3)) {
		t.Fatal("the new tail's slot is closed before its next is linked")
	}
}
