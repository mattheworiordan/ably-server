package loadgen

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// fakeNTP serves SNTP from a clock skew ahead of the local one.
func fakeNTP(t *testing.T, skew time.Duration, mutate func(resp []byte)) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	go func() {
		buf := make([]byte, 64)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 48 {
				continue
			}
			now := time.Now().Add(skew)
			var resp [48]byte
			resp[0] = 0x24 // version 4, mode 4 (server)
			resp[1] = 2    // stratum
			copy(resp[24:32], buf[40:48])
			s, f := toNTP(now)
			binary.BigEndian.PutUint32(resp[32:], s)
			binary.BigEndian.PutUint32(resp[36:], f)
			binary.BigEndian.PutUint32(resp[40:], s)
			binary.BigEndian.PutUint32(resp[44:], f)
			if mutate != nil {
				mutate(resp[:])
			}
			_, _ = pc.WriteTo(resp[:], addr)
		}
	}()
	return pc.LocalAddr().String()
}

func TestMeasureClockOffsetSign(t *testing.T) {
	// The server is 40 ms ahead of this box, so this box's clock minus the
	// server's is about -40 ms.
	addr := fakeNTP(t, 40*time.Millisecond, nil)
	off, err := MeasureClockOffset(context.Background(), addr, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if off.OffsetUS > -35_000 || off.OffsetUS < -45_000 || off.Samples != 3 || off.Server != addr {
		t.Fatalf("offset %+v, want about -40000 us", off)
	}
	addr = fakeNTP(t, -25*time.Millisecond, nil)
	off, err = MeasureClockOffset(context.Background(), addr, 1, time.Second)
	if err != nil || off.OffsetUS < 20_000 || off.OffsetUS > 30_000 {
		t.Fatalf("offset %+v err %v, want about +25000 us (this box ahead)", off, err)
	}
}

func TestMeasureClockOffsetRejectsBadReplies(t *testing.T) {
	for name, mutate := range map[string]func([]byte){
		"kiss-o-death":    func(r []byte) { r[1] = 0 },
		"wrong mode":      func(r []byte) { r[0] = 0x23 },
		"not our request": func(r []byte) { r[25] ^= 0xff },
	} {
		addr := fakeNTP(t, 0, mutate)
		if _, err := MeasureClockOffset(context.Background(), addr, 1, 300*time.Millisecond); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	// Nothing listening: a timeout, not a hang.
	pc, _ := net.ListenPacket("udp", "127.0.0.1:0")
	addr := pc.LocalAddr().String()
	defer func() { _ = pc.Close() }()
	start := time.Now()
	if _, err := MeasureClockOffset(context.Background(), addr, 2, 150*time.Millisecond); err == nil || time.Since(start) > 2*time.Second {
		t.Errorf("silent server: err %v after %s", err, time.Since(start))
	}
}
