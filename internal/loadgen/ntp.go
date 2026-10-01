package loadgen

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"
)

// ClockOffset is one measurement of a box's clock against an NTP server
// (an SNTP exchange; on AWS the Amazon Time Sync address, which is what
// chrony disciplines the box to).
type ClockOffset struct {
	Server string `json:"server"`
	// OffsetUS is this box's clock minus the server's, in microseconds:
	// positive means the box is ahead.
	OffsetUS int64 `json:"offset_us"`
	// RTTUS is the round-trip delay of the exchange that gave the offset
	// (the one with the smallest delay of the samples).
	RTTUS   int64 `json:"rtt_us"`
	Samples int   `json:"samples"`
	AtUS    int64 `json:"at_us"`
}

const ntpEpochOffset = 2208988800 // seconds from 1900 to 1970

func toNTP(t time.Time) (sec, frac uint32) {
	sec = uint32(t.Unix() + ntpEpochOffset)
	frac = uint32(uint64(t.Nanosecond()) << 32 / 1_000_000_000)
	return sec, frac
}

func fromNTP(sec, frac uint32) time.Time {
	ns := int64(uint64(frac) * 1_000_000_000 >> 32)
	return time.Unix(int64(sec)-ntpEpochOffset, ns)
}

// MeasureClockOffset asks server (host:port, UDP) for the time samples
// times and returns the offset from the exchange with the smallest delay,
// which is the least affected by queueing. Samples that time out or fail
// are skipped; it fails if none succeed.
func MeasureClockOffset(ctx context.Context, server string, samples int, timeout time.Duration) (ClockOffset, error) {
	if samples < 1 {
		samples = 1
	}
	var best ClockOffset
	var lastErr error
	got := 0
	for range samples {
		if ctx.Err() != nil {
			break
		}
		off, rtt, err := sntpOnce(ctx, server, timeout)
		if err != nil {
			lastErr = err
			continue
		}
		if got == 0 || rtt < best.RTTUS {
			best = ClockOffset{Server: server, OffsetUS: off, RTTUS: rtt}
		}
		got++
	}
	if got == 0 {
		if lastErr == nil {
			lastErr = ctx.Err()
		}
		return ClockOffset{}, fmt.Errorf("ntp %s: %w", server, lastErr)
	}
	best.Samples = got
	best.AtUS = time.Now().UnixMicro()
	return best, nil
}

// sntpOnce does one SNTP exchange and returns the offset (local minus
// server) and the round-trip delay, both in microseconds.
func sntpOnce(ctx context.Context, server string, timeout time.Duration) (offsetUS, rttUS int64, err error) {
	var d net.Dialer
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := d.DialContext(ctx, "udp", server)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	var req [48]byte
	req[0] = 0x23 // LI 0, version 4, mode 3 (client)
	t1 := time.Now()
	s1, f1 := toNTP(t1)
	binary.BigEndian.PutUint32(req[40:], s1)
	binary.BigEndian.PutUint32(req[44:], f1)
	if _, err := conn.Write(req[:]); err != nil {
		return 0, 0, err
	}
	var resp [64]byte
	n, err := conn.Read(resp[:])
	t4 := time.Now()
	if err != nil {
		return 0, 0, err
	}
	if n < 48 {
		return 0, 0, errors.New("short NTP response")
	}
	if mode := resp[0] & 7; mode != 4 && mode != 5 {
		return 0, 0, fmt.Errorf("NTP response in mode %d", mode)
	}
	if resp[1] == 0 {
		return 0, 0, errors.New("NTP kiss-o'-death (stratum 0)")
	}
	// The server echoes our transmit time as its originate time: a reply
	// that does not is not to our request.
	if binary.BigEndian.Uint32(resp[24:]) != s1 || binary.BigEndian.Uint32(resp[28:]) != f1 {
		return 0, 0, errors.New("NTP response does not echo the request")
	}
	t2 := fromNTP(binary.BigEndian.Uint32(resp[32:]), binary.BigEndian.Uint32(resp[36:]))
	t3 := fromNTP(binary.BigEndian.Uint32(resp[40:]), binary.BigEndian.Uint32(resp[44:]))
	// Standard SNTP: offset = server minus local = ((t2-t1)+(t3-t4))/2.
	serverMinusLocal := (t2.Sub(t1) + t3.Sub(t4)) / 2
	delay := t4.Sub(t1) - t3.Sub(t2)
	return -serverMinusLocal.Microseconds(), delay.Microseconds(), nil
}
