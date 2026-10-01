package loadgen

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

// HostCPU is a reading of the box's cumulative CPU time from /proc/stat,
// in jiffies. Two readings give the box's busy fraction between them.
// A generator or publisher box that is saturated produces latency numbers
// that measure the box, so the conductor reads each agent's box at the
// start and end of the hold.
type HostCPU struct {
	// Busy is user+nice+system+irq+softirq+steal; Total adds idle and
	// iowait.
	Busy  uint64 `json:"busy"`
	Total uint64 `json:"total"`
	CPUs  int    `json:"cpus"`
}

// ParseProcStat reads the aggregate "cpu" line and counts the "cpuN"
// lines of /proc/stat.
func ParseProcStat(r io.Reader) (HostCPU, error) {
	var h HostCPU
	got := false
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 0 || !strings.HasPrefix(f[0], "cpu") {
			continue
		}
		if f[0] != "cpu" {
			h.CPUs++
			continue
		}
		if len(f) < 8 {
			return HostCPU{}, errors.New("short cpu line in /proc/stat")
		}
		var v [10]uint64
		for i := 1; i < len(f) && i <= 10; i++ {
			n, err := strconv.ParseUint(f[i], 10, 64)
			if err != nil {
				return HostCPU{}, err
			}
			v[i-1] = n
		}
		// user nice system idle iowait irq softirq steal (guest is
		// already inside user).
		idle := v[3] + v[4]
		h.Busy = v[0] + v[1] + v[2] + v[5] + v[6] + v[7]
		h.Total = h.Busy + idle
		got = true
	}
	if err := sc.Err(); err != nil {
		return HostCPU{}, err
	}
	if !got {
		return HostCPU{}, errors.New("no cpu line in /proc/stat")
	}
	return h, nil
}

// ReadHostCPU reads /proc/stat (Linux only).
func ReadHostCPU() (HostCPU, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return HostCPU{}, err
	}
	defer func() { _ = f.Close() }()
	return ParseProcStat(f)
}

// BusyFraction is the share of CPU time spent busy between two readings
// (0 when no time passed).
func BusyFraction(from, to HostCPU) float64 {
	dt := float64(to.Total) - float64(from.Total)
	if dt <= 0 {
		return 0
	}
	return (float64(to.Busy) - float64(from.Busy)) / dt
}
