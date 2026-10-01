package postgres

import "testing"

func TestParsePresenceLeaseMode(t *testing.T) {
	for in, want := range map[string]string{"": PresenceLeaseNode, "node": PresenceLeaseNode, "member": PresenceLeaseMember} {
		got, err := ParsePresenceLeaseMode(in)
		if err != nil || got != want {
			t.Errorf("ParsePresenceLeaseMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"row", "Node", "members"} {
		if _, err := ParsePresenceLeaseMode(in); err == nil {
			t.Errorf("ParsePresenceLeaseMode(%q) succeeded, want an error", in)
		}
	}
}
