package core

import (
	"fmt"
	"testing"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// BenchmarkPresenceSnapshotBuild measures one rebuild of a 2,000-member
// room's SYNC snapshot, the work an attach does at most once per refresh
// window while the room's members change (DESIGN.md §12.4).
func BenchmarkPresenceSnapshotBuild(b *testing.B) {
	v := memberView{members: make(map[string]*protocol.PresenceMessage)}
	for i := range 2000 {
		cs := fmt.Sprintf("%014d-%03d@abcdef0123", 1790000000000+int64(i), i%7)
		p := &protocol.PresenceMessage{Serial: cs + ":000", Action: protocol.PresenceEnter,
			ConnectionID: fmt.Sprintf("conn%06d", i), ClientID: fmt.Sprintf("client%06d", i), Data: "lg"}
		v.members[storage.MemberKey(p.ConnectionID, p.ClientID)] = p
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = v.build("x")
	}
}
