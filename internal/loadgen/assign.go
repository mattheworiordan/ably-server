package loadgen

import (
	"strconv"
)

// AttachPlan is one attachment a subscriber connection makes.
type AttachPlan struct {
	Channel string
	Class   int
	Sampled bool
}

// ConnPlan is one subscriber connection and the channels it attaches.
type ConnPlan struct {
	// Global is the connection's index across all subscriber processes;
	// it picks the node (Global mod nodes) so load spreads evenly.
	Global int
	Attach []AttachPlan
	// ChurnSlot: the connection holds one churn channel, replaced at the
	// channel-open rate during the hold.
	ChurnSlot bool
}

// SubscriberSlice returns the connections owned by subscriber process
// index of count. Attachments are enumerated class by class, channel by
// channel, subscriber slot by slot; global attachment a goes to
// connection a mod Connections, and connection c belongs to process
// c mod count. Because a channel's subscribers are consecutive
// attachments and no channel has more subscribers than there are
// connections, no connection attaches the same channel twice.
func (p *Plan) SubscriberSlice(index, count int) []ConnPlan {
	if count < 1 || index < 0 || index >= count {
		return nil
	}
	conns := make([]ConnPlan, 0, p.Connections/count+1)
	for c := index; c < p.Connections; c += count {
		conns = append(conns, ConnPlan{Global: c, ChurnSlot: p.ChurnSlot(c)})
	}
	C := int64(p.Connections)
	for ci := range p.Classes {
		rc := &p.Classes[ci]
		a := rc.firstAttachment
		for j := 0; j < rc.ChannelCount; j++ {
			n := int64(p.Subscribers(rc, j))
			if n == 0 {
				continue
			}
			var name string
			var sampled bool
			named := false
			// Slots of this channel owned by this process: connections
			// (a+k) mod C for k in [0, n) with conn mod count == index.
			for k := int64(0); k < n; k++ {
				conn := (a + k) % C
				if int(conn%int64(count)) != index {
					continue
				}
				if !named {
					name = p.ChannelName(rc, j)
					sampled = p.Sampled(rc, j)
					named = true
				}
				li := int(conn) / count
				conns[li].Attach = append(conns[li].Attach, AttachPlan{Channel: name, Class: ci, Sampled: sampled})
			}
			a += n
		}
	}
	return conns
}

// StreamPlan is one sequential publish stream a publisher process owns.
type StreamPlan struct {
	Channel string
	PubID   string
	Class   int
	Sampled bool
}

// ClassStreams is a publisher process's streams in one class, all at the
// same per-stream rate.
type ClassStreams struct {
	Class      int
	Rate       float64 // messages/s per stream
	MsgBytes   int
	Streams    []StreamPlan
	TotalShare float64 // this process's messages/s for the class
}

// PublisherSlice returns the streams of every class published with
// transport ("rest" or "realtime") owned by publisher process index of
// count. Stream l of channel ch is owned by process
// (hash(ch) + l) mod count.
func (p *Plan) PublisherSlice(transport string, index, count int) []ClassStreams {
	if count < 1 || index < 0 || index >= count {
		return nil
	}
	var out []ClassStreams
	for ci := range p.Classes {
		rc := &p.Classes[ci]
		if rc.Publisher != transport || rc.RatePerChannel <= 0 {
			continue
		}
		cs := ClassStreams{Class: ci, Rate: rc.RatePerChannel / float64(rc.StreamCount), MsgBytes: rc.MsgBytes}
		for j := 0; j < rc.ChannelCount; j++ {
			name := p.ChannelName(rc, j)
			h := hash64(name)
			var sampled, sampledKnown bool
			for l := 0; l < rc.StreamCount; l++ {
				if int((h+uint64(l))%uint64(count)) != index {
					continue
				}
				if !sampledKnown {
					sampled = p.Sampled(rc, j)
					sampledKnown = true
				}
				cs.Streams = append(cs.Streams, StreamPlan{Channel: name, PubID: p.PubID(l), Class: ci, Sampled: sampled})
			}
		}
		cs.TotalShare = cs.Rate * float64(len(cs.Streams))
		out = append(out, cs)
	}
	return out
}

// PresenceMember is one presence member a presence process owns.
type PresenceMember struct {
	Global   int
	Channel  string
	ClientID string
}

// PresenceSlice returns the presence members owned by presence process
// index of count: member g (channel g / membersPerChannel) belongs to
// process g mod count.
func (p *Plan) PresenceSlice(index, count int) []PresenceMember {
	if !p.Presence.Enabled || count < 1 || index < 0 || index >= count {
		return nil
	}
	m := p.Presence.MembersPerChannel
	total := p.Presence.Channels * m
	var out []PresenceMember
	for g := index; g < total; g += count {
		out = append(out, PresenceMember{
			Global:   g,
			Channel:  p.Prefix + "-presence-" + strconv.Itoa(g/m),
			ClientID: p.RunTag + "m" + strconv.Itoa(g),
		})
	}
	return out
}
