package realtime

import (
	"context"
	"strconv"
	"time"

	"github.com/ably/ably-server/internal/auth"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// wildcardClientID is the §3.2 marker meaning "the bearer may assume any
// clientId". It is never itself a member identity. Aliased to the
// canonical constant in internal/auth so connection resolution and the
// presence/message rules agree.
const wildcardClientID = auth.WildcardClientID

// teardownLeaveTimeout bounds the synthesised-LEAVE publishes done when a
// connection terminates; the connection's own context is already gone by
// then, so these run on a fresh, bounded context.
const teardownLeaveTimeout = 5 * time.Second

// handlePresence processes an inbound PRESENCE frame: enter / update /
// leave for one or more members (DESIGN.md §12.2). It authorises against
// the attachment's PRESENCE mode, resolves and validates each clientId
// (§12.3), stamps the connectionId, publishes via the channel, and
// ACK/NACKs on the msgSerial.
func (c *connection) handlePresence(ctx context.Context, msg *protocol.ProtocolMessage) {
	msgSerial := msg.GetMsgSerial()
	if msg.GetChannel() == "" || len(msg.Presence) == 0 {
		c.logger.Warn("PRESENCE with empty channel or no payload; rejecting", "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, nil)
		return
	}

	// Presence requires an attachment holding the PRESENCE mode.
	a, ok := c.attachments[msg.GetChannel()]
	if !ok || !a.hasMode(protocol.FlagPresence) {
		c.logger.Warn("PRESENCE without an attached PRESENCE-mode channel; rejecting",
			"channel", msg.GetChannel(), "msgSerial", msgSerial)
		c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
			Message:    "presence requires an attachment with the presence mode",
			Code:       40160,
			StatusCode: 401,
		})
		return
	}

	// Resolve + validate every member's clientId, stamp connectionId, and
	// mint the Ably-form id for genuine (non-synthesized) members.
	for i, p := range msg.Presence {
		cid, ok := resolvePresenceClientID(c.clientID, p.ClientID)
		if !ok {
			c.logger.Warn("PRESENCE clientId rejected", "channel", msg.GetChannel(),
				"connClientId", c.clientID, "msgClientId", p.ClientID, "msgSerial", msgSerial)
			c.enqueueNack(ctx, msgSerial, &protocol.ErrorInfo{
				Message:    "invalid clientId for presence",
				Code:       91000,
				StatusCode: 400,
			})
			return
		}
		p.ClientID = cid
		p.ConnectionID = c.id
		// Stamp the id only when the client did not supply one, mirroring the
		// reference (realtimeattachment.ts presence(): `if (!entry.id)`); a
		// client-supplied id is honoured for idempotency exactly as for
		// messages.
		if p.ID == "" {
			p.ID = presenceID(c.id, msgSerial, i)
		}
	}

	// Track membership for teardown LEAVE and lapse re-entry (DESIGN.md
	// §12.5) on the read goroutine, which is the only goroutine that
	// removes members from the entered set. This is done optimistically
	// before the store completes; a rare store failure would leave a
	// spurious entry whose only effect is a harmless synthesised LEAVE
	// for a member that never durably entered (or, after a lease lapse,
	// an ENTER of it).
	for _, p := range msg.Presence {
		c.recordPresence(msg.GetChannel(), p)
	}

	// The presence store runs on the publish worker so its ACK stays
	// ordered with this connection's message/mutation ACKs (msgSerial is
	// shared across all publish kinds) and is emitted only after a durable
	// commit. Count is 1: an ACK acknowledges one protocol
	// message (this PRESENCE frame), not the members it carries.
	channel := msg.GetChannel()
	presence := msg.Presence
	ch := a.channel
	c.enqueuePublish(ctx, func() {
		c.presMu.Lock()
		_, _, err := ch.PublishPresence(ctx, presence)
		c.presMu.Unlock()
		if err != nil {
			c.logger.Warn("presence publish failed; NACKing", "channel", channel, "msgSerial", msgSerial, "err", err)
			// The retriable storage failures carry their codes, as for a
			// publish: 42910 when the node is at its presence in-flight
			// bound or a lane queue is full, 50003 when a batch failed
			// (DESIGN.md §6.3, §12.5).
			c.nack(ctx, msgSerial, publishErrorInfo(err))
			return
		}
		c.queue(ctx, &protocol.ProtocolMessage{
			Action:    protocol.ActionAck,
			MsgSerial: &msgSerial,
			Count:     1,
		})
	})
}

// presenceID mints the Ably-form id a genuine presence message carries:
// "<connectionId>:<msgSerial>:<index>" — the publishing member's
// connectionId, the inbound PRESENCE frame's msgSerial, and the message's
// position in that frame's presence array (DESIGN.md §12.1). SDKs treat a
// message whose id is prefixed by its own connectionId as non-synthesized
// and order members by (msgSerial, index) rather than by timestamp
// (RTP2b2). msgSerial and index are
// plain (unpadded) integers, matching the reference
// (realtimeattachment.ts: `msgId + ':' + i`, msgId == `connectionId + ':'
// + msgSerial`). Server-fabricated events (teardown/detach LEAVEs) carry
// no id and stay genuinely synthesized (RTP2b1).
func presenceID(connectionID string, msgSerial int64, idx int) string {
	return connectionID + ":" + strconv.FormatInt(msgSerial, 10) + ":" + strconv.Itoa(idx)
}

// resolvePresenceClientID applies the §12.3 rules and returns the
// clientId to stamp on the member, or ok=false if the operation is not
// permitted. connClientID is the connection's resolved clientId:
// "" (anonymous), wildcardClientID (the bearer may assume any clientId),
// or a concrete value.
func resolvePresenceClientID(connClientID, msgClientID string) (string, bool) {
	switch connClientID {
	case "":
		// Anonymous connections cannot enter presence.
		return "", false
	case wildcardClientID:
		// A wildcard bearer must select a concrete clientId; "*" is never
		// itself a member identity.
		if msgClientID == "" || msgClientID == wildcardClientID {
			return "", false
		}
		return msgClientID, true
	default:
		// Concrete clientId: the member may omit it (we stamp ours) or
		// supply the matching value; anything else is rejected.
		if msgClientID == "" || msgClientID == connClientID {
			return connClientID, true
		}
		return "", false
	}
}

// recordPresence updates the per-connection entered set as a presence
// operation is accepted: ENTER/UPDATE/PRESENT store a copy of the
// message (so the store stamping the original, or a later frame, cannot
// change it under a re-entry reading it), LEAVE/ABSENT remove the member.
func (c *connection) recordPresence(channel string, p *protocol.PresenceMessage) {
	c.enteredMu.Lock()
	defer c.enteredMu.Unlock()
	switch p.Action {
	case protocol.PresenceLeave, protocol.PresenceAbsent:
		if set, ok := c.entered[channel]; ok {
			delete(set, p.ClientID)
			if len(set) == 0 {
				delete(c.entered, channel)
			}
		}
	default: // Enter, Update, Present
		set := c.entered[channel]
		if set == nil {
			set = make(map[string]*protocol.PresenceMessage)
			c.entered[channel] = set
		}
		cp := *p
		set[p.ClientID] = &cp
	}
}

// takeEntered removes and returns the members entered on channel.
func (c *connection) takeEntered(channel string) map[string]*protocol.PresenceMessage {
	c.enteredMu.Lock()
	defer c.enteredMu.Unlock()
	set := c.entered[channel]
	delete(c.entered, channel)
	return set
}

// closeEntered removes and returns every member this connection holds
// and closes the set, so neither a lapse re-entry nor a grace hand-over
// touches it after teardown.
func (c *connection) closeEntered() map[string]map[string]*protocol.PresenceMessage {
	c.enteredMu.Lock()
	defer c.enteredMu.Unlock()
	all := c.entered
	c.entered = make(map[string]map[string]*protocol.PresenceMessage)
	c.presenceClosed = true
	return all
}

// adoptPresence hands members held for a dropped connection's grace
// window to this connection, which resumed its connectionId (DESIGN.md
// §12.5): it now owns them, so its own LEAVE paths and a lapse
// re-entry cover them. A member the connection already holds keeps its
// own state. It reports false, adopting nothing, if this connection's
// teardown has already taken its set.
func (c *connection) adoptPresence(members map[string]map[string]*protocol.PresenceMessage) bool {
	c.enteredMu.Lock()
	defer c.enteredMu.Unlock()
	if c.presenceClosed {
		return false
	}
	for channel, held := range members {
		set := c.entered[channel]
		if set == nil {
			set = make(map[string]*protocol.PresenceMessage, len(held))
			c.entered[channel] = set
		}
		for clientID, m := range held {
			if _, ok := set[clientID]; !ok {
				set[clientID] = m
			}
		}
	}
	return true
}

// leaveChannel synthesises a LEAVE for every member this connection
// entered on channel and clears them from the entered set. Used on
// DETACH. Best-effort: a publish failure is logged, not surfaced.
func (c *connection) leaveChannel(ctx context.Context, channel string) {
	c.presMu.Lock()
	defer c.presMu.Unlock()
	set := c.takeEntered(channel)
	if len(set) == 0 {
		return
	}
	c.publishLeaves(ctx, channel, set)
}

// emitTeardownLeaves synthesises LEAVE for every member still held by
// this connection across all channels, on a fresh bounded context (the
// connection's own context is already cancelled). Called once as the
// connection terminates (DESIGN.md §12.5).
func (c *connection) emitTeardownLeaves() {
	c.presMu.Lock()
	defer c.presMu.Unlock()
	all := c.closeEntered()
	if len(all) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), teardownLeaveTimeout)
	defer cancel()
	for channel, set := range all {
		c.publishLeaves(ctx, channel, set)
	}
}

// scheduleTeardownLeaves hands this connection's still-held presence
// members to the server's delayed-leave reaper (DESIGN.md §12.5), which
// synthesises their LEAVE after the grace window unless a connection
// with the same connectionId (a resume) is live by then. Used when the
// connection drops abruptly (not a clean CLOSE), so a resume within the
// window preserves the member.
func (c *connection) scheduleTeardownLeaves() {
	c.presMu.Lock()
	defer c.presMu.Unlock()
	all := c.closeEntered()
	if len(all) == 0 {
		return
	}
	c.srv.scheduleConnectionLeaves(c.id, all)
}

// reenterPresence publishes an ENTER, with its last data, for every
// member this connection holds, after the node's presence lease lapsed
// and other nodes may have reaped them (DESIGN.md §12.5). It holds presMu
// throughout, so a LEAVE the client sends meanwhile is stored after the
// ENTER, never before it. Members still in the store are skipped. It
// returns the members re-entered and those that failed.
func (c *connection) reenterPresence(ctx context.Context) (entered, failed int) {
	c.presMu.Lock()
	defer c.presMu.Unlock()
	c.enteredMu.Lock()
	if c.presenceClosed {
		c.enteredMu.Unlock()
		return 0, 0
	}
	snap := make(map[string][]*protocol.PresenceMessage, len(c.entered))
	for channel, set := range c.entered {
		snap[channel] = reentries(c.id, set)
	}
	c.enteredMu.Unlock()

	for channel, enters := range snap {
		var cm *protocol.ChannelMessage
		ch, err := c.manager.GetChannel(ctx, channel)
		if err == nil {
			// Members still present are skipped (storage.WithPresenceReentry).
			cm, _, err = ch.PublishPresence(storage.WithPresenceReentry(ctx), enters)
		}
		if err != nil {
			c.logger.Warn("presence re-entry failed", "channel", channel, "members", len(enters), "err", err)
			failed += len(enters)
			continue
		}
		if cm != nil {
			entered += len(cm.Presence)
		}
	}
	return entered, failed
}

// reentries returns a server-synthesised ENTER for each member in set,
// stamped with connID and carrying the member's last data, encoding and
// extras (DESIGN.md §12.5).
func reentries(connID string, set map[string]*protocol.PresenceMessage) []*protocol.PresenceMessage {
	enters := make([]*protocol.PresenceMessage, 0, len(set))
	for _, m := range set {
		enters = append(enters, &protocol.PresenceMessage{
			Action:       protocol.PresenceEnter,
			ClientID:     m.ClientID,
			ConnectionID: connID,
			Data:         m.Data,
			Encoding:     m.Encoding,
			Extras:       m.Extras,
		})
	}
	return enters
}

// publishLeaves publishes one LEAVE per member in set onto channel,
// stamped with this connection's id.
func (c *connection) publishLeaves(ctx context.Context, channel string, set map[string]*protocol.PresenceMessage) {
	ch, err := c.manager.GetChannel(ctx, channel)
	if err != nil {
		c.logger.Warn("teardown leave: GetChannel failed", "channel", channel, "err", err)
		return
	}
	if _, _, err := ch.PublishPresence(storage.WithServerPresence(ctx), leavesFor(c.id, set)); err != nil {
		c.logger.Warn("teardown leave publish failed", "channel", channel, "err", err)
	}
}

// leavesFor returns a synthesised LEAVE for each member in set, stamped
// with connID: action, clientId and connectionId, no id (DESIGN.md
// §12.5).
func leavesFor(connID string, set map[string]*protocol.PresenceMessage) []*protocol.PresenceMessage {
	leaves := make([]*protocol.PresenceMessage, 0, len(set))
	for clientID := range set {
		leaves = append(leaves, &protocol.PresenceMessage{
			Action:       protocol.PresenceLeave,
			ClientID:     clientID,
			ConnectionID: connID,
		})
	}
	return leaves
}

// nack queues a NACK for msgSerial, optionally carrying an ErrorInfo.
// Count is 1: a NACK rejects exactly one inbound frame. SDKs correlate
// an ACK/NACK by counting Count messages from MsgSerial, so a missing
// Count (0) leaves the operation uncorrelated and the client hanging.
func (c *connection) nack(ctx context.Context, msgSerial int64, errInfo *protocol.ErrorInfo) {
	c.queue(ctx, &protocol.ProtocolMessage{
		Action:    protocol.ActionNack,
		MsgSerial: &msgSerial,
		Count:     1,
		Error:     errInfo,
	})
}
