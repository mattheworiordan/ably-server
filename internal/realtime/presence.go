package realtime

import (
	"context"
	"errors"
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

	// The presence store runs on the publish worker so its ACK stays
	// ordered with this connection's message/mutation ACKs (msgSerial is
	// shared across all publish kinds) and is emitted only after a durable
	// commit. Count is 1: an ACK acknowledges one protocol
	// message (this PRESENCE frame), not the members it carries.
	//
	// The entered set (teardown LEAVE, grace LEAVE and lapse re-entry,
	// DESIGN.md §12.5) is updated from the store's answer, under presMu
	// with the write, so it reflects what the store holds: a committed
	// operation is recorded as such (recordPresence), one the store
	// refused before storing anything is not recorded, and one whose
	// outcome is unknown is recorded as uncertain (recordUncertain), so a
	// LEAVE follows it but a lapse re-entry does not bring it back. A
	// DETACH or a re-entry takes presMu too, so it sees the set as of the
	// last answered write.
	//
	// The store call runs on a context that the connection's teardown does
	// not cancel, bounded by presenceWriteTimeout: teardown cancels the
	// connection's context while a batch may be in flight, and a write the
	// caller abandoned can still commit, which would leave a member no
	// LEAVE path knows of. Teardown waits for this worker (loops.Wait)
	// before it takes the entered set, so the answer is recorded first.
	channel := msg.GetChannel()
	presence := msg.Presence
	ch := a.channel
	timeout := c.srv.presenceWriteTimeoutOrDefault()
	c.enqueuePublish(ctx, func() {
		c.presMu.Lock()
		// The records are built from copies taken before the store sees
		// the messages: a batch the caller stopped waiting for still
		// stamps them (serial, timestamp) while it commits.
		recs := make([]protocol.PresenceMessage, len(presence))
		for i, p := range presence {
			recs[i] = *p
		}
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		_, _, err := ch.PublishPresence(wctx, presence)
		cancel()
		switch {
		case err == nil:
			for i := range recs {
				c.recordPresence(channel, &recs[i])
			}
		case presenceRefused(err):
			// Nothing was stored: the set is unchanged.
		default:
			for i := range recs {
				c.recordUncertain(channel, &recs[i])
			}
		}
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

// DefaultPresenceWriteTimeout bounds one client presence write on the
// publish worker (handlePresence). The write runs on a context the
// connection's teardown does not cancel, so this is what bounds how long
// a stalled store holds up that teardown.
const DefaultPresenceWriteTimeout = 10 * time.Second

// presenceWriteTimeoutOrDefault returns the server's presence write
// bound, DefaultPresenceWriteTimeout unless a test set one.
func (s *Server) presenceWriteTimeoutOrDefault() time.Duration {
	if s == nil || s.presenceWriteTimeout <= 0 {
		return DefaultPresenceWriteTimeout
	}
	return s.presenceWriteTimeout
}

// presenceRefused reports whether a presence write failed before the
// store wrote anything: the backend turned it away (a full lane queue or
// the presence in-flight bound, storage.ErrOverloaded), could not store
// the channel's name, or refused a client-supplied id. Every other
// failure, a timeout or a cancelled wait, storage.ErrUnavailable ("most
// likely not stored"), is treated as possibly committed (DESIGN.md
// §12.5).
func presenceRefused(err error) bool {
	return errors.Is(err, storage.ErrOverloaded) || errors.Is(err, storage.ErrInvalidChannelName) ||
		errors.Is(err, storage.ErrInvalidMessageID)
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

// recordPresence updates the per-connection entered set once a presence
// operation has committed: ENTER/UPDATE/PRESENT store a copy of the
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

// uncertainAction marks an entered-set record whose presence is unknown
// (recordUncertain): the member may or may not be in the store. Every
// LEAVE path covers it (a LEAVE of a member that is absent stores
// nothing, though subscribers see a LEAVE for a member they never saw
// enter), and a lapse re-entry skips it (reentries), since the client
// was told the operation failed. A committed operation replaces the
// record, and a committed record is preferred to an uncertain one when
// two sets merge (preferRecord).
const uncertainAction = protocol.PresenceAbsent

// preferRecord reports whether next should replace held, the record a
// set already has for the same member, when two sets merge (a grace
// hand-over, a second drop in the window): a committed record is kept
// over an uncertain one, otherwise the newer wins.
func preferRecord(held, next *protocol.PresenceMessage) bool {
	return held == nil || held.Action == uncertainAction || next.Action != uncertainAction
}

// recordUncertain updates the entered set for a presence operation whose
// outcome is unknown (DESIGN.md §12.5): the write timed out, or failed
// in a way that does not prove nothing was stored. An ENTER, UPDATE or
// PRESENT for a member the set does not hold records it as uncertain, so
// the connection's LEAVE paths still cover it if the write committed; a
// member the set already holds is present whatever the outcome and keeps
// its record. A LEAVE or ABSENT turns a held member uncertain: it may
// have left, so it must not be re-entered, but it is still left.
func (c *connection) recordUncertain(channel string, p *protocol.PresenceMessage) {
	c.enteredMu.Lock()
	defer c.enteredMu.Unlock()
	set := c.entered[channel]
	held := set[p.ClientID]
	switch p.Action {
	case protocol.PresenceLeave, protocol.PresenceAbsent:
		if held != nil {
			cp := *held
			cp.Action = uncertainAction
			set[p.ClientID] = &cp
		}
	default: // Enter, Update, Present
		if held != nil {
			return
		}
		if set == nil {
			set = make(map[string]*protocol.PresenceMessage)
			c.entered[channel] = set
		}
		cp := *p
		cp.Action = uncertainAction
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
// touches it after teardown. owed reports a re-entry of adopted members
// that was due and had not taken its snapshot yet (reentryOwed).
func (c *connection) closeEntered() (all map[string]map[string]*protocol.PresenceMessage, owed bool) {
	c.enteredMu.Lock()
	defer c.enteredMu.Unlock()
	all, owed = c.entered, c.reentryOwed > 0
	c.entered = make(map[string]map[string]*protocol.PresenceMessage)
	c.presenceClosed = true
	c.reentryOwed = 0
	return all, owed
}

// adoptPresence hands members held for a dropped connection's grace
// window to this connection, which resumed its connectionId (DESIGN.md
// §12.5): it now owns them, so its own LEAVE paths and a lapse
// re-entry cover them. A member the connection already holds keeps its
// own state. It reports false, adopting nothing, if this connection's
// teardown has already taken its set.
//
// owe records that the connection must re-enter what it adopted
// (adoptGraceLocked): until reenterMembers has taken its snapshot, the
// duty passes with the members to a grace entry if this connection drops
// first (closeEntered).
func (c *connection) adoptPresence(members map[string]map[string]*protocol.PresenceMessage, owe bool) bool {
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
			// A record the connection already holds is its own, newer
			// state, unless it is uncertain and the adopted one committed.
			if cur, ok := set[clientID]; !ok || (cur.Action == uncertainAction && m.Action != uncertainAction) {
				set[clientID] = m
			}
		}
	}
	if owe {
		c.reentryOwed++
	}
	return true
}

// leaveChannel synthesises a LEAVE for every member this connection
// entered on channel and clears them from the entered set. Run on the
// publish worker (leaveChannelOrdered). A LEAVE that fails is logged, and
// its members go back into the set as uncertain (recordUncertain), so the
// connection's teardown still leaves them.
func (c *connection) leaveChannel(ctx context.Context, channel string) {
	c.presMu.Lock()
	defer c.presMu.Unlock()
	set := c.takeEntered(channel)
	if len(set) == 0 {
		return
	}
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.srv.presenceWriteTimeoutOrDefault())
	defer cancel()
	if err := c.publishLeaves(wctx, channel, set); err != nil {
		c.restoreUncertain(channel, set)
	}
}

// restoreUncertain puts members whose LEAVE failed back into the entered
// set as uncertain (recordUncertain), unless the set holds them again.
func (c *connection) restoreUncertain(channel string, members map[string]*protocol.PresenceMessage) {
	c.enteredMu.Lock()
	defer c.enteredMu.Unlock()
	if c.presenceClosed {
		return
	}
	set := c.entered[channel]
	if set == nil {
		set = make(map[string]*protocol.PresenceMessage, len(members))
		c.entered[channel] = set
	}
	for clientID, m := range members {
		if _, ok := set[clientID]; ok {
			continue
		}
		cp := *m
		cp.Action = uncertainAction
		set[clientID] = &cp
	}
}

// leaveChannelOrdered runs leaveChannel for a DETACH (or a capability
// change that ends the attachment) on the publish worker and waits for
// it, so it follows every presence write this connection queued before
// it (DESIGN.md §12.5). Run on the read goroutine, before it stops the
// attachment and queues DETACHED: an ENTER still queued behind an
// earlier publish would otherwise commit after the leave and leave its
// member present on a channel the client has detached from. If the
// connection is ending first, its teardown leaves the members instead.
func (c *connection) leaveChannelOrdered(ctx context.Context, channel string) {
	done := make(chan struct{})
	if !c.enqueuePublish(ctx, func() {
		defer close(done)
		c.leaveChannel(ctx, channel)
	}) {
		return
	}
	// ctx is cancelled only once the read loop has returned, so in
	// practice this waits for the worker; the second case keeps the wait
	// tied to the connection's life all the same.
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// emitTeardownLeaves synthesises LEAVE for every member still held by
// this connection across all channels, on a fresh bounded context (the
// connection's own context is already cancelled). Called once as the
// connection terminates (DESIGN.md §12.5).
func (c *connection) emitTeardownLeaves() {
	c.presMu.Lock()
	defer c.presMu.Unlock()
	all, _ := c.closeEntered() // leaving them: nothing to re-enter
	if len(all) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), teardownLeaveTimeout)
	defer cancel()
	for channel, set := range all {
		_ = c.publishLeaves(ctx, channel, set) // logged; the connection is gone
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
	all, owed := c.closeEntered()
	if len(all) == 0 {
		return
	}
	c.srv.scheduleConnectionLeaves(c.id, all, owed)
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
	return c.publishReentries(ctx, snap)
}

// reenterMembers re-enters, under presMu, the members of the given set
// (channel -> clientId) that this connection still holds, each with the
// connection's own record of it, as reenterPresence does for the whole
// entered set. Used for members adopted from a grace entry during a
// re-entry pass. The connection may be running by then (a hand-over at
// the grace window's end, or this running after CONNECTED), so a member
// its client has since left is not brought back, and one it has updated
// is re-entered with the update, not with the grace entry's copy.
func (c *connection) reenterMembers(ctx context.Context, members map[string]map[string]*protocol.PresenceMessage) (entered, failed int) {
	c.presMu.Lock()
	defer c.presMu.Unlock()
	c.enteredMu.Lock()
	if c.presenceClosed {
		c.enteredMu.Unlock()
		return 0, 0
	}
	if c.reentryOwed > 0 {
		c.reentryOwed-- // the snapshot below discharges one adoption's duty
	}
	snap := make(map[string][]*protocol.PresenceMessage, len(members))
	for channel, set := range members {
		held := c.entered[channel]
		cur := make(map[string]*protocol.PresenceMessage, len(set))
		for clientID := range set {
			if m := held[clientID]; m != nil {
				cur[clientID] = m
			}
		}
		snap[channel] = reentries(c.id, cur)
	}
	c.enteredMu.Unlock()
	return c.publishReentries(ctx, snap)
}

// publishReentries publishes one server-synthesised re-entry per channel
// (storage.WithPresenceReentry: members still present are skipped).
// Called with presMu held.
func (c *connection) publishReentries(ctx context.Context, snap map[string][]*protocol.PresenceMessage) (entered, failed int) {
	for channel, enters := range snap {
		if len(enters) == 0 {
			continue // every member uncertain (reentries)
		}
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
// extras (DESIGN.md §12.5). An uncertain record (recordUncertain) is
// left out.
func reentries(connID string, set map[string]*protocol.PresenceMessage) []*protocol.PresenceMessage {
	enters := make([]*protocol.PresenceMessage, 0, len(set))
	for _, m := range set {
		if m.Action == uncertainAction {
			continue // the client was told the operation failed
		}
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
// stamped with this connection's id. A failure is logged and returned.
func (c *connection) publishLeaves(ctx context.Context, channel string, set map[string]*protocol.PresenceMessage) error {
	ch, err := c.manager.GetChannel(ctx, channel)
	if err != nil {
		c.logger.Warn("presence leave: GetChannel failed", "channel", channel, "err", err)
		return err
	}
	if _, _, err := ch.PublishPresence(storage.WithServerPresence(ctx), leavesFor(c.id, set)); err != nil {
		c.logger.Warn("presence leave publish failed", "channel", channel, "err", err)
		return err
	}
	return nil
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
