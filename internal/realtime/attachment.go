package realtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ably/ably-server/internal/core"
	"github.com/ably/ably-server/internal/logging"
	"github.com/ably/ably-server/internal/metrics"
	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage"
)

// ParamAppendMode is the ATTACH channel param that selects how a
// subscriber receives streamed appends (DESIGN.md §13.3); AppendModeFull
// is its only value, opting the subscriber into full rolled-up versions
// instead of incremental append deltas. Names match Ably's reference.
const (
	ParamAppendMode = "appendMode"
	AppendModeFull  = "full"
)

// defaultReplayCap caps the number of channel messages (ChannelMessage,
// one per publish, each carrying one or more Messages) replayed per ATTACH
// (resume or rewind), not the number of Messages: the replay reads the
// log in channel-message units, so a client that missed publishes of
// several Messages each receives more than this many Messages. A client
// that has missed more than this many channel messages still receives the
// most recent defaultReplayCap of them, with ATTACHED.Error set and
// FlagResumed cleared so the SDK can surface a discontinuity.
const defaultReplayCap = 1000

// attachment is the (connection, channel) pair on this node. It owns
// one goroutine that walks the channel's Stream and pushes frames
// (ATTACHED, then MESSAGE per published message) onto the connection's
// outbound chan. The goroutine exits when the attachment's context is
// cancelled — either because the connection is closing, or because the
// client sent a DETACH.
type attachment struct {
	channelName string
	channel     *core.Channel
	stream      *core.Stream
	// resumeFrom: client-supplied channelSerial, "" for a fresh attach.
	// Takes precedence over rewindParam (per DESIGN §4.3).
	resumeFrom string
	// rewindParam: raw value of params["rewind"], parsed by ParseRewind.
	// Ignored when resumeFrom is non-empty.
	rewindParam string
	// params: full ATTACH params, echoed in ATTACHED.params.
	params map[string]string
	// mu guards the fields a repeat ATTACH mutates in place:
	// modes, params, appendModeFull, and curSerial. handleAttach writes
	// them from the connection read loop while the run goroutine reads
	// them on the forward path (and the read loop itself consults modes
	// on the publish/presence/annotation paths), so all access is
	// serialised here.
	mu sync.Mutex
	// modes is the effective channel-mode set for this attachment: the
	// requested modes intersected with the capability-permitted set,
	// resolved by the connection before the attachment is created
	// (DESIGN.md §3.1, §4.2). Gates frame flow — SUBSCRIBE for MESSAGE,
	// PRESENCE_SUBSCRIBE for PRESENCE/SYNC. Guarded by mu.
	modes int64
	// requestedModes is the channel-mode set originally requested at
	// ATTACH, before capability intersection. recheckCapability
	// re-derives modes from this (not from the already-narrowed modes) on
	// a live capability change, so a later widening reauth can restore a
	// mode an earlier narrowing dropped — matching the reference server's
	// permittedModeAndParams, which always recomputes from the request.
	// Guarded by mu.
	requestedModes int64
	// curSerial is the channelSerial the run goroutine has advanced the
	// live cursor to (the attach point until the first live cm). A repeat
	// ATTACH replies ATTACHED at this position (DESIGN.md §4.1). Guarded
	// by mu.
	curSerial string
	replayCap int
	// out queues a frame on the owning connection's bounded outbound
	// queue (connection.queue); false means the frame was not queued
	// and the attachment should stop.
	out func(context.Context, *protocol.ProtocolMessage) bool
	// outShared queues a frame whose encoding is shared across
	// connections through memo (connection.queueShared); nil sends
	// through out.
	outShared func(context.Context, *protocol.ProtocolMessage, memoizer) bool
	// outObserved is outShared (or out, for a nil memoizer) with a
	// callback the write loop runs once the frame is on the wire
	// (connection.queueObserved); the attach times its ATTACHED and SYNC
	// frames with it. Nil falls back to outShared and out.
	outObserved func(context.Context, *protocol.ProtocolMessage, memoizer, func(bytes int)) bool
	// received is when the connection read the ATTACH that created this
	// attachment, the start of ably_attach_seconds (DESIGN.md §10). Zero
	// for attachments made outside a connection (tests). Set before run.
	received time.Time
	// sampled is set on one connection's attachments in
	// metrics.DeliverySampleEvery: they record the fan-out time of each
	// live frame (ably_delivery_fanout_seconds, DESIGN.md §10).
	sampled bool
	// connID is the owning connection's id, and echo its `echo` setting.
	// When echo is false the fan-out skips message cms this connection
	// published itself (DESIGN.md §2.1).
	connID  string
	echo    bool
	metrics *metrics.Metrics
	logger  *logging.Logger

	// appendModeFull is set when the subscriber requested
	// appendMode=full: it always receives full rolled-up versions rather
	// than incremental append deltas (DESIGN.md §13.3). Guarded by mu.
	appendModeFull bool
	// seen tracks the message identity serials this attachment has
	// delivered since attach, so the first append for a not-yet-seen
	// message is a full aggregated update and later appends arrive as
	// deltas (DESIGN.md §13.3). It is bounded (seenSet); a serial it no
	// longer holds gets the full version, which is always correct.
	// Touched only by the run goroutine.
	seen seenSet
	// trackCreates is set when the channel's namespace has mutable
	// messages enabled, so a message delivered without an append delta
	// (a create) may later receive appends and is recorded in seen. On
	// any other channel only messages that carry an append delta are
	// recorded, so ordinary traffic records nothing (DESIGN.md §13.3).
	// Set before run starts.
	trackCreates bool

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// newAttachment derives a cancellable context from parent and returns
// an attachment ready to be run. requestedModes is the channel modes the
// ATTACH asked for, before capability intersection; modes is the
// already-resolved effective set (requestedModes ∩ capability-permitted).
// resumeFrom may be empty for a fresh attach; if non-empty, run() will
// replay the gap before entering the live Stream loop. rewindParam takes
// effect only when resumeFrom is empty — channelSerial wins (DESIGN §4.3).
func newAttachment(parent context.Context, name string, channel *core.Channel, stream *core.Stream, resumeFrom string, attachResume bool, requestedModes, modes int64, params map[string]string, out func(context.Context, *protocol.ProtocolMessage) bool, connID string, echo bool, m *metrics.Metrics, logger *logging.Logger) *attachment {
	ctx, cancel := context.WithCancel(parent)
	// rewind is suppressed when the attach is a resume — either a supplied
	// channelSerial cursor or the ATTACH_RESUME flag (RTL4j) — so a
	// continuation does not replay history the client has already seen
	// (DESIGN.md §4.3, matching the reference's isResume gate).
	rewind := ""
	if resumeFrom == "" && !attachResume {
		rewind = params["rewind"]
	}
	return &attachment{
		channelName:    name,
		channel:        channel,
		stream:         stream,
		resumeFrom:     resumeFrom,
		rewindParam:    rewind,
		params:         params,
		modes:          modes,
		requestedModes: requestedModes,
		replayCap:      defaultReplayCap,
		out:            out,
		connID:         connID,
		echo:           echo,
		metrics:        m,
		logger:         logger,
		appendModeFull: params[ParamAppendMode] == AppendModeFull,
		seen:           newSeenSet(DefaultAttachmentSeenMax),
		trackCreates:   true,
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
}

// defaultModes is the ATTACH default when a client requests no specific
// modes (DESIGN.md §4.2): the three message/presence modes, presence
// subscribe, and ANNOTATION_PUBLISH — matching the reference server's
// MODE_DEFAULT and the SDK's expected default channel.modes. Only
// ANNOTATION_SUBSCRIBE is opt-in (§14.3): raw annotation delivery must be
// requested explicitly.
const defaultModes = protocol.FlagPresence | protocol.FlagPublish | protocol.FlagSubscribe | protocol.FlagPresenceSubscribe | protocol.FlagAnnotationPublish

// modeMask is every recognised channel-mode bit — the default set plus the
// opt-in ANNOTATION_SUBSCRIBE (DESIGN.md §4.2, §14.3). Used to extract the
// requested mode bits from an ATTACH flags word.
const modeMask = defaultModes | protocol.FlagAnnotationSubscribe

// resolveModes extracts the channel-mode bits from an ATTACH flags word.
// A request with no mode bits is treated as the default set (DESIGN.md
// §4.2; ANNOTATION_SUBSCRIBE must be opted into explicitly, §14.3).
func resolveModes(flags int64) int64 {
	if m := flags & modeMask; m != 0 {
		return m
	}
	return defaultModes
}

// resolveRequestedModes resolves the channel modes an ATTACH requests,
// honouring the reference precedence (DESIGN.md §4.2): a comma-separated
// `modes` channel param wins over the flags mode bits, which win over the
// default set. The `modes` param is how SDKs send ChannelOptions.params
// = {modes: 'subscribe,presence'} (RTL4k), and the SDK expects the resolved
// set reflected both in ATTACHED.flags (from which it reads channel.modes)
// and echoed in ATTACHED.params.modes. A param that names no valid mode is
// ignored and the flags/default path is used instead.
func resolveRequestedModes(flags int64, params map[string]string) int64 {
	if s, ok := params["modes"]; ok {
		if m := parseModesParam(s); m != 0 {
			return m
		}
	}
	return resolveModes(flags)
}

// modeParamNames maps each channel-mode bit to its wire token, in the
// order the reference emits them in ATTACHED.params.modes (Mode.ParamsString).
var modeParamNames = []struct {
	bit  int64
	name string
}{
	{protocol.FlagPresence, "presence"},
	{protocol.FlagPublish, "publish"},
	{protocol.FlagSubscribe, "subscribe"},
	{protocol.FlagPresenceSubscribe, "presence_subscribe"},
	{protocol.FlagAnnotationSubscribe, "annotation_subscribe"},
	{protocol.FlagAnnotationPublish, "annotation_publish"},
}

// parseModesParam parses a comma-separated channel-modes param value into
// its mode-bit set, ignoring unrecognised tokens (matching the reference's
// ParseModes, which drops invalid tokens rather than erroring).
func parseModesParam(s string) int64 {
	var m int64
	for _, tok := range strings.Split(s, ",") {
		tok = strings.ToLower(strings.TrimSpace(tok))
		for _, mp := range modeParamNames {
			if tok == mp.name {
				m |= mp.bit
			}
		}
	}
	return m
}

// modesParamString renders a mode-bit set as the comma-separated token
// list carried in ATTACHED.params.modes (reference Mode.ParamsString order).
func modesParamString(modes int64) string {
	names := make([]string, 0, len(modeParamNames))
	for _, mp := range modeParamNames {
		if modes&mp.bit != 0 {
			names = append(names, mp.name)
		}
	}
	return strings.Join(names, ",")
}

// hasMode reports whether this attachment holds the given channel mode.
// Read under mu since a repeat ATTACH can mutate the mode set in place.
func (a *attachment) hasMode(mode int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.modes&mode != 0
}

// modeSet returns the current effective mode set under mu.
func (a *attachment) modeSet() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.modes
}

// applyReattach mutates the attachment in place for a repeat ATTACH:
// it swaps in the freshly-requested mode set, the freshly-resolved
// effective mode set, and the new params (re-deriving appendMode) and
// returns the current live serial to advertise on the re-ATTACHED. The
// stream and its goroutine are left untouched, so delivery continuity is
// preserved by construction.
func (a *attachment) applyReattach(requestedModes, modes int64, params map[string]string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requestedModes = requestedModes
	a.modes = modes
	a.params = params
	a.appendModeFull = params[ParamAppendMode] == AppendModeFull
	return a.curSerial
}

// recheckCapability re-derives the effective mode set from the
// originally-requested modes intersected with newPermitted (DESIGN.md §3,
// RTC8a1) — used after an inband reauth changes the connection's
// capability, so a widening reauth can restore a mode a previous
// narrowing dropped, and a narrowing one only fails the channel when the
// intersection empties out completely (matching the reference server's
// permittedModeAndParams recheck). Returns the new effective set (0 if no
// longer permitted at all).
func (a *attachment) recheckCapability(newPermitted int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.modes = a.requestedModes & newPermitted
	return a.modes
}

// setCurSerial records the channelSerial the live cursor has advanced to,
// so a concurrent repeat ATTACH can advertise the current position.
func (a *attachment) setCurSerial(serial string) {
	a.mu.Lock()
	a.curSerial = serial
	a.mu.Unlock()
}

// recognisedParams is the set of ATTACH channel params this server
// understands (DESIGN.md §4.2). ATTACHED.params echoes only these: an
// unrecognised param is dropped rather than reflected back, matching the
// reference, so channel.params (RTL4k1) carries only params that actually
// took effect. A client that requests a bogus param sees channel.params
// without it.
var recognisedParams = map[string]struct{}{
	"rewind": {},
	"modes":  {},
	// delta is echoed for SDK compatibility (the client asked for it) even
	// though delta encoding itself is unsupported: this server never emits
	// deltas, so the echo is inert.
	"delta":         {},
	ParamAppendMode: {},
}

// echoParams builds the params map echoed on ATTACHED (DESIGN.md §4.1),
// from which the SDK populates channel.params (RTL4k1). It reflects only
// the recognised params the client requested (recognisedParams), with the
// `modes` entry rewritten to the effective (capability-intersected) mode
// set so channel.params.modes agrees with the modes the SDK decodes from
// ATTACHED.flags. Returns nil when no recognised param was requested.
func (a *attachment) echoParams() map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	echo := make(map[string]string, len(a.params))
	for k, v := range a.params {
		if _, ok := recognisedParams[k]; !ok {
			continue
		}
		echo[k] = v
	}
	if _, ok := echo["modes"]; ok {
		echo["modes"] = modesParamString(a.modes)
	}
	if len(echo) == 0 {
		return nil
	}
	return echo
}

// run sends ATTACHED, optionally replays history (resume or rewind),
// then forwards stream ChannelMessages to the connection until the
// attachment's context is cancelled. done is closed on exit.
func (a *attachment) run() {
	defer close(a.done)
	// Closing the stream releases the attachment's hold on the channel,
	// so it can be evicted once idle (DESIGN.md §5.1).
	defer a.stream.Close()

	anchor := a.stream.ChannelSerial()
	replay, attachPoint, resumed, errInfo := a.computeReplay(anchor)
	// Seed the position a concurrent repeat ATTACH advertises:
	// the advertised attach point until the live loop advances it.
	a.setCurSerial(attachPoint)

	// Presence sync snapshot (DESIGN.md §12.4): only PRESENCE_SUBSCRIBE
	// attachments get the current set. Captured before ATTACHED so the
	// HAS_PRESENCE flag can be set. The snapshot is taken at-or-after the
	// live anchor; any member that enters or leaves past the anchor also
	// arrives on the live cursor, and the client converges by serial.
	var snap *core.PresenceSnapshot
	if a.hasMode(protocol.FlagPresenceSubscribe) {
		began := time.Now()
		var err error
		if snap, err = a.stream.Channel().PresenceSync(a.ctx); err != nil {
			a.syncSkipped(a.ctx, err)
			snap = nil
		} else {
			a.metrics.PresenceSyncSnapshot(time.Since(began))
		}
	}
	hasSync := snap != nil && len(snap.Members) > 0

	// ATTACHED.flags carries the effective channel-mode set (DESIGN.md
	// §4.2), plus the status flags below.
	flags := a.modeSet()
	if resumed {
		flags |= protocol.FlagResumed
	}
	// Any replay (rewind or resume gap-fill) means backlog was delivered
	// ahead of live messages; the SDK surfaces this as hasBacklog (RTL2i).
	if len(replay) > 0 {
		flags |= protocol.FlagHasBacklog
	}
	if hasSync {
		flags |= protocol.FlagHasPresence
	}

	attached := &protocol.ProtocolMessage{
		Action:        protocol.ActionAttached,
		Channel:       new(a.channelName),
		ChannelSerial: attachPoint,
		Flags:         flags,
		Error:         errInfo,
		Params:        a.echoParams(),
	}
	if !a.sendAttached(attached) {
		return
	}

	// Deliver the presence set as a SYNC frame before live delivery. A
	// single frame suffices at our scale; the channelSerial carries the
	// sync cursor — "<serial>:" with an empty cursor part marks the set
	// complete (paging is a later phase, DESIGN.md §12.4).
	if hasSync && !a.sendSync(snap, true) {
		return
	}

	// Replay (resume/rewind) is backlog delivery: appends arrive as full
	// rolled-up updates, never deltas (DESIGN.md §13.3).
	for _, cm := range replay {
		if !a.forward(cm, true) {
			return
		}
	}

	for {
		cm, err := a.stream.Next(a.ctx)
		if err != nil {
			return
		}
		if a.stream.Discontinuity() {
			if !a.signalDiscontinuity(cm.ChannelSerial) {
				return
			}
			continue
		}
		// Advance the position a concurrent repeat ATTACH advertises to
		// this cm's serial before delivery, so the re-ATTACHED reflects
		// where the (never-interrupted) stream has reached.
		a.setCurSerial(cm.ChannelSerial)
		if !a.forward(cm, false) {
			return
		}
	}
}

// errMessagesExpired is the ATTACHED error of a channel update sent for
// a discontinuity (DESIGN.md §7.2): the code a resume older than the
// retention floor gets (§4.3), since the cause is the same.
var errMessagesExpired = protocol.ErrorInfo{
	Message:    "unable to recover channel (messages expired): a gap in delivery could not be filled from the log",
	Code:       80016,
	StatusCode: 404,
}

// signalDiscontinuity tells the client that cms may be missing at this
// point of the live stream (a discontinuity marker, core.Stream.
// Discontinuity; DESIGN.md §7.2): a server-initiated channel update
// (RTL12), an ATTACHED with the current channelSerial, the effective
// modes without RESUMED, and error 80016, so the SDK emits an update
// event with resumed false and the application can reconcile. Nothing is
// replayed and the attachment carries on with the live stream. A
// PRESENCE_SUBSCRIBE attachment also gets the presence set again, as on
// attach: HAS_PRESENCE and a SYNC when the set has members, so the SDK
// replaces its set rather than clearing it (RTP19a); the channel dropped
// its local set with the marker, so the snapshot is read afresh.
// Returns false if a send failed.
func (a *attachment) signalDiscontinuity(serial string) bool {
	var snap *core.PresenceSnapshot
	if a.hasMode(protocol.FlagPresenceSubscribe) {
		var err error
		if snap, err = a.stream.Channel().PresenceSync(a.ctx); err != nil {
			a.syncSkipped(a.ctx, err)
			if a.ctx.Err() != nil {
				return false
			}
			snap = nil
		}
	}
	hasSync := snap != nil && len(snap.Members) > 0
	flags := a.modeSet()
	if hasSync {
		flags |= protocol.FlagHasPresence
	}
	a.setCurSerial(serial)
	errInfo := errMessagesExpired
	if !a.send(&protocol.ProtocolMessage{
		Action:        protocol.ActionAttached,
		Channel:       new(a.channelName),
		ChannelSerial: serial,
		Flags:         flags,
		Error:         &errInfo,
		Params:        a.echoParams(),
	}) {
		return false
	}
	return !hasSync || a.sendSync(snap, false)
}

// forward delivers one ChannelMessage to the connection as the wire
// frame appropriate to its kind, gated by the attachment's modes: a
// message cm becomes a MESSAGE frame (SUBSCRIBE), a presence cm becomes
// a PRESENCE frame (PRESENCE_SUBSCRIBE). A cm the attachment is not
// subscribed to is skipped. backlog is true during resume/rewind replay,
// which forces appends to be delivered as full versions. Returns false
// if the send is cancelled.
func (a *attachment) forward(cm *protocol.ChannelMessage, backlog bool) bool {
	if len(cm.Messages) > 0 {
		if !a.hasMode(protocol.FlagSubscribe) {
			return true
		}
		// echo=false suppresses delivery of this connection's own published
		// messages back to it (DESIGN.md §2.1). All messages in one cm share
		// the publishing connection, so the first message's stamped
		// connectionId identifies the origin. Presence is never suppressed —
		// it is handled by the branch below.
		selfEcho := a.connID != "" && cm.Messages[0].ConnectionID == a.connID
		if !a.echo && selfEcho {
			a.logger.Trace("message delivery skipped (echo=false)",
				"channelSerial", cm.ChannelSerial, "name", cm.Messages[0].Name)
			return true
		}
		msgs := a.resolveAppends(cm.Messages, backlog)
		frame := &protocol.ProtocolMessage{
			Action:        protocol.ActionMessage,
			Channel:       new(a.channelName),
			ChannelSerial: cm.ChannelSerial,
			Messages:      msgs,
		}
		// With no append delta to resolve, the frame is the same for
		// every attachment on the channel, so it is encoded once per
		// wire format and shared (DESIGN.md §5.1).
		if !a.sendLive(frame, backlog, sameItems(msgs, cm.Messages)) {
			return false
		}
		// Guarded: the arguments allocate on every delivery, even with
		// trace logging off.
		if a.logger.Enabled(a.ctx, logging.LevelTrace) {
			a.logger.Trace("message delivered",
				"channelSerial", cm.ChannelSerial, "name", cm.Messages[0].Name, "selfEcho", selfEcho)
		}
		a.metrics.MessageDelivered()
		return true
	}
	if len(cm.Presence) > 0 {
		if !a.hasMode(protocol.FlagPresenceSubscribe) {
			return true
		}
		return a.sendLive(&protocol.ProtocolMessage{
			Action:        protocol.ActionPresence,
			Channel:       new(a.channelName),
			ChannelSerial: cm.ChannelSerial,
			Presence:      cm.Presence,
		}, backlog, true)
	}
	if len(cm.Annotations) > 0 {
		// An annotation cm fans out two ways (DESIGN.md §14.3), both derived
		// from this one cm so cluster nodes deliver identically:
		//   - SUBSCRIBE: a MESSAGE with action summary (4) per annotation,
		//     carrying the target's unchanged serial and the post-fold
		//     snapshot the backend stamped on the annotation — this is how a
		//     subscriber that knows nothing about annotations sees reactions
		//     accumulate;
		//   - ANNOTATION_SUBSCRIBE: the raw ANNOTATION frame, as today.
		// A dual-mode attachment gets both.
		if a.hasMode(protocol.FlagSubscribe) {
			for _, an := range cm.Annotations {
				if !a.send(&protocol.ProtocolMessage{
					Action:        protocol.ActionMessage,
					Channel:       new(a.channelName),
					ChannelSerial: cm.ChannelSerial,
					Messages: []*protocol.Message{{
						Action:  protocol.MessageSummary,
						Serial:  an.MessageSerial,
						Summary: an.Summary,
					}},
				}) {
					return false
				}
			}
		}
		if !a.hasMode(protocol.FlagAnnotationSubscribe) {
			return true
		}
		return a.send(&protocol.ProtocolMessage{
			Action:        protocol.ActionAnnotation,
			Channel:       new(a.channelName),
			ChannelSerial: cm.ChannelSerial,
			Annotations:   cm.Annotations,
		})
	}
	return true
}

// resolveAppends adapts a message cm's payload to this subscriber's
// append delivery mode (DESIGN.md §13.3). An append is stored as a full
// action=update carrying the rolled-up aggregate plus the incremental
// delta in Alt[DeltaAppend]. A caught-up subscriber receives the delta
// (action=append); otherwise the full aggregate is delivered.
//
// Deltas are used only when NONE of the following forces a full version:
// the subscriber opted into full versions (appendMode=full); delivery is
// backlog replay (resume/rewind); or the cm carries an append for a
// message this attachment has not yet seen — the first delivery for a
// not-yet-seen message is always the full aggregate so the subscriber
// has complete state before later deltas apply. The decision is
// all-or-nothing across the cm's messages, matching the atomic frame.
// The internal Alt carrier is stripped from any message delivered as a
// full version.
//
// A message's identity is then recorded as seen only when a later delta
// for it could be delivered as a delta: it carries an append delta, or
// the channel is in a mutable-messages namespace (trackCreates), so a
// create may be appended to later. Nothing is recorded under
// appendMode=full. The seen set is bounded; a serial it has evicted or
// never recorded makes the next append a full version, which is always
// a valid delivery (DESIGN.md §13.3).
func (a *attachment) resolveAppends(msgs []*protocol.Message, backlog bool) []*protocol.Message {
	a.mu.Lock()
	appendModeFull := a.appendModeFull
	a.mu.Unlock()
	asDeltas := !backlog && !appendModeFull
	if asDeltas {
		for _, m := range msgs {
			if m.HasAppendDelta() && m.Serial != "" && !a.seen.has(m.Serial) {
				asDeltas = false
				break
			}
		}
	}
	if !appendModeFull {
		for _, m := range msgs {
			if m.Serial != "" && (a.trackCreates || m.HasAppendDelta()) {
				a.seen.add(m.Serial)
			}
		}
	}

	// Fast path: nothing carries an append delta, so there is nothing to
	// strip or swap.
	transform := false
	for _, m := range msgs {
		if m.HasAppendDelta() {
			transform = true
			break
		}
	}
	if !transform {
		return msgs
	}

	out := make([]*protocol.Message, len(msgs))
	for i, m := range msgs {
		switch {
		case !m.HasAppendDelta():
			out[i] = m
		case asDeltas:
			out[i] = m.Alt[protocol.DeltaAppend]
		default:
			// Full version: hand over the aggregate without the internal
			// Alt carrier.
			clone := *m
			clone.Alt = nil
			out[i] = &clone
		}
	}
	return out
}

// computeReplay decides what to replay, what attach point to advertise
// in ATTACHED.channelSerial, whether RESUMED should be set, and any
// ErrorInfo to surface.
//
// Three modes:
//
//   - Fresh attach (no resumeFrom, no rewindParam): no replay,
//     attach point = anchor (the live tail), RESUMED clear.
//   - Resume (resumeFrom != ""): replay the gap from the client's
//     cursor up to anchor; attach point = client's cursor.
//   - Rewind (rewindParam != "" and resumeFrom == ""): replay
//     historical context preceding the live tail; attach point =
//     the predecessor of the rewind window (or the channel's
//     immutable initial serial when the window covers everything).
//     RESUMED always clear for rewind.
func (a *attachment) computeReplay(anchor string) (replay []*protocol.ChannelMessage, attachPoint string, resumed bool, errInfo *protocol.ErrorInfo) {
	switch {
	case a.resumeFrom != "":
		return a.computeResumeReplay(anchor)
	case a.rewindParam != "":
		return a.computeRewindReplay(anchor)
	default:
		return nil, anchor, false, nil
	}
}

func (a *attachment) computeResumeReplay(anchor string) ([]*protocol.ChannelMessage, string, bool, *protocol.ErrorInfo) {
	if a.resumeFrom == anchor {
		// Caught up: nothing to replay, but the resume is "complete".
		return nil, a.resumeFrom, true, nil
	}

	// Backwards from the anchor inclusive; cap+1 lets us distinguish
	// a perfect-fit gap (== cap) from a cap-exceeded gap (> cap).
	page, err := a.channel.History(a.ctx, storage.HistoryQuery{
		Direction:        storage.DirectionBackwards,
		EndChannelSerial: anchor,
		Limit:            a.replayCap + 1,
	})
	if err != nil {
		a.logger.Warn("resume history failed; falling back to fresh attach", "err", err)
		return nil, a.resumeFrom, false, &protocol.ErrorInfo{
			Message:    "history lookup failed; resume replay was skipped",
			Code:       40000,
			StatusCode: 500,
		}
	}

	cms := page.ChannelMessages

	// Find the oldest cm whose serial <= resumeFrom — everything
	// before that index is part of the gap to replay.
	cut := len(cms)
	for i, cm := range cms {
		if cm.ChannelSerial <= a.resumeFrom {
			cut = i
			break
		}
	}

	if cut < len(cms) {
		return reverseAndNormalise(cms[:cut]), a.resumeFrom, true, nil
	}

	// Client cursor not matched. Three sub-cases:
	//   - the cursor predates the channel's retention floor → cms
	//     between it and the oldest retained one may have aged out, so
	//     continuity cannot be proven: attach at the live head with
	//     RESUMED clear, an error, and no replay (DESIGN.md §4.3).
	//   - returned cap+1 cms → cap exceeded
	//   - returned <= cap cms → exhausted the backwards walk; storage
	//     has nothing older and nothing it held was dropped, so we
	//     delivered the full history, RESUMED set. The client's cursor
	//     is effectively older than everything we have — common after
	//     a rewind that used the channel's initial serial as its attach
	//     point.
	if floor := a.channel.RetainedSince(time.Now()); floor != "" && a.resumeFrom < floor {
		return nil, anchor, false, &protocol.ErrorInfo{
			Message:    "unable to recover channel (messages expired): the resume point is older than the channel's retained history",
			Code:       80016,
			StatusCode: 404,
		}
	}
	if len(cms) <= a.replayCap {
		return reverseAndNormalise(cms), a.resumeFrom, true, nil
	}
	return reverseAndNormalise(cms[:a.replayCap]), a.resumeFrom, false, &protocol.ErrorInfo{
		Message:    fmt.Sprintf("replay was truncated to the most recent %d messages", a.replayCap),
		Code:       40012,
		StatusCode: 200,
	}
}

func (a *attachment) computeRewindReplay(anchor string) ([]*protocol.ChannelMessage, string, bool, *protocol.ErrorInfo) {
	mode, count, dur, err := ParseRewind(a.rewindParam)
	if err != nil {
		return nil, anchor, false, &protocol.ErrorInfo{
			Message:    fmt.Sprintf("invalid rewind value %q: %v", a.rewindParam, err),
			Code:       40000,
			StatusCode: 400,
		}
	}

	// Common: backwards from anchor with a Limit one greater than the
	// deliverable maximum. The "+1" returned (if any) is the cm just
	// before the rewind window — the natural attach point. If the
	// query returns <= the deliverable maximum (rewind window covers
	// the entire channel), the attach point falls back to the
	// channel's immutable initial serial.
	var (
		query    storage.HistoryQuery
		capLimit int // deliverable maximum (not Limit)
	)
	switch mode {
	case rewindCount:
		capLimit = min(count, a.replayCap)
		query = storage.HistoryQuery{
			Direction:        storage.DirectionBackwards,
			EndChannelSerial: anchor,
			Limit:            capLimit + 1,
		}
	case rewindDuration:
		capLimit = a.replayCap
		nowMs := time.Now().UnixMilli()
		startMs := max(nowMs-dur.Milliseconds(), 1)
		query = storage.HistoryQuery{
			Direction:        storage.DirectionBackwards,
			EndChannelSerial: anchor,
			Start:            startMs,
			Limit:            capLimit + 1,
		}
	default:
		return nil, anchor, false, nil
	}

	page, err := a.channel.History(a.ctx, query)
	if err != nil {
		a.logger.Warn("rewind history failed", "err", err)
		return nil, anchor, false, &protocol.ErrorInfo{
			Message:    "history lookup failed; rewind replay was skipped",
			Code:       40000,
			StatusCode: 500,
		}
	}

	cms := page.ChannelMessages
	// page is newest-first. The oldest in cms (last element) is the
	// "+1" entry when present — the predecessor of the rewind window.
	var (
		attachPoint string
		windowCms   []*protocol.ChannelMessage
		capExceeded bool
	)
	if len(cms) > capLimit {
		// The (capLimit+1)-th oldest is the predecessor; drop it,
		// deliver the rest.
		attachPoint = cms[capLimit].ChannelSerial
		windowCms = cms[:capLimit]
		if mode == rewindCount && count > a.replayCap {
			capExceeded = true
		} else if mode == rewindDuration {
			capExceeded = true
		}
	} else if len(cms) > 0 {
		// Rewind window covers everything we have; no predecessor in
		// storage. Use the channel's immutable initial serial.
		attachPoint = a.channel.InitialChannelSerial()
		windowCms = cms
	} else {
		// Nothing retained at all: attach at the live head. The initial
		// serial would be equivalent for a backend that keeps
		// everything, but under retention (DESIGN.md §6.3) it is older
		// than the retention floor, so the client's next resume from it
		// would be refused as a discontinuity although nothing was lost.
		attachPoint = anchor
	}

	var info *protocol.ErrorInfo
	if capExceeded {
		info = &protocol.ErrorInfo{
			Message:    fmt.Sprintf("rewind was truncated to the most recent %d messages", a.replayCap),
			Code:       40012,
			StatusCode: 200,
		}
	}
	return reverseAndNormalise(windowCms), attachPoint, false, info
}

// reverseAndNormalise flips the ChannelMessage slice from newest-first
// to oldest-first, and un-reverses each cm's Messages slice (which
// the backwards-direction storage scan emits in reverse idx order).
// The cms are copies — safe to mutate.
func reverseAndNormalise(cms []*protocol.ChannelMessage) []*protocol.ChannelMessage {
	out := make([]*protocol.ChannelMessage, len(cms))
	for i, cm := range cms {
		out[len(cms)-1-i] = cm
		for j, k := 0, len(cm.Messages)-1; j < k; j, k = j+1, k-1 {
			cm.Messages[j], cm.Messages[k] = cm.Messages[k], cm.Messages[j]
		}
	}
	return out
}

// stop cancels the attachment and waits for its goroutine to exit, so
// callers can safely queue a DETACHED frame after stop returns
// knowing no further MESSAGE frames will arrive on the outbound chan.
func (a *attachment) stop() {
	a.cancel()
	<-a.done
}

// resync answers a client-initiated SYNC frame (ably-js RealtimeChannel.sync,
// RTP19) by re-delivering the channel's current presence set as a SYNC frame,
// exactly as the ATTACH-time snapshot does (DESIGN.md §12.4). The client
// reconciles its local presence map against this authoritative set,
// synthesising LEAVEs for any member it holds that is absent here. Only a
// PRESENCE_SUBSCRIBE attachment receives presence, so a channel without that
// mode gets no reply. Called on the connection read goroutine; the send goes
// through the same outbound channel as the live loop, so ordering holds.
func (a *attachment) resync(ctx context.Context) {
	if !a.hasMode(protocol.FlagPresenceSubscribe) {
		return
	}
	snap, err := a.stream.Channel().PresenceSyncNow(ctx)
	if err != nil {
		a.syncSkipped(ctx, err)
		return
	}
	a.sendSync(snap, false)
}

// syncSkipped records a SYNC that delivers no presence set because its
// snapshot could not be obtained (ably_presence_syncs_skipped_total,
// DESIGN.md §12.4). When ctx has ended, the attachment or its connection
// is already going away and nobody is waiting for the set: that is
// counted as "closed" and logged at debug level. Anything else is a
// failed store read, counted as "error" and logged as a warning; the
// client then has no presence set until it re-attaches or sends SYNC.
func (a *attachment) syncSkipped(ctx context.Context, err error) {
	if ctx.Err() != nil {
		a.metrics.PresenceSyncSkipped("closed")
		a.logger.Debug("presence sync: attachment closed while obtaining the snapshot; skipping sync", "err", err)
		return
	}
	a.metrics.PresenceSyncSkipped("error")
	a.logger.Warn("presence sync: snapshot failed; skipping sync", "err", err)
}

// sendAttached queues an attach's ATTACHED frame, timing it from the
// ATTACH being read to the frame being written (ably_attach_seconds,
// DESIGN.md §10).
func (a *attachment) sendAttached(msg *protocol.ProtocolMessage) bool {
	if a.outObserved == nil || a.received.IsZero() {
		return a.send(msg)
	}
	received, m := a.received, a.metrics
	return a.outObserved(a.ctx, msg, nil, func(int) { m.AttachWritten(time.Since(received)) })
}

// sendSync delivers snap as one SYNC frame. A single frame carries the
// whole set at our scale; the "<serial>:" channelSerial (empty cursor
// part) marks the sync complete so the client ends its sync and applies
// the reconciliation in one step (paging is a later phase, DESIGN.md
// §12.4). The frame is the same for every attach snap is served to, so
// it is encoded once per wire format.
//
// The frame's size is recorded (ably_presence_sync_frame_bytes) and, for
// the SYNC of an attach (onAttach), its queue and write stages
// (ably_presence_sync_stage_seconds) and the time from the ATTACH being
// read to the frame being written (ably_attach_seconds{until="synced"}).
func (a *attachment) sendSync(snap *core.PresenceSnapshot, onAttach bool) bool {
	msg := &protocol.ProtocolMessage{
		Action:        protocol.ActionSync,
		Channel:       new(a.channelName),
		ChannelSerial: snap.AsOf + ":",
		Presence:      snap.Members,
	}
	if a.outObserved == nil {
		if a.outShared == nil {
			return a.send(msg)
		}
		return a.outShared(a.ctx, msg, snap)
	}
	m, received := a.metrics, a.received
	queued := time.Now()
	ok := a.outObserved(a.ctx, msg, snap, func(bytes int) {
		m.PresenceSyncFrame(bytes)
		if onAttach {
			m.PresenceSyncWritten(time.Since(queued))
			if !received.IsZero() {
				m.AttachSynced(time.Since(received))
			}
		}
	})
	if ok && onAttach {
		m.PresenceSyncQueued(time.Since(queued))
	}
	return ok
}

// sendLive sends one frame derived from the cm the stream last returned.
// A live frame that is the same for every attachment on the channel
// (shareable) is encoded once per wire format on the stream's entry and
// the bytes are shared (DESIGN.md §5.1); anything else, and every backlog
// frame (replayed cms are read from the log, not the live list), is
// encoded for this attachment alone. A sampled attachment records the
// time from the cm's append to the frame being queued.
func (a *attachment) sendLive(msg *protocol.ProtocolMessage, backlog, shareable bool) bool {
	var ok bool
	if !backlog && shareable && a.outShared != nil {
		ok = a.outShared(a.ctx, msg, a.stream)
	} else {
		ok = a.send(msg)
	}
	if ok && !backlog && a.sampled {
		a.metrics.DeliveryFanout(time.Since(a.stream.AppendedAt()))
	}
	return ok
}

// sameItems reports whether resolved is items itself (the same backing
// array), which is how resolveAppends hands back a cm's messages it did
// not need to change.
func sameItems(resolved, items []*protocol.Message) bool {
	return len(resolved) == len(items) && (len(items) == 0 || &resolved[0] == &items[0])
}

// send pushes a frame onto the connection's outbound queue, waiting
// under backpressure for at most the write timeout (DESIGN.md §5.2).
// Returns false if the attachment's context is cancelled or the
// connection is closing (including a slow-consumer disconnect).
func (a *attachment) send(msg *protocol.ProtocolMessage) bool {
	return a.out(a.ctx, msg)
}
