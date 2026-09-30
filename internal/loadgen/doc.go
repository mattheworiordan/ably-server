// Package loadgen is the library behind cmd/ably-loadgen and
// cmd/ably-conductor: a minimal raw-WebSocket client for the Ably realtime
// protocol (DESIGN.md §2.1, §4, §8), a REST publisher pool, a per-channel
// serial-continuity checker, mergeable latency histograms, deterministic
// assignment of channels and connections to generator processes, and the
// JSON summary a run produces.
//
// The client speaks only what a load generator needs: CONNECT (the
// WebSocket upgrade), CONNECTED, ATTACH/ATTACHED, DETACH/DETACHED,
// MESSAGE (publish and delivery), ACK/NACK, PRESENCE enter/leave,
// HEARTBEAT, CLOSE, DISCONNECTED and ERROR. Continuity after a
// reconnect is per channel (DESIGN.md §4.3): the client re-attaches with
// the last channelSerial it saw and the checker verifies that nothing is
// missing or repeated across the gap.
package loadgen
