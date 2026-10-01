//go:build integration

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/ably/ably-server/internal/protocol"
	"github.com/ably/ably-server/internal/storage/postgres/natstest"
	"github.com/ably/ably-server/internal/storage/postgres/pgtest"
)

// TestNATSBusDropsForgedEnvelopes: a raw NATS publish onto a bound
// channel's subject of an envelope whose serial is malformed, of one
// whose serial was minted far in the future, and of a valid-looking one
// of another cluster are all dropped and counted, and the channel goes
// on delivering genuine cms after them (DESIGN.md §7.2). Before the
// envelope was validated, the first two were appended and moved the
// delivery mark above every serial the channel would mint, so every
// later genuine cm was dropped as a duplicate: the channel was wedged
// until it was rebound.
func TestNATSBusDropsForgedEnvelopes(t *testing.T) {
	c := pgtest.Start(t)
	n := natstest.Start(t)
	dsn := c.FreshSchemaDSN(t)
	ctx := context.Background()
	swapNATSTimings(t, 50*time.Millisecond, time.Hour)

	s1 := openNATSNode(t, dsn, n.URL) // the publisher
	s2 := openNATSNode(t, dsn, n.URL) // the receiver under attack
	rec := &cmRecorder{}
	ch1 := bindChannel(t, s1, "room", &cmRecorder{})
	bindChannel(t, s2, "room", rec)

	publish := func(i int) string {
		t.Helper()
		cm, _, err := ch1.Store(ctx, []*protocol.Message{{Data: fmt.Sprintf("genuine-%d", i)}})
		if err != nil {
			t.Fatalf("Store %d: %v", i, err)
		}
		return cm.ChannelSerial
	}
	want := []string{publish(0)}
	rec.waitFor(t, 1, 5*time.Second)

	raw, err := nats.Connect(n.URL)
	if err != nil {
		t.Fatalf("raw NATS connect: %v", err)
	}
	t.Cleanup(raw.Close)
	bus2 := natsBusOf(t, s2)
	subject := natsSubject(bus2.prefix, "room")
	inject := func(deployment, serial, prev string) {
		t.Helper()
		env := natsEnvelope{
			Channel: "room", Serial: serial, Prev: prev, Deployment: deployment,
			CM:     &protocol.ChannelMessage{ChannelSerial: serial, Messages: []*protocol.Message{{Data: "forged"}}},
			SentAt: time.Now().UnixNano(),
		}
		data, err := msgpack.Marshal(&env)
		if err != nil {
			t.Fatalf("encode forged envelope: %v", err)
		}
		if err := raw.Publish(subject, data); err != nil {
			t.Fatalf("publish forged envelope: %v", err)
		}
		if err := raw.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	last := want[0]
	soon := fmt.Sprintf("%014d-000@forged0000", time.Now().UnixMilli()+1)
	far := fmt.Sprintf("%014d-000@forged0000", time.Now().Add(24*time.Hour).UnixMilli())
	inject(bus2.deployment, "zzz", last)          // malformed serial, sorts above every real one
	inject(bus2.deployment, soon, "not-a-serial") // malformed predecessor
	inject(bus2.deployment, soon, soon)           // predecessor not below the serial
	inject(bus2.deployment, far, last)            // minted a day ahead
	inject("another-cluster", soon, last)         // valid-looking, of another cluster

	// Genuine cms after the forgeries still arrive, once each, in order.
	for i := 1; i <= 5; i++ {
		want = append(want, publish(i))
	}
	rec.waitFor(t, len(want), 5*time.Second)
	assertSerials(t, "receiver", rec.serials(), want)
	for _, cm := range rec.cms() {
		if cm.Messages[0].Data == "forged" {
			t.Errorf("a forged cm %s was delivered", cm.ChannelSerial)
		}
	}

	waitFor(t, 2*time.Second, "the forged envelopes to be counted", func() bool {
		st := s2.BusStats()
		return st.MalformedSerial == 3 && st.MalformedFuture == 1 && st.UnroutedForeign == 1
	})
	if st := s2.BusStats(); st.Malformed != 4 || st.Unrouted != 1 {
		t.Errorf("malformed = %d, unrouted = %d; want 4 and 1", st.Malformed, st.Unrouted)
	}
}
