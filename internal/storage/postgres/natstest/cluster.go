//go:build integration

package natstest

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Cluster is a NATS core cluster of several servers on one Docker
// network, fully meshed by routes, the shape the nats bus runs against in
// production (DESIGN.md §7.2). Unlike Start it is per test, because a
// test kills servers.
type Cluster struct {
	Servers []*ClusterServer
}

// ClusterServer is one server of a Cluster.
type ClusterServer struct {
	Name string // container alias on the cluster network, e.g. "nats-0"
	URL  string // client URL on the host, e.g. nats://127.0.0.1:32768
	Addr string // host:port part of URL

	ctr     testcontainers.Container
	monitor string // host:port of the HTTP monitoring endpoint
}

// URL is the comma-separated client URL list of every server, as an
// operator passes it to --nats-url.
func (c *Cluster) URL() string {
	urls := make([]string, len(c.Servers))
	for i, s := range c.Servers {
		urls[i] = s.URL
	}
	return strings.Join(urls, ",")
}

// ByURL returns the server whose client URL is url (as reported by a
// client's ConnectedUrl), or nil.
func (c *Cluster) ByURL(url string) *ClusterServer {
	for _, s := range c.Servers {
		if s.URL == url {
			return s
		}
	}
	return nil
}

// Kill removes the server's container at once (SIGKILL), as a crashed
// machine would disappear.
func (s *ClusterServer) Kill(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.ctr.Terminate(ctx); err != nil {
		t.Fatalf("kill %s: %v", s.Name, err)
	}
}

// StartCluster starts an n-server NATS cluster and waits until every
// server has a route to every other. Servers are started with
// --no_advertise, so clients use only the host URLs they were given and
// never the container-internal addresses the cluster gossips. The
// cluster is removed on t.Cleanup.
func StartCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("nats cluster network: %v", err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	names := make([]string, n)
	routes := make([]string, n)
	for i := range n {
		names[i] = fmt.Sprintf("nats-%d", i)
		routes[i] = "nats://" + names[i] + ":6222"
	}

	c := &Cluster{}
	for _, name := range names {
		ctr, err := testcontainers.Run(ctx, "nats:2.11-alpine",
			network.WithNetwork([]string{name}, nw),
			testcontainers.WithExposedPorts("4222/tcp", "8222/tcp"),
			testcontainers.WithCmd(
				"--name", name,
				"--cluster_name", "ably",
				"--cluster", "nats://0.0.0.0:6222",
				"--routes", strings.Join(routes, ","),
				"--no_advertise",
				"--http_port", "8222",
			),
			testcontainers.WithWaitStrategy(wait.ForLog("Server is ready")),
		)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		srv := &ClusterServer{Name: name, ctr: ctr}
		t.Cleanup(func() { _ = srv.ctr.Terminate(context.Background()) })
		host, err := ctr.Host(ctx)
		if err != nil {
			t.Fatalf("%s host: %v", name, err)
		}
		client, err := ctr.MappedPort(ctx, "4222/tcp")
		if err != nil {
			t.Fatalf("%s client port: %v", name, err)
		}
		mon, err := ctr.MappedPort(ctx, "8222/tcp")
		if err != nil {
			t.Fatalf("%s monitor port: %v", name, err)
		}
		srv.Addr = net.JoinHostPort(host, client.Port())
		srv.URL = "nats://" + srv.Addr
		srv.monitor = net.JoinHostPort(host, mon.Port())
		c.Servers = append(c.Servers, srv)
	}

	deadline := time.Now().Add(60 * time.Second)
	for _, srv := range c.Servers {
		for {
			if routesUp(srv.monitor) >= n-1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: routes to every peer did not form in time", srv.Name)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return c
}

// routesUp reads how many distinct peers a server has a route to, from
// its /routez monitoring endpoint (0 on any error). Route pooling can
// open several connections per peer, so peers are counted by id.
func routesUp(monitor string) int {
	resp, err := http.Get("http://" + monitor + "/routez")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var body struct {
		Routes []struct {
			RemoteID string `json:"remote_id"`
		} `json:"routes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0
	}
	peers := make(map[string]struct{}, len(body.Routes))
	for _, r := range body.Routes {
		peers[r.RemoteID] = struct{}{}
	}
	return len(peers)
}
