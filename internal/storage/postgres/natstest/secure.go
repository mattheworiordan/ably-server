//go:build integration

package natstest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Security is what StartSecure makes the server require: a user and
// password (both or neither), and TLS with TLS's server certificate,
// verifying client certificates against TLS's CA when VerifyClients.
type Security struct {
	User, Pass    string
	TLS           *TLSFiles
	VerifyClients bool
}

// TLSFiles are PEM files NewTLS writes: a CA, a server certificate for
// localhost and 127.0.0.1 signed by it, and a client certificate signed
// by it, each with its key.
type TLSFiles struct {
	CA, ServerCert, ServerKey, ClientCert, ClientKey string
}

// StartSecure starts a NATS server, per test, that requires sec, and
// returns its handle. The URL carries no credentials. The server is
// removed on t.Cleanup.
func StartSecure(t *testing.T, sec Security) *Server {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var (
		args  []string
		files []testcontainers.ContainerFile
	)
	if sec.User != "" {
		args = append(args, "--user", sec.User, "--pass", sec.Pass)
	}
	if sec.TLS != nil {
		for _, f := range []struct{ host, ctr string }{
			{sec.TLS.ServerCert, "/certs/server.pem"},
			{sec.TLS.ServerKey, "/certs/server-key.pem"},
			{sec.TLS.CA, "/certs/ca.pem"},
		} {
			files = append(files, testcontainers.ContainerFile{HostFilePath: f.host, ContainerFilePath: f.ctr, FileMode: 0o644})
		}
		args = append(args, "--tls", "--tlscert", "/certs/server.pem", "--tlskey", "/certs/server-key.pem")
		if sec.VerifyClients {
			args = append(args, "--tlsverify", "--tlscacert", "/certs/ca.pem")
		}
	}
	ctr, err := testcontainers.Run(ctx, "nats:2.11-alpine",
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithFiles(files...),
		testcontainers.WithCmd(args...),
		testcontainers.WithWaitStrategy(wait.ForLog("Server is ready")),
	)
	if err != nil {
		t.Fatalf("start secure nats container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("nats container host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "4222/tcp")
	if err != nil {
		t.Fatalf("nats container port: %v", err)
	}
	addr := net.JoinHostPort(host, port.Port())
	return &Server{URL: "nats://" + addr, Addr: addr}
}

// NewTLS writes a fresh CA, a server certificate for localhost and
// 127.0.0.1 and a client certificate, all signed by the CA, as PEM files
// in a test temp dir.
func NewTLS(t *testing.T) *TLSFiles {
	t.Helper()
	dir := t.TempDir()
	write := func(name, typ string, der []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return path
	}
	key := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		return k
	}
	keyDER := func(k *ecdsa.PrivateKey) []byte {
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			t.Fatalf("marshal key: %v", err)
		}
		return der
	}
	serialNo := int64(1)
	template := func(cn string) *x509.Certificate {
		serialNo++
		return &x509.Certificate{
			SerialNumber: big.NewInt(serialNo),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
		}
	}

	caKey := key()
	caTmpl := template("natstest CA")
	caTmpl.IsCA, caTmpl.BasicConstraintsValid = true, true
	caTmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	leaf := func(cn string, usage x509.ExtKeyUsage, hosts bool) ([]byte, *ecdsa.PrivateKey) {
		k := key()
		tmpl := template(cn)
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{usage}
		if hosts {
			tmpl.DNSNames = []string{"localhost"}
			tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback}
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &k.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create %s certificate: %v", cn, err)
		}
		return der, k
	}
	serverDER, serverKey := leaf("natstest server", x509.ExtKeyUsageServerAuth, true)
	clientDER, clientKey := leaf("natstest client", x509.ExtKeyUsageClientAuth, false)
	return &TLSFiles{
		CA:         write("ca.pem", "CERTIFICATE", caDER),
		ServerCert: write("server.pem", "CERTIFICATE", serverDER),
		ServerKey:  write("server-key.pem", "PRIVATE KEY", keyDER(serverKey)),
		ClientCert: write("client.pem", "CERTIFICATE", clientDER),
		ClientKey:  write("client-key.pem", "PRIVATE KEY", keyDER(clientKey)),
	}
}
