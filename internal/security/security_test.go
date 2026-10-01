package security

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/testcerts"
)

// handshake runs one TLS connection from a client configuration to a server
// configuration over loopback and returns both sides' errors.
func handshake(t *testing.T, server, client *tls.Config) (serverErr, clientErr error) {
	t.Helper()
	lis, err := tls.Listen("tcp", "127.0.0.1:0", server)
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		err = conn.(*tls.Conn).Handshake()
		if err == nil {
			// In TLS 1.3 the server learns that the client rejected it only
			// when it next reads.
			_, err = conn.Read(make([]byte, 1))
		}
		done <- err
	}()
	conn, clientErr := tls.Dial("tcp", lis.Addr().String(), client)
	if clientErr == nil {
		_, clientErr = conn.Write([]byte("x"))
		conn.Close()
	}
	return <-done, clientErr
}

func peerConfigs(t *testing.T, ca *testcerts.CA, id string) (*tls.Config, func(string) *tls.Config) {
	t.Helper()
	cert, key := ca.Issue(id)
	server, client, err := PeerTLS(ca.File, cert, key, id)
	if err != nil {
		t.Fatalf("PeerTLS for %s: %v", id, err)
	}
	return server, client
}

func TestPeersAuthenticateEachOther(t *testing.T) {
	ca := testcerts.NewCA(t, t.TempDir(), "cluster")
	n1Server, _ := peerConfigs(t, ca, "n1")
	_, n2Client := peerConfigs(t, ca, "n2")

	if serverErr, clientErr := handshake(t, n1Server, n2Client("n1")); serverErr != nil || clientErr != nil {
		t.Fatalf("n2 dialling n1: server error %v, client error %v", serverErr, clientErr)
	}
}

func TestDiallerRejectsAServerThatIsNotTheIntendedPeer(t *testing.T) {
	ca := testcerts.NewCA(t, t.TempDir(), "cluster")
	n3Server, _ := peerConfigs(t, ca, "n3")
	_, n2Client := peerConfigs(t, ca, "n2")

	// n2 means to reach n1 but the address answers with n3's certificate:
	// a legitimate cluster member, but not the one that was dialled.
	_, clientErr := handshake(t, n3Server, n2Client("n1"))
	if clientErr == nil || !strings.Contains(clientErr.Error(), `identifies "n3"`) {
		t.Fatalf("client error = %v, want a rejection naming the certificate's identity", clientErr)
	}
}

func TestServerRejectsClientsWithoutAClusterCertificate(t *testing.T) {
	dir := t.TempDir()
	ca := testcerts.NewCA(t, dir, "cluster")
	n1Server, _ := peerConfigs(t, ca, "n1")

	t.Run("no client certificate", func(t *testing.T) {
		pool := mustPool(t, ca.File)
		serverErr, clientErr := handshake(t, n1Server, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13})
		if serverErr == nil && clientErr == nil {
			t.Fatal("a client with no certificate completed a peer connection")
		}
	})
	t.Run("certificate from another CA", func(t *testing.T) {
		other := testcerts.NewCA(t, dir, "intruder")
		cert, key := other.Issue("n2")
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			t.Fatal(err)
		}
		// The intruder trusts the real CA (to get past its own checks) and
		// presents a certificate naming a real node, signed by its own CA.
		serverErr, clientErr := handshake(t, n1Server, &tls.Config{RootCAs: mustPool(t, ca.File), Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13})
		if serverErr == nil && clientErr == nil {
			t.Fatal("a certificate signed by a foreign CA was accepted")
		}
	})
	t.Run("server certificate from another CA", func(t *testing.T) {
		other := testcerts.NewCA(t, dir, "impostor")
		cert, key := other.Issue("n1")
		impostor, _, err := PeerTLS(other.File, cert, key, "n1")
		if err != nil {
			t.Fatal(err)
		}
		_, n2Client := peerConfigs(t, ca, "n2")
		if _, clientErr := handshake(t, impostor, n2Client("n1")); clientErr == nil {
			t.Fatal("a server certificate signed by a foreign CA was accepted")
		}
	})
}

func TestPeerTLSRejectsCertificateForAnotherNode(t *testing.T) {
	ca := testcerts.NewCA(t, t.TempDir(), "cluster")
	cert, key := ca.Issue("n2")
	if _, _, err := PeerTLS(ca.File, cert, key, "n1"); err == nil || !strings.Contains(err.Error(), `is for "n2"`) {
		t.Fatalf("node n1 accepted n2's certificate as its own: %v", err)
	}
}

func TestPeerTLSNeverDisablesVerification(t *testing.T) {
	ca := testcerts.NewCA(t, t.TempDir(), "cluster")
	server, client := peerConfigs(t, ca, "n1")
	c := client("n2")
	if c.InsecureSkipVerify {
		t.Fatal("the client configuration skips certificate verification")
	}
	if server.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("server ClientAuth = %v, want RequireAndVerifyClientCert", server.ClientAuth)
	}
	if server.MinVersion != tls.VersionTLS13 || c.MinVersion != tls.VersionTLS13 {
		t.Fatal("peer connections must require TLS 1.3")
	}
}

func TestLoadToken(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	good := strings.Repeat("s", MinTokenLength)
	if got, err := LoadToken(write("good", good+"\n")); err != nil || got != good {
		t.Fatalf("LoadToken = %q, %v; want the token without its trailing newline", got, err)
	}
	if _, err := LoadToken(write("short", "hunter2")); err == nil {
		t.Fatal("a 7-character token was accepted")
	}
	if _, err := LoadToken(write("empty", "\n")); err == nil {
		t.Fatal("an empty token was accepted")
	}
	if _, err := LoadToken(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("a missing token file was accepted")
	}
}

func TestTokenMatches(t *testing.T) {
	if !TokenMatches("correct-horse", "correct-horse") {
		t.Fatal("equal tokens did not match")
	}
	for _, presented := range []string{"", "correct-hors", "correct-horse ", "Correct-horse"} {
		if TokenMatches(presented, "correct-horse") {
			t.Fatalf("%q matched", presented)
		}
	}
}

func mustPool(t *testing.T, caFile string) *x509.CertPool {
	t.Helper()
	pem, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatalf("%s contains no certificates", caFile)
	}
	return pool
}
