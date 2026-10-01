// Package security builds the TLS configurations and loads the client token
// used in secure mode.
//
// Peer connections use mutual TLS. Every node holds a certificate signed by
// the cluster's CA whose common name is its node ID. A node accepts a peer
// connection only from a certificate that chains to that CA, and it accepts
// a Raft message only if the sender named in the message is the identity in
// the certificate. When dialling, a node additionally requires the server's
// certificate to carry the ID of the peer it meant to reach.
package security

import (
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

// PeerTLS returns the server-side configuration for the peer listener and a
// function producing the client-side configuration for dialling one peer.
func PeerTLS(caFile, certFile, keyFile, nodeID string) (server *tls.Config, client func(peerID string) *tls.Config, err error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, nil, fmt.Errorf("peer CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("peer CA %s contains no certificates", caFile)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("peer certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, nil, fmt.Errorf("peer certificate: %w", err)
	}
	if leaf.Subject.CommonName != nodeID {
		return nil, nil, fmt.Errorf("peer certificate is for %q but this node is %q", leaf.Subject.CommonName, nodeID)
	}

	server = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	client = func(peerID string) *tls.Config {
		return &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			// Standard verification (chain and host name) still runs first.
			// This adds one more requirement: the server must be the peer
			// we intended to dial, not merely some member of the cluster.
			VerifyConnection: func(cs tls.ConnectionState) error {
				if len(cs.PeerCertificates) == 0 {
					return errors.New("peer presented no certificate")
				}
				if got := cs.PeerCertificates[0].Subject.CommonName; got != peerID {
					return fmt.Errorf("dialled peer %q but the certificate identifies %q", peerID, got)
				}
				return nil
			},
		}
	}
	return server, client, nil
}

// HTTPServerTLS returns the configuration for the HTTPS client API.
func HTTPServerTLS(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("HTTP certificate: %w", err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}, nil
}

// MinTokenLength is the shortest client token accepted.
const MinTokenLength = 24

// LoadToken reads the client bearer token from a file. Keeping it in a file
// rather than a flag keeps it out of process listings and shell history.
func LoadToken(file string) (string, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("client token: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if len(token) < MinTokenLength {
		return "", fmt.Errorf("client token in %s is %d characters; use at least %d", file, len(token), MinTokenLength)
	}
	return token, nil
}

// TokenMatches compares a presented token with the expected one in constant
// time, so response timing does not reveal how much of a guess was right.
func TokenMatches(presented, expected string) bool {
	return subtle.ConstantTimeCompare([]byte(presented), []byte(expected)) == 1
}
