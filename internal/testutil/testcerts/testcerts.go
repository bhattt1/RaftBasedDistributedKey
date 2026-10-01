// Package testcerts generates throwaway certificate authorities and
// certificates for tests. Nothing here is written to the repository; tests
// create what they need in a temporary directory each run.
package testcerts

import (
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
)

// CA is a test certificate authority.
type CA struct {
	t    testing.TB
	dir  string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	// File is the path of the CA certificate in PEM form.
	File string
}

// NewCA creates a certificate authority in dir.
func NewCA(t testing.TB, dir, name string) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	ca := &CA{t: t, dir: dir, cert: cert, key: key, File: filepath.Join(dir, name+"-ca.pem")}
	writePEM(t, ca.File, "CERTIFICATE", der)
	return ca
}

// Issue creates a certificate with the given common name, valid for
// localhost and 127.0.0.1 as both server and client. It returns the paths of
// the certificate and its key.
func (ca *CA) Issue(commonName string) (certFile, keyFile string) {
	ca.t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		ca.t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(ca.t),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost", commonName},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		ca.t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		ca.t.Fatal(err)
	}
	base := filepath.Join(ca.dir, ca.cert.Subject.CommonName+"-"+commonName)
	certFile, keyFile = base+".pem", base+".key"
	writePEM(ca.t, certFile, "CERTIFICATE", der)
	writePEM(ca.t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func serial(t testing.TB) *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 100))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func writePEM(t testing.TB, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
