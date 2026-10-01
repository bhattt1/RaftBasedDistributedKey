#!/usr/bin/env bash
# Generates DEVELOPMENT certificates and a client token for secure mode.
#
#   scripts/gen-dev-certs.sh [output-dir] [node...]
#
# Defaults: output to ./certs, nodes n1 n2 n3. The output directory is
# git-ignored; private keys and the token must never be committed.
#
# What it creates:
#   ca.pem / ca.key        a certificate authority, valid 30 days
#   <node>.pem / .key      one certificate per node. Its common name is the
#                          node ID, which is what peers check each other's
#                          identity against. It is valid for the names
#                          <node>, localhost and 127.0.0.1, as both a TLS
#                          server and a TLS client.
#   client.token           a random bearer token for the HTTP API
#
# These are for local experiments. In a real deployment the CA key would not
# sit next to the node keys, and certificates would come from your own PKI.
set -euo pipefail

OUT="${1:-certs}"
shift || true
NODES=("$@")
((${#NODES[@]})) || NODES=(n1 n2 n3)
DAYS=30

command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 2; }
umask 077
mkdir -p "$OUT"

if [[ -f "$OUT/ca.key" ]]; then
  echo "$OUT/ca.key already exists; remove the directory to regenerate" >&2
  exit 1
fi

openssl ecparam -name prime256v1 -genkey -noout -out "$OUT/ca.key"
openssl req -x509 -new -key "$OUT/ca.key" -sha256 -days "$DAYS" \
  -subj "/CN=raft-kv development CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out "$OUT/ca.pem"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

for node in "${NODES[@]}"; do
  openssl ecparam -name prime256v1 -genkey -noout -out "$OUT/$node.key"
  openssl req -new -key "$OUT/$node.key" -subj "/CN=$node" -out "$tmp/$node.csr"
  cat >"$tmp/$node.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:$node,DNS:localhost,IP:127.0.0.1
EOF
  openssl x509 -req -in "$tmp/$node.csr" -CA "$OUT/ca.pem" -CAkey "$OUT/ca.key" \
    -CAcreateserial -CAserial "$tmp/ca.srl" -sha256 -days "$DAYS" \
    -extfile "$tmp/$node.ext" -out "$OUT/$node.pem" 2>/dev/null
  openssl verify -CAfile "$OUT/ca.pem" "$OUT/$node.pem" >/dev/null
done

# 32 random bytes, hex encoded: 64 characters.
openssl rand -hex 32 >"$OUT/client.token"

# Certificates are public; keys and the token stay private to this user.
chmod 0644 "$OUT"/*.pem
chmod 0600 "$OUT"/*.key "$OUT/client.token"

echo "wrote development certificates for ${NODES[*]} and a client token to $OUT/ (valid $DAYS days)"
