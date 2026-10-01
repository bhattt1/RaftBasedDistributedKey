#!/usr/bin/env bash
# Runs govulncheck and compares its findings with .vuln-exceptions.
#
# Exit status is non-zero if the scan reports a vulnerability that is not an
# accepted exception, or if an accepted exception is no longer reported. The
# full findings are always printed, exceptions included, so they stay visible.
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$(go env GOPATH)/bin:$PATH"

command -v govulncheck >/dev/null || { echo "govulncheck not found; run scripts/install-tools.sh" >&2; exit 2; }

out="$(mktemp)"
trap 'rm -f "$out"' EXIT

# govulncheck exits 3 when it finds something; that is not a script error.
govulncheck ./... >"$out" 2>&1 || true
cat "$out"

if ! grep -qE 'No vulnerabilities found|Your code is affected by' "$out"; then
  echo "vulncheck: govulncheck did not complete" >&2
  exit 2
fi

mapfile -t found < <(grep -oE '^Vulnerability #[0-9]+: (GO-[0-9]+-[0-9]+)' "$out" | awk '{print $3}' | sort -u)
mapfile -t allowed < <(grep -vE '^\s*(#|$)' .vuln-exceptions | awk '{print $1}' | sort -u)

status=0
for id in "${found[@]}"; do
  if printf '%s\n' "${allowed[@]}" | grep -qx "$id"; then
    echo "vulncheck: $id is an accepted exception (see .vuln-exceptions)"
  else
    echo "vulncheck: $id is NOT an accepted exception" >&2
    status=1
  fi
done
for id in "${allowed[@]}"; do
  if ! printf '%s\n' "${found[@]}" | grep -qx "$id"; then
    echo "vulncheck: exception $id is no longer reported; remove it from .vuln-exceptions" >&2
    status=1
  fi
done

if ((status == 0)); then
  echo "vulncheck: ${#found[@]} finding(s), all accepted exceptions"
fi
exit "$status"
