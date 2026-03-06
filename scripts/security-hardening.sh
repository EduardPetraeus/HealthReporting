#!/usr/bin/env bash
# Security hardening verification script.
# Checks (does not auto-apply) system-level security settings.
#
# Usage: ./scripts/security-hardening.sh

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; }

echo "=== Health Platform Security Hardening Check ==="
echo ""

# 1. Keychain timeout
echo "--- Keychain ---"
KEYCHAIN="$HOME/Library/Keychains/claude.keychain-db"
if [ -f "$KEYCHAIN" ]; then
    pass "claude.keychain-db exists"
    TIMEOUT=$(security show-keychain-info "$KEYCHAIN" 2>&1 | grep -o 'timeout=[0-9]*' | cut -d= -f2 || echo "unknown")
    if [ "$TIMEOUT" = "unknown" ]; then
        warn "Could not read keychain timeout"
    elif [ "$TIMEOUT" -le 3600 ] 2>/dev/null; then
        pass "Keychain timeout: ${TIMEOUT}s (1 hour or less)"
    else
        warn "Keychain timeout: ${TIMEOUT}s — consider: security set-keychain-settings -t 3600 -l '$KEYCHAIN'"
    fi
else
    fail "claude.keychain-db not found at $KEYCHAIN"
fi
echo ""

# 2. FileVault
echo "--- FileVault ---"
FV_STATUS=$(fdesetup status 2>/dev/null || echo "unknown")
if echo "$FV_STATUS" | grep -q "On"; then
    pass "FileVault is enabled"
elif echo "$FV_STATUS" | grep -q "Off"; then
    fail "FileVault is OFF — enable in System Settings > Privacy & Security > FileVault"
else
    warn "Could not determine FileVault status"
fi
echo ""

# 3. DNS
echo "--- DNS ---"
DNS_SERVERS=$(scutil --dns 2>/dev/null | grep 'nameserver\[0\]' | head -1 | awk '{print $3}' || echo "unknown")
if [ "$DNS_SERVERS" = "1.1.1.1" ] || [ "$DNS_SERVERS" = "1.0.0.1" ]; then
    pass "DNS set to Cloudflare ($DNS_SERVERS)"
elif [ "$DNS_SERVERS" = "8.8.8.8" ] || [ "$DNS_SERVERS" = "8.8.4.4" ]; then
    pass "DNS set to Google ($DNS_SERVERS)"
else
    warn "DNS server: $DNS_SERVERS — consider Cloudflare (1.1.1.1) or Google (8.8.8.8)"
fi
echo ""

# 4. Firewall
echo "--- Firewall ---"
FW_STATUS=$(/usr/libexec/ApplicationFirewall/socketfilterfw --getglobalstate 2>/dev/null || echo "unknown")
if echo "$FW_STATUS" | grep -q "enabled"; then
    pass "macOS Firewall is enabled"
else
    fail "macOS Firewall is disabled — enable in System Settings > Network > Firewall"
fi

STEALTH=$(/usr/libexec/ApplicationFirewall/socketfilterfw --getstealthmode 2>/dev/null || echo "unknown")
if echo "$STEALTH" | grep -q "enabled"; then
    pass "Stealth Mode is enabled"
else
    warn "Stealth Mode is disabled — consider enabling for reduced attack surface"
fi
echo ""

echo "=== Check complete ==="
