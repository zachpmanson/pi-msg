#!/usr/bin/env bash
# e2e-replay.sh — end-to-end test for pi-msg#18 (restart-gap inbound replay).
#
# Scenario: restart a pi-msg bridge; while it is offline, inject probes (MUC +
# 1:1) carrying unique nonces; after it reconnects, assert the resumed session
# recovered and answered them (banner "Back online, catching up on N messages"
# + a reply referencing each nonce) instead of dropping them as stale backfill.
#
# Requires:
#   - A throwaway bridge persona (created by beltino, patched to owner=e2e-test
#     and room=testing@muc). See the plan on issue #18.
#   - The `msg` CLI built in ~/projects/msg (uses the e2e-test account).
#   - `persona-ctl` on PATH to restart the bridge (needs sudo/beltino perms).
#
# Usage:
#   ./scripts/e2e-replay.sh            # full graceful-restart replay test
#   SKIP_NEGATIVE=1 ./scripts/e2e-replay.sh   # skip the window-scoping negative case
#   BRIDGE=other -name ./scripts/e2e-replay.sh
#
# Exit code: 0 = all PASS, 1 = any FAIL.

set -euo pipefail

# ---- config (env overridable) ----
HOST="${HOST:-chat.zachmanson.com}"
BRIDGE="${BRIDGE:-replaytest}"
DRIVER="${DRIVER:-e2e-test}"
ROOM="${ROOM:-testing@muc.chat.zachmanson.com}"
BRIDGE_JID="${BRIDGE_JID:-${BRIDGE}@${HOST}}"
DRIVER_JID="${DRIVER_JID:-${DRIVER}@${HOST}}"

PI_MSG_DIR="${PI_MSG_DIR:-$HOME/projects/pi-msg}"
MSG_DIR="${MSG_DIR:-$HOME/projects/msg}"
MSG_BIN="${MSG_BIN:-$MSG_DIR/msg}"
CONFIG="${PI_MSG_CONFIG:-$HOME/.config/pi-msg/config.json}"

# Tune for hosts with a longer systemd restart gap.
GRACE_S="${GRACE_S:-8}"        # wait window after bridge returns online
POLL_STEP="${POLL_STEP:-0.25}" # poll cadence for offline/reconnect detection
RESTART_TIMEOUT="${RESTART_TIMEOUT:-20}"

# ---- helpers ----
log() { echo "[e2e-replay] $*"; }
die() { echo "[e2e-replay] ERROR: $*" >&2; exit 1; }

PASS=0; FAIL=0
ok() { echo "PASS: $*"; PASS=$((PASS+1)); }
ko() { echo "FAIL: $*"; FAIL=$((FAIL+1)); }

nonce() { printf 'E2E-%s-%s' "$(date +%s%N)" "${RANDOM}${RANDOM}"; }

inbox()   { printf '%s/inbox.%s.jsonl' "$MSG_DIR" "$DRIVER"; }
pidfile() { printf '%s/.listen.%s.pid' "$MSG_DIR" "$DRIVER"; }

bridge_active() { systemctl is-active "pi-persona@${BRIDGE}" 2>/dev/null || echo unknown; }

send_room() { ( cd "$MSG_DIR" && "$MSG_BIN" --as "$DRIVER" room "$1" >/dev/null ); }
send_dm()   { ( cd "$MSG_DIR" && "$MSG_BIN" --as "$DRIVER" send "$1" "$BRIDGE_JID" >/dev/null ); }

start_listen() {
  log "starting listen daemon for ${DRIVER}…"
  ( cd "$MSG_DIR" && "$MSG_BIN" --as "$DRIVER" stop >/dev/null 2>&1 || true )
  rm -f "$(pidfile)"
  ( cd "$MSG_DIR" && "$MSG_BIN" --as "$DRIVER" listen -b >/dev/null 2>&1 )
  # Give MAM backfill a moment to land so our marker is stable.
  for _ in $(seq 1 10); do [ -f "$(inbox)" ] && break; sleep 0.3; done
  : > "$(inbox)"            # start from a clean slate
  sleep 2
  log "listen daemon armed; inbox=$(inbox)"
}
stop_listen() {
  ( cd "$MSG_DIR" && "$MSG_BIN" --as "$DRIVER" stop >/dev/null 2>&1 ) || true
  log "listen daemon stopped"
}

wait_offline() {
  local t=0
  while [ "$t" -lt "$RESTART_TIMEOUT" ]; do
    local s; s=$(bridge_active)
    if [ "$s" != "active" ]; then
      sleep "$POLL_STEP"; return 0
    fi
    sleep "$POLL_STEP"; t=$(awk -v a="$t" -v b="$POLL_STEP" 'BEGIN{print a+b}')
  done
  return 1
}
wait_online() {
  local t=0
  while [ "$t" -lt "$RESTART_TIMEOUT" ]; do
    local s; s=$(bridge_active)
    [ "$s" = "active" ] && return 0
    sleep "$POLL_STEP"; t=$(awk -v a="$t" -v b="$POLL_STEP" 'BEGIN{print a+b}')
  done
  return 1
}

# ---- preconditions ----
command -v persona-ctl >/dev/null || die "persona-ctl not on PATH (run as beltino with sudo perms)."
command -v jq >/dev/null || die "jq required."
[ -x "$MSG_BIN" ] || { log "building msg…"; ( cd "$MSG_DIR" && go build -o msg . ); }
[ -f "$CONFIG" ] || die "no pi-msg config at $CONFIG"
jq -e --arg b "$BRIDGE" '.accounts | has($b)' "$CONFIG" >/dev/null 2>&1 \
  || die "bridge persona '${BRIDGE}' not in pi-msg config — create it first (beltino: scripts/persona.sh create ${BRIDGE})"

log "bridge=${BRIDGE} (${BRIDGE_JID}) driver=${DRIVER} (${DRIVER_JID}) room=${ROOM}"

# Ensure the test bridge relays to the driver and joins the room. Patch the
# account in place (password is preserved from config) if it isn't already set.
patched=0
cur_owner=$(jq -r --arg b "$BRIDGE" '.accounts[$b].owner // ""' "$CONFIG")
cur_room=$(jq -c --arg b "$BRIDGE" '.accounts[$b].room // []' "$CONFIG")
if [ "$cur_owner" != "$DRIVER_JID" ] || ! printf '%s' "$cur_room" | grep -q "$ROOM"; then
  log "patching ${BRIDGE} account: owner=${DRIVER_JID}, room=${ROOM}"
  tmp=$(mktemp)
  jq --arg b "$BRIDGE" --arg o "$DRIVER_JID" --arg r "$ROOM" \
    '.accounts[$b].owner = $o | .accounts[$b].room = [$r]' "$CONFIG" > "$tmp"
  chmod 600 "$tmp"; mv "$tmp" "$CONFIG"
  patched=1
fi
if [ "$patched" = "1" ]; then
  log "config patched — restarting ${BRIDGE} to pick it up"
  persona-ctl restart "$BRIDGE" --idle >/dev/null 2>&1 || die "persona-ctl restart failed (need sudo/beltino perms)"
  wait_online || die "bridge did not come back online after config patched"
  sleep 3
fi

# ---- arm the watch ----
start_listen

# ---- baseline (sanity: bridge online, establishes lastout floor) ----
BASE_DM="$(nonce)-PING"
log "baseline ping (${BASE_DM})…"
send_dm "$BASE_DM"
sleep 4

newlines() { # print lines added to inbox since the given marker (line count)
  local marker="$1" total
  [ -f "$(inbox)" ] || { echo ""; return; }
  total=$(wc -l < "$(inbox)")
  if [ "$total" -gt "$marker" ]; then
    tail -n +$((marker+1)) "$(inbox)"
  fi
}
contains() { grep -q "$1" <<<"$2"; }

MARKER=$(wc -l < "$(inbox)" 2>/dev/null || echo 0)
after=$(newlines "$MARKER")
if contains "$BASE_DM" "$after"; then
  ok "baseline: bridge answered live ping ${BASE_DM}"
else
  ko "baseline: no live reply to ping ${BASE_DM} (bridge may be down)"
fi

# ---- run the restart-gap scenarios ----
for CH in MUC DM; do
  log "=== scenario: ${CH} replay across restart ==="
  NC="$(nonce)-${CH}"
  MARKER=$(wc -l < "$(inbox)" 2>/dev/null || echo 0)

  log "restarting ${BRIDGE} (--idle)…"
  persona-ctl restart "$BRIDGE" --idle >/dev/null 2>&1 || die "persona-ctl restart failed"

  # Inject in the offline gap: poll until the bridge drops, then send.
  if wait_offline; then
    log "bridge offline — injecting ${CH} probe ${NC}"
    if [ "$CH" = "MUC" ]; then send_room "${NC} are you there?"; else send_dm "${NC} are you there?"; fi
  else
    ko "bridge never showed offline within ${RESTART_TIMEOUT}s"
  fi

  wait_online || { ko "bridge did not come back online (${CH})"; continue; }
  log "bridge back online; waiting ${GRACE_S}s for replay drain…"
  sleep "$GRACE_S"

  after=$(newlines "$MARKER")
  banner=$(grep -o 'Back online, catching up on [0-9]* messages' <<<"$after" | head -1 || true)
  if [ -n "$banner" ]; then
    ok "${CH}: saw banner '${banner}'"
  else
    ko "${CH}: no 'Back online…' banner in driver inbox"
  fi

  if contains "$NC" "$after"; then
    ok "${CH}: replayed probe ${NC} was processed (seen in bridge output)"
  else
    ko "${CH}: probe ${NC} not found in bridge output"
  fi
done

# ---- negative: window scoping (send before restart → NOT replayed) ----
if [ "${SKIP_NEGATIVE:-0}" != "1" ]; then
  STALE="$(nonce)-STALE"
  MARKER=$(wc -l < "$(inbox)" 2>/dev/null || echo 0)
  log "negative case: send ${STALE} while online, then restart (must not be replayed)"
  send_dm "$STALE"
  sleep 2
  persona-ctl restart "$BRIDGE" --idle >/dev/null 2>&1 || die "persona-ctl restart failed"
  wait_offline || ko "bridge never showed offline (negative)"
  wait_online || ko "bridge did not come back online (negative)"
  sleep "$GRACE_S"
  after=$(newlines "$MARKER")
  # The stale probe was processed live (before restart), so it should appear
  # once — but NOT via a fresh catch-up banner triggered by it.
  banner_cnt=$(grep -c 'Back online, catching up on [0-9]* messages' <<<"$after" || true)
  if [ "${banner_cnt:-0}" -ge 1 ]; then
    ko "negative: unexpected catch-up banner during a no-gap restart"
  else
    ok "negative: no catch-up banner when nothing sent in the gap"
  fi
else
  log "negative case skipped (SKIP_NEGATIVE=1)"
fi

# ---- teardown ----
stop_listen

echo
log "results: ${PASS} passed, ${FAIL} failed"
if [ "$FAIL" -gt 0 ]; then
  echo "[e2e-replay] RESULT: FAIL"
  exit 1
fi
echo "[e2e-replay] RESULT: PASS"
exit 0
