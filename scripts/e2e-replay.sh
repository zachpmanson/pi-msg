#!/usr/bin/env bash
# e2e-replay.sh — end-to-end test for pi-msg#18 (restart-gap inbound replay).
#
# The supported recovery path is the OWNER 1:1: when a bridge is down, a DM is
# held by the server's offline store; on resume the swap-window replay handfs it
# to the resumed session and emits the deterministic banner
#     "Back online, catching up on N messages"
# (only fired when DrainReplay returns >=1 recovered message).
#
# Test strategy: STOP the bridge for a real, controllable offline window (GAP_S),
# inject probes while it is truly down, then START it and assert the resumed
# session recovered them. The service's live `systemctl restart` is near-atomic
# (offline gap ~4ms) so it can never be caught by polling — hence stop/start.
#
# Known limitation (documented, not a feature guarantee): MUC room messages sent
# while the bridge is down are NOT recovered — joinRoom joins the room with
# maxstanzas=0 (history suppressed), so the delayed-room buffer has nothing to
# collect. We assert the OPPOSITE (no banner) for a gap room message so the
# limitation is pinned without a red suite.
#
# Requires:
#   - A throwaway droid bridge (b1-1..b1-4 via scripts/droid-up.sh from the
#     beltino repo), owner patched to the driver and room=testing@muc.
#   - The `msg` CLI built in ~/projects/msg (uses a droid as driver; driods
#     self-puppet via msg --as b1-N).
#   - `persona-ctl` on PATH to stop/start the bridge (needs sudo/beltino perms).
#   - A clean bridge session (no queued turn backlog) for a reliable baseline.
#
# Usage:
#   ./scripts/e2e-replay.sh                # full replay test
#   SKIP_NEGATIVE=1 ./scripts/e2e-replay.sh   # skip the window-scoping negative case
#   BRIDGE=other ./scripts/e2e-replay.sh
#
# Exit code: 0 = all PASS, 1 = any FAIL.

set -euo pipefail

# ---- config (env overridable) ----
HOST="${HOST:-chat.zachmanson.com}"
BRIDGE="${BRIDGE:?set BRIDGE to the throwaway bridge persona JID localpart}"
DRIVER="${DRIVER:-b1-1}"
ROOM="${ROOM:-testing@muc.chat.zachmanson.com}"
BRIDGE_JID="${BRIDGE_JID:-${BRIDGE}@${HOST}}"
DRIVER_JID="${DRIVER_JID:-${DRIVER}@${HOST}}"

PI_MSG_DIR="${PI_MSG_DIR:-$HOME/projects/pi-msg}"
MSG_DIR="${MSG_DIR:-$HOME/projects/msg}"
MSG_BIN="${MSG_BIN:-$MSG_DIR/msg}"
CONFIG="${PI_MSG_CONFIG:-$HOME/.config/pi-msg/config.json}"

GAP_S="${GAP_S:-6}"            # how long the bridge stays stopped (offline window)
GRACE_S="${GRACE_S:-30}"        # wait AFTER re-online for replay banner + LLM replies
POLL_STEP="${POLL_STEP:-0.25}"  # poll cadence for offline/reconnect detection
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

newlines() { # print lines added to inbox since the given marker (line count)
  local marker="$1" total
  [ -f "$(inbox)" ] || { echo ""; return; }
  total=$(wc -l < "$(inbox)")
  if [ "$total" -gt "$marker" ]; then
    tail -n +$((marker+1)) "$(inbox)"
  fi
}
contains() { grep -q "$1" <<<"$2"; }
# bridge_lines filters inbox JSONL down to bridge-authored lines (the bot JID in
# the from field: bare 1:1 ${BRIDGE}@… or its MUC nick …/${BRIDGE}).
bridge_lines() { grep -E '"from":"[^"]*${BRIDGE}@' <<< "$1"; }
# has_bridge_line asserts a pattern exists on a bridge-authored line.
has_bridge_line() { bridge_lines "$1" | grep -q "$2"; }

max_iters() { awk -v t="$RESTART_TIMEOUT" -v p="$POLL_STEP" 'BEGIN{print int(t/p)}'; }
wait_offline() {
  local i=0 max; max=$(max_iters)
  while [ "$i" -lt "$max" ]; do
    local s; s=$(bridge_active)
    if [ "$s" != "active" ]; then
      return 0
    fi
    sleep "$POLL_STEP"; i=$((i+1))
  done
  return 1
}
wait_online() {
  local i=0 max; max=$(max_iters)
  while [ "$i" -lt "$max" ]; do
    local s; s=$(bridge_active)
    [ "$s" = "active" ] && return 0
    sleep "$POLL_STEP"; i=$((i+1))
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

# ---- baseline (sanity: bridge online and answering on the 1:1) ----
BASE_DM="$(nonce)-PING"
log "baseline ping (${BASE_DM})…"
MARKER=$(wc -l < "$(inbox)" 2>/dev/null || echo 0)
send_dm "$BASE_DM"
# The bot's reply is a free-text LLM turn; the FIRST one on a cold pi session is
# very slow (can exceed 35s). Poll up to WAIT_S instead of one fixed sleep.
WAIT_S="${WAIT_S:-70}"
saw=0; t=0
while [ "$t" -lt "$WAIT_S" ]; do
  if has_bridge_line "$(newlines "$MARKER")" '.'; then
    ok "baseline: bridge answered the live ping (saw bridge output)"
    saw=1; break
  fi
  sleep 3; t=$((t+3))
done
[ "$saw" = 1 ] || ko "baseline: no bridge reply to ping within ${WAIT_S}s (bridge down or session cold-stuck)"

# ---- restart-gap replay: the 1:1 owner DM is the supported recovery path ----
for CH in DM; do
  log "=== scenario: ${CH} replay across restart ==="
  NC="$(nonce)-${CH}"
  MARKER=$(wc -l < "$(inbox)" 2>/dev/null || echo 0)

  log "stopping ${BRIDGE} (offline window ${GAP_S}s)…"
  persona-ctl stop "$BRIDGE" >/dev/null 2>&1 || ko "persona-ctl stop failed (${CH})"
  if wait_offline; then
    log "bridge offline — holding ${GAP_S}s, injecting ${CH} probe"
    sleep "$GAP_S"
    send_dm "replay check: reply with exactly the token ${NC}"
  else
    ko "bridge never showed offline within ${RESTART_TIMEOUT}s"
  fi

  persona-ctl start "$BRIDGE" >/dev/null 2>&1 || ko "persona-ctl start failed (${CH})"
  wait_online || { ko "bridge did not come back online (${CH})"; continue; }
  log "bridge back online; waiting ${GRACE_S}s for replay drain…"
  sleep "$GRACE_S"

  after=$(newlines "$MARKER")
  if has_bridge_line "$after" 'Back online, catching up on [1-9][0-9]* messages'; then
    banner=$(grep -o 'Back online, catching up on [1-9][0-9]* messages' <<<"$after" | head -1)
    ok "${CH}: recovered + replayed — saw banner '${banner}'"
  else
    ko "${CH}: no catch-up banner (recovered 0 messages)"
  fi

  # Secondary: the replayed probe should be handed to the resumed session and
  # answered. Bot replies are free-text, so the probe asks for the token back.
  if contains "$NC" "$(bridge_lines "$after")"; then
    ok "${CH}: replayed probe ${NC} echoed by bridge"
  else
    ko "${CH}: probe ${NC} not echoed in bridge output"
  fi
done

# ---- MUC known limitation (documented, not a feature pass) ----
# Room messages sent while the bridge is down are NOT recovered: joinRoom joins
# MUC with maxstanzas=0 (history suppressed), so the delayed-room buffer has
# nothing to collect. Pin it as a non-recovery without a red suite.
if [ "${SKIP_MUC_LIMIT:-0}" != "1" ]; then
  NC="$(nonce)-MUC-LIMIT"
  MARKER=$(wc -l < "$(inbox)" 2>/dev/null || echo 0)
  log "MUC known-limitation check: gap room message must NOT be replayed"
  persona-ctl stop "$BRIDGE" >/dev/null 2>&1
  if wait_offline; then
    sleep "$GAP_S"
    send_room "replay check: reply with exactly the token ${NC}"
  else
    ko "MUC: bridge never showed offline"
  fi
  persona-ctl start "$BRIDGE" >/dev/null 2>&1
  wait_online || ko "MUC: bridge did not come back online"
  sleep "$GRACE_S"
  after=$(newlines "$MARKER")
  banner_cnt=$(grep -c 'Back online, catching up on [1-9][0-9]* messages' <<<"$after" || true)
  if [ "${banner_cnt:-0}" -ge 1 ]; then
    ko "MUC: unexpected replay banner (gap room message was recovered?)"
  else
    ok "MUC: no replay banner for gap room message (known limitation)"
  fi
fi

# ---- negative: window scoping (send before restart → NOT replayed) ----
if [ "${SKIP_NEGATIVE:-0}" != "1" ]; then
  STALE="$(nonce)-STALE"
  MARKER=$(wc -l < "$(inbox)" 2>/dev/null || echo 0)
  log "negative case: send ${STALE} while online, then restart (must not be replayed)"
  send_dm "$STALE"
  sleep 2
  persona-ctl stop "$BRIDGE" >/dev/null 2>&1
  wait_offline || ko "bridge never showed offline (negative)"
  sleep "$GAP_S"
  persona-ctl start "$BRIDGE" >/dev/null 2>&1
  wait_online || ko "bridge did not come back online (negative)"
  sleep "$GRACE_S"
  after=$(newlines "$MARKER")
  # The stale probe was processed live (before restart); it must NOT produce a
  # fresh catch-up banner (it arrived before the offline window).
  banner_cnt=$(grep -c 'Back online, catching up on [1-9][0-9]* messages' <<<"$after" || true)
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
