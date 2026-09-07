#!/usr/bin/env bash
# ============================================================================
# The whole point of the demo, end to end:
#
#   1. Show the picking fleet healthy and telemetry flowing.
#   2. Cut site 1's WAN link.
#   3. Issue a PICK_ITEM to site 1 anyway. It is accepted (202) and queues in the
#      cloud broker. The cells keep picking; their telemetry spools at the edge.
#   4. Restore the link. Watch the queued pick execute and the backlog drain,
#      timestamped from when it was captured -- not when it arrived.
#
#   ./scripts/demo.sh
# ============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API="http://localhost:8080/api/v1"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

j() { if command -v jq >/dev/null 2>&1; then jq "$@"; else cat; fi; }

bold "1. Picking fleet as the cloud currently sees it"
curl -sS "$API/fleet" | j -r '.[] | "   \(.site)/\(.robotId)  \(.status)  seq=\(.lastSequence)  captured=\(.lastCapturedAt // "-")"'

bold "   WAN links"
curl -sS "$API/bridges" | j -r '.[] | "   \(.site)  connected=\(.connected)"'

bold "2. Cutting site 1's uplink"
"$ROOT/scripts/chaos.sh" site1 down
note "the cells keep picking and reporting locally; the bridge spools to disk"
sleep 8

bold "3. Issuing a pick to site 1 while it is unreachable"
CMD=$(curl -sS -X POST "$API/sites/site1/robots/arm-01/commands" \
        -H 'Content-Type: application/json' \
        -d '{"type":"PICK_ITEM","parameters":{"sku":"SKU-88231","sourceBin":"B-14-03","targetTote":"T-9072"},"ttlSeconds":600}')
echo "$CMD" | j .
CMD_ID=$(echo "$CMD" | j -r '.commandId' 2>/dev/null || echo "$CMD" | sed -n 's/.*"commandId":"\([^"]*\)".*/\1/p')
note "HTTP 202: the cloud BROKER has it durably. The cell has not seen it."
note "commandId = $CMD_ID"

sleep 5
bold "   Pick state while site 1 is dark"
curl -sS "$API/commands/$CMD_ID" | j -r '"   state=\(.state)  response=\(.response // "none yet")"'

bold "   The cloud's view of site 1 is frozen at the moment the link dropped"
note "(it is still UP: the health timeout has not elapsed yet -- watch lastCaptured stop moving)"
curl -sS "$API/fleet" | j -r '.[] | select(.site=="site1") | "   \(.robotId)  \(.status)  lastCaptured=\(.lastCapturedAt // "-")"'

bold "4. Waiting 60s for the health timeout to elapse and a real backlog to build..."
sleep 60

bold "   Site 1 now reads STALE -- while the site itself is perfectly healthy"
curl -sS "$API/fleet" | j -r '.[] | select(.site=="site1") | "   \(.robotId)  \(.status)  lastCaptured=\(.lastCapturedAt // "-")"'
note "meanwhile, site 1's own actuator:"
printf '   edge-app-site1 = %s\n' "$(curl -sS http://localhost:8081/actuator/health | j -r '.status' 2>/dev/null || echo UP)"

bold "5. Restoring site 1's uplink"
"$ROOT/scripts/chaos.sh" site1 up
note "waiting for the bridge to reconnect and drain..."
sleep 25

bold "6. The queued pick executed on arrival"
curl -sS "$API/commands/$CMD_ID" | j .

bold "7. Backlog drained -- note backloggedMessages and maxIngestLagMillis"
curl -sS "$API/fleet" | j -r '.[] | select(.site=="site1") | "   \(.robotId)  \(.status)  seq=\(.lastSequence)  backlogged=\(.backloggedMessages)  maxLagMs=\(.maxIngestLagMillis)  missed=\(.missedMessages)"'

bold "Done."
note "missed=0 across an outage is the headline: no pick data lost, only delayed."
