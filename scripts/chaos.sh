#!/usr/bin/env bash
# ============================================================================
# Simulate a flaky WAN link for one site.
#
#   ./scripts/chaos.sh site1 down     # pull site 1's uplink
#   ./scripts/chaos.sh site1 up       # restore it
#   ./scripts/chaos.sh site1 flap 6 20 10   # 6 cycles: 20s down, 10s up
#   ./scripts/chaos.sh status
#
# Detaching the site's BROKER from the `wan` network is a faithful outage: the
# site LAN keeps working, the edge app keeps publishing locally at full rate,
# and only the bridge notices. That is precisely the failure this architecture
# is built for -- and precisely what you cannot demonstrate if the application
# talks to the cloud broker directly.
# ============================================================================
set -euo pipefail

WAN_NET="jinternals_wan"

usage() {
  sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 1
}

broker_for() {
  case "$1" in
    site1|site2) echo "edge-broker-$1" ;;
    *) echo "Unknown site '$1' (expected site1 or site2)" >&2; exit 1 ;;
  esac
}

attached() {
  docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$1" 2>/dev/null \
    | tr ' ' '\n' | grep -qx "$WAN_NET"
}

link_down() {
  local broker; broker="$(broker_for "$1")"
  if attached "$broker"; then
    docker network disconnect "$WAN_NET" "$broker"
    printf '\033[31m  %s uplink DOWN\033[0m  (bridge is now spooling to disk)\n' "$1"
  else
    printf '  %s uplink already down\n' "$1"
  fi
}

link_up() {
  local broker; broker="$(broker_for "$1")"
  if attached "$broker"; then
    printf '  %s uplink already up\n' "$1"
  else
    docker network connect "$WAN_NET" "$broker"
    printf '\033[32m  %s uplink UP\033[0m  (bridge will reconnect within ~5-30s and drain)\n' "$1"
  fi
}

status() {
  printf '%-22s %-10s %s\n' SITE UPLINK BROKER
  for site in site1 site2; do
    broker="$(broker_for "$site")"
    if ! docker inspect "$broker" >/dev/null 2>&1; then
      printf '%-22s %-10s %s\n' "$site" "-" "$broker (not running)"
    elif attached "$broker"; then
      printf '%-22s \033[32m%-10s\033[0m %s\n' "$site" "UP" "$broker"
    else
      printf '%-22s \033[31m%-10s\033[0m %s\n' "$site" "DOWN" "$broker"
    fi
  done
}

case "${1:-}" in
  status) status ;;
  site1|site2)
    SITE="$1"
    case "${2:-}" in
      down) link_down "$SITE" ;;
      up)   link_up "$SITE" ;;
      flap)
        CYCLES="${3:-5}"; DOWN_S="${4:-20}"; UP_S="${5:-10}"
        echo "Flapping $SITE: $CYCLES cycles of ${DOWN_S}s down / ${UP_S}s up"
        for i in $(seq 1 "$CYCLES"); do
          echo "-- cycle $i/$CYCLES"
          link_down "$SITE"; sleep "$DOWN_S"
          link_up   "$SITE"; sleep "$UP_S"
        done
        ;;
      *) usage ;;
    esac
    ;;
  *) usage ;;
esac
