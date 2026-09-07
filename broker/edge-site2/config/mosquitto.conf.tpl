# ============================================================================
# EDGE BROKER  --  site2
#
# WHY A BROKER AT ALL ON THE EDGE (this is the whole point of the demo):
#
#   The edge app publishes to *localhost*. That publish always succeeds in
#   single-digit milliseconds, because it never leaves the site. The WAN link
#   is someone else's problem -- specifically, this broker's.
#
#   The bridge below owns the flaky link: it reconnects with backoff, keeps a
#   persistent (cleansession=false) session on the cloud side, and spools
#   messages to disk while the link is down. When connectivity returns it
#   drains the backlog in order, at QoS 1, without the application knowing an
#   outage happened.
#
#   Without it, the app would need retry loops, its own disk spool, and
#   backpressure handling -- reimplemented in every service on every gateway.
# ============================================================================

per_listener_settings false

# ---------------------------------------------------------------------------
# Durability -- the store-and-forward buffer for WAN outages
# ---------------------------------------------------------------------------
persistence true
persistence_location /mosquitto/data/
autosave_interval 30

# Sized for a multi-hour outage. Tune from: publish rate x worst-case outage.
# At 1 msg/s/device this holds roughly 27h of backlog for a single device.
max_queued_messages 100000
max_queued_bytes 268435456
max_inflight_messages 40
message_size_limit 262144
queue_qos0_messages false

# ---------------------------------------------------------------------------
# Local site listeners
# ---------------------------------------------------------------------------
listener 1883 0.0.0.0
protocol mqtt

listener 8883 0.0.0.0
protocol mqtt
cafile   /mosquitto/certs/ca.crt
certfile /mosquitto/certs/edge-broker-site2.crt
keyfile  /mosquitto/certs/edge-broker-site2.key
tls_version tlsv1.2
require_certificate false

allow_anonymous false
password_file /mosquitto/config/passwd
acl_file      /mosquitto/config/acl

max_keepalive 120

# As on the cloud broker, persistent_client_expiration is deliberately unset:
# the local app's session (and anything queued for it) must survive an app
# restart of any length. Nothing at this site expires by time.
log_dest stdout
log_type error
log_type warning
log_type notice
log_type information
connection_messages true
log_timestamp true
sys_interval 10

# ===========================================================================
# BRIDGE: site2 -> cloud
# ===========================================================================
connection bridge-site2-to-cloud
address cloud-broker:8883

# --- identity -------------------------------------------------------------
# remote_clientid must be STABLE and UNIQUE. It is the key the cloud broker
# uses to find this site's persistent session after a reconnect. Change it and
# you silently orphan every queued command.
remote_clientid bridge-site2
remote_username bridge-site2
remote_password __BRIDGE_PASSWORD__

# --- transport ------------------------------------------------------------
bridge_cafile /mosquitto/certs/ca.crt
bridge_insecure false
bridge_tls_version tlsv1.2

# MQTT 3.1.1 on this hop only -- the applications either side are MQTT 5.
#
# WHY, precisely. A command travels cloud-app -> cloud broker -> [bridge] -> site
# broker -> edge-app. Hop 2 is the one that matters: this bridge dials out and
# subscribes on the cloud broker as `bridge-<site>`, so from the cloud broker's
# point of view it is simply a subscribed client. When the uplink drops,
# everything depends on whether the cloud broker KEEPS that client's session --
# its subscription plus anything queued for it.
#
# If the session survives, a command published while we are dark is queued and
# flushed on reconnect. If it does not, the publish matches no subscriber at all,
# and MQTT is a router rather than a queue: the message is discarded. Silently.
# The publish still succeeds and nothing logs an error.
#
# In MQTT 3.1.1, cleansession=0 means exactly "keep my session indefinitely".
# In MQTT 5 that was split in two: cleanStart=0 only asks to resume a session
# NOW, while how long it survives afterwards is a separate Session Expiry
# Interval property whose default, when absent, is 0 -- delete immediately.
# mosquitto's bridge never sends that property (send__connect(..., NULL) in
# src/bridge.c) and mosquitto's server applies the v5 default on receipt
# (src/handle_connect.c). Both are still true on the develop branch, so this is
# not something a version bump fixes.
#
# The failure is nasty because it is asymmetric. Telemetry is queued on THIS
# broker for our own outgoing connection, so it is untouched and drains with
# missed=0. Only commands depend on the cloud broker's session. Dashboards stay
# green while commands quietly vanish.
#
# The alternative is to run this hop on MQTT 5 and replay commands from the cloud
# service when `sites/<site>/bridge/state` flips back to 1 -- moving command
# durability out of the broker and into application code that must then be up and
# remember what is outstanding. That is a real option, and the reason this file
# is not on v5 today is that the broker already does the job correctly.
bridge_protocol_version mqttv311

# --- the flaky-link settings ----------------------------------------------
# cleansession false: works in BOTH directions here. It makes this broker keep our
# own session and spool telemetry/health to disk while the link is down, and --
# because this hop is 3.1.1 -- it also makes the CLOUD broker hold our session and
# queue commands for us while we are dark.
cleansession false

# try_private tells the remote it is talking to a bridge, so it will not echo
# our own messages back to us and create a loop.
try_private true

# Reconnect backoff: first retry after 5s, doubling up to 30s. The random
# jitter (final arg) stops 200 sites from reconnecting in lockstep after a
# regional blip and stampeding the cloud broker (mosquitto jitters the two-arg
# form automatically).
restart_timeout 5 30

# Short keepalive so a black-holed TCP connection (very common on cellular and
# NAT'd links -- the socket looks alive but nothing flows) is detected in ~30s
# instead of hanging until the OS TCP timeout minutes later.
keepalive_interval 20

notifications true
notification_topic sites/site2/bridge/state
bridge_attempt_unsubscribe false

# --- topic map ------------------------------------------------------------
#   topic <pattern> <direction> <qos>
# Direction is from the perspective of THIS (edge) broker.
# Every mapping is scoped to sites/site2/ -- with the site first in the topic,
# that scoping is a literal prefix on each line, and the bridge cannot leak
# another site's traffic even if a local publisher tries.

# Telemetry: high volume, upstream only.
topic sites/site2/telemetry/+ out 1

# Health/liveness: upstream only, retained so the cloud always has the last
# known state of every device even after a cloud broker restart.
topic sites/site2/health/+ out 1

# Commands: cloud -> site. This is the subscription the cloud broker holds
# open on our behalf while we are offline.
topic sites/site2/command/+/req in 1

# Command results/acks: site -> cloud.
topic sites/site2/command/+/res out 1

# Retain handling: keep the retain flag intact across the bridge so that
# retained health messages published at the edge stay retained in the cloud.
bridge_outgoing_retain true
