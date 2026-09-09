# Can the bridge hop run MQTT 5? A measured answer

The applications either side are MQTT 5. The bridge between them is MQTT 3.1.1,
which is an inconsistency worth removing if it can be removed safely. This is the
evidence for what it would take.

## The requirement

A command published to the cloud while a site is offline must be delivered when
the site returns. That needs the cloud broker to **keep the bridge's session** —
its subscription and its queued QoS 1 messages — across the disconnect.

In MQTT 3.1.1 `cleanSession=0` means exactly that. In MQTT 5 it does not:
`cleanStart=0` only asks to *resume* a session now, while how long one survives a
disconnect is a separate **Session Expiry Interval** property on CONNECT, whose
default when absent is **0 — delete immediately**.

So the question for any broker is narrow and checkable: **does its bridge send
that property?**

## Method

A listener that decodes the CONNECT packet each bridge sends, then answers
CONNACK. This reads the answer off the wire rather than from documentation, and
works identically for every broker.

Validated against the known case first: mosquitto's v5 bridge, which the source
says passes `NULL` for CONNECT properties (`send__connect(..., NULL)` in
`src/bridge.c`). The wire agreed.

## Results

| Bridge broker | Proto | `clean_start` | Session Expiry sent | Client id stable across restart | Durable remote session |
|---|---|---|---|---|---|
| mosquitto 2.0.22, `mqttv311` | 3.1.1 | false | n/a — no such property in 3.1.1 | yes (`remote_clientid`) | **yes** |
| mosquitto 2.0.22, `mqttv50` | 5 | false | **absent** | yes | no |
| NanoMQ, default v5 bridge | 5 | false | **absent** | yes (`clientid`) | no |
| NanoMQ + `conn_properties.session_expiry_interval` | 5 | false | **4294967295** | yes | **yes** |
| EMQX 6.3.0, `connectors.mqtt` | 5 | false | **absent** | **no — random suffix per restart** | no |
| HiveMQ CE 2024 | — | — | — | — | no bridge feature in the community edition |

Two things stand out.

**EMQX fails twice over.** Beyond the missing property, its MQTT connector
generates a fresh client id on every restart — observed as
`probe-emqx286ed74bf4384` then `probe-emqx212ef51a6e952`. Even with the session
expiry fixed, the remote could never match the returning client to its old
session. That is not a defect: EMQX's MQTT connector is built for data
integration, not for the store-and-forward bridging mosquitto does.

**NanoMQ can do it, but only if you ask.** Its default v5 bridge behaves exactly
like mosquitto's — `clean_start=false` and no expiry property. Adding one block
changes the verdict:

```hocon
bridges.mqtt.cloud {
    proto_ver   = 5
    clientid    = "bridge-site1"      # stable: the key to the remote session
    clean_start = false
    conn_properties = { session_expiry_interval = 4294967295 }
}
```

## Functional confirmation

The CONNECT property is necessary; it is not obviously sufficient. So the full
scenario was run end to end — NanoMQ edge bridging to a mosquitto cloud, a
subscriber on the edge, the edge detached from the network, a QoS 1 command
published to the cloud during the outage, then the network restored:

| Configuration | Command queued during the outage |
|---|---|
| NanoMQ v5 **with** `session_expiry_interval` | **delivered on reconnect** |
| NanoMQ v5 **without** it (same config otherwise) | **lost** |

The control matters: it is the same broker and the same topic map, differing by
one property. That isolates the cause rather than inferring it.

## The other half: does NanoMQ spool *outbound* while the link is down?

Session expiry fixes the **inbound** path — commands queued by the cloud for an
absent site. It says nothing about the **outbound** path, which is where telemetry
durability lives and which is what `missed=0` refers to.

NanoMQ documents exactly this: an embedded SQLite spool so QoS 1/2 messages
survive a disconnected uplink without holding them all in RAM. The feature is
real and it is the right design for an edge gateway. The question is only whether
it works in the shipped images.

### Method

Site LAN separate from the WAN, so the edge broker stays reachable while its
uplink is cut — the same shape as the real deployment. Publish five QoS 1
messages to the edge with the WAN down, restore, count what reaches the cloud.

The cache block goes at top level, *beside* the bridge rather than inside it:

```hocon
bridges.mqtt.cache {
    disk_cache_size     = 102400
    mounted_file_path   = "/tmp/"
    flush_mem_threshold = 10
    resend_interval     = 2000
}
```

(An earlier run of this test used a top-level `sqlite { ... }` block, which is
what the upstream example config shows for broker-level persistence. That is the
wrong key for the bridge cache and the results from it were void. Everything
below uses `bridges.mqtt.cache`.)

### Results

| Image | Arch | Cache file created | Delivered after restore | Broker survived |
|---|---|---|---|---|
| `emqx/nanomq:latest` | arm64 | no | **2 of 7** | yes |
| `emqx/nanomq:latest-full` | arm64 | no | **2 of 7** | **no — SIGSEGV** |
| `emqx/nanomq:latest-full` | amd64 | no | **2 of 7** | **no — SIGSEGV** |

Messages 1 and 2 were the pre-outage baseline; 3 to 7 were published during it.
None of them arrived, under any combination.

On the base image the broker stays up and simply drops them, saying so:

```
bridge_pub_handler: Cached Message in ctx_msgs is lost!
bridge_pub_handler: Msg lost! put msg to ctx_msgs failed!
```

On the `-full` image — the variant that carries the optional features — the
broker **segfaults** on that same code path:

```
broker.c:118 sig_handler: signal signumber: 11 received!
```

Reproduced on both architectures, so this is not an emulation artefact.

No SQLite file was ever written to `mounted_file_path` in any run.

### Reading this fairly

The documentation is not wrong that the feature exists, and it may well work in a
build or version not tested here. What can be said from measurement is narrower
and still decisive for this decision: **with the current official images and the
documented configuration, a message published while the bridge is down does not
survive** — and on the full image the broker does not survive either. That is
worth reporting upstream rather than working around.

## What this would cost

NanoMQ supports what the security model needs — a TLS listener
(`listeners.ssl` with `cacertfile`/`certfile`/`keyfile`), password auth, and
`auth { password = {include ...}, acl = {include ...} }`.

The bill for full MQTT 5 consistency is therefore:

- port each site broker's config from `mosquitto.conf` to NanoMQ's HOCON,
  including TLS, credentials and the per-site ACLs;
- run two broker technologies (NanoMQ at the edge, mosquitto in the cloud) — or
  move the cloud to NanoMQ too, which is a bigger claim, since NanoMQ is built
  for edge gateways rather than as a hub;
- re-verify the security properties on a less battle-tested ACL implementation.

## What is gained

Message Expiry Interval currently does not survive the 3.1.1 hop, and a denied
publish over it returns `PUBACK RC:0` rather than `0x87 Not authorized`
(both measured — see the README). Full v5 would carry both.

Neither changes correctness today: command TTL is enforced by the cloud broker's
own queue and re-checked in the payload at the edge, and authorisation failures
are visible in the broker log.

## Recommendation

**Stay on mosquitto with the 3.1.1 bridge hop.**

It was already a deliberate, documented trade rather than drift. What this
investigation adds is that the obvious alternative is worse: the official NanoMQ
image would buy MQTT 5 on the hop at the price of telemetry that does not survive
an outage — measured, not assumed.

Revisit only if a v5-only feature is genuinely needed across that hop
(per-message expiry enforced at the site, or reason codes surfaced to the bridge).
The order of work would then be:

1. find or build a NanoMQ image where the outbound spool test in this document
   passes — if none exists, stop;
2. port TLS, credentials and per-site ACLs to HOCON and re-verify them;
3. only then change the bridge protocol version.

Doing step 3 first is the tempting order and the wrong one.
