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

The 3.1.1 hop is a deliberate, documented trade rather than drift, and it costs
nothing that currently matters. Move to NanoMQ when a v5-only feature is actually
needed across that hop — per-message expiry enforced at the site, or reason codes
surfaced to the bridge — and treat the ACL port as the real work, not the bridge
config.
