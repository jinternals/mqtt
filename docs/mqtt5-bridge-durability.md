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
durability lives and which is the guarantee this whole system is built on
(`missed=0` across an outage).

Tested the same way, with a site LAN separate from the WAN so the edge stayed
reachable while its uplink was cut: publish to the edge broker with the WAN down,
restore, count what reached the cloud.

| Messages published to the edge during the outage | Reached the cloud |
|---|---|
| 5 | **0** |

NanoMQ says so itself, in its own log:

```
bridge_pub_handler: Cached Message in ctx_msgs is lost!
bridge_pub_handler: Msg lost! put msg to ctx_msgs failed!
```

The `sqlite { disk_cache_size = ... }` block that is supposed to provide the
disk-backed bridge cache was configured and had no effect. The reason is that the
feature is not compiled into the official image:

```
$ strings $(command -v nanomq) | grep -c sqlite3_
0
```

Zero SQLite symbols, no cache file created, and **no warning that the
configuration was ignored** — a durability setting that silently does nothing is
worse than one that is absent.

## Verdict

| | mosquitto (current) | NanoMQ, official image |
|---|---|---|
| MQTT 5 on the bridge hop | no | **yes** |
| Commands queued for an absent site | **yes** (3.1.1) | **yes** (with `conn_properties`) |
| Telemetry spooled while the uplink is down | **yes** | **no — measured, 5 of 5 lost** |

Swapping to NanoMQ as shipped would fix the lesser problem and break the greater
one. Commands already survive an outage today; telemetry durability is the thing
`missed=0` refers to, and it would go.

NanoMQ *can* do it — the SQLite cache exists upstream behind
`-DNNG_ENABLE_SQLITE=ON`. Taking that path means building and maintaining a custom
NanoMQ image and re-running this outbound test against it, on top of the ACL port
below. That is a different and much larger commitment than a config change.

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

1. build NanoMQ with `-DNNG_ENABLE_SQLITE=ON` and re-run the outbound spool test
   in this document — if that fails, stop;
2. port TLS, credentials and per-site ACLs to HOCON and re-verify them;
3. only then change the bridge protocol version.

Doing step 3 first is the tempting order and the wrong one.
