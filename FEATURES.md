# Feature reference

Every feature this project actually uses, where it is configured, and why it is
there. Nothing aspirational — if it is listed here it is in the running stack.

Companion to [README.md](README.md), which explains the architecture.

---

## 1. MQTT protocol features

| Feature | Where | Why it is used |
|---|---|---|
| **QoS 1 (at-least-once)** | every publish and subscribe | QoS 0 loses data on exactly the link least able to afford it; QoS 2 costs two extra round trips on that same link. The price of QoS 1 is duplicates — handled explicitly (§4). |
| **QoS 0** | `observer` UI subscriptions only | An inspection tool must never make the broker queue on its behalf. It watches the system; it does not become part of it. |
| **Retained messages** | `sites/+/health/+`, `sites/+/bridge/state` | Health is 15s apart. Without retain, a restarted cloud service is blind to a device for a full interval, and "quiet" is indistinguishable from "never seen". |
| **Deliberately *not* retained** | telemetry, `command/…/req`, `command/…/res` | A retained reading looks like live data and isn't. A retained command is re-executed by every future subscriber. A retained result is misleading. |
| **Last Will and Testament** | `MqttWill`, installed at `MqttConnection#buildOptions` | A dead process cannot announce its own death. The broker publishes `status: DOWN` on `sites/<site>/health/gateway` if the session ends without a clean DISCONNECT. |
| **Clean DISCONNECT on shutdown** | `MqttConnection#stop` | Tells the broker *not* to fire the will. Skipping this is why many deployments show phantom DOWN events on every rolling restart. |
| **Persistent sessions** (`cleanStart=false`) | both services, and the bridge | The broker keeps our subscriptions and queues QoS 1 messages for us while we are gone. This is the whole point of a durable client. |
| **Session Expiry Interval** (MQTT 5) | `app.mqtt.session-expiry: 4294967295s` | Set to the protocol maximum, i.e. never. Anything queued for an absent service is telemetry or health, and neither may expire. |
| **Message Expiry Interval** (MQTT 5) | `CommandDispatcher#dispatch` only | The *only* time-based expiry in the system. The cloud broker drops a lapsed command from an offline site's queue rather than shipping it. |
| **Topic wildcards `+` and `#`** | fleet-wide subs, ACLs, bridge maps | `sites/+/telemetry/+` for cross-site reads; `sites/site1/#` for a one-line tenant ACL. Matching is pinned by tests. |
| **`$SYS` topics** | broker health checks, UI | Broker-reported stats. Note `#` does **not** match `$SYS`, so ACLs grant it explicitly. |
| **Keepalive (20s)** | `app.mqtt.keep-alive`, `keepalive_interval` | Short on purpose: a black-holed TCP connection (routine on cellular/NAT links — socket looks alive, nothing flows) is detected in ~30s instead of at the OS timeout minutes later. |
| **MQTT 5** | both application clients | Needed for Message Expiry Interval and Session Expiry Interval. |
| **MQTT 3.1.1** | the bridge, deliberately | In v5 the Session Expiry Interval defaults to 0 and mosquitto's bridge has no option to set it — so a v5 bridge silently loses its session on every disconnect. See README, "one real bug this design flushed out". |

## 2. Mosquitto broker features

| Directive | Where | Purpose |
|---|---|---|
| `connection` / `address` / `topic` | edge brokers | The bridge itself and its per-direction topic map. |
| `remote_clientid` | edge brokers | Stable and unique — it is the key the cloud broker uses to find this site's persistent session. Change it and you orphan every queued command. |
| `cleansession false` | bridge | Cloud broker queues `sites/<site>/command/+/req` while the site is dark. |
| `try_private true` | bridge | Tells the remote it is talking to a bridge, so it does not echo our own messages back and create a loop. |
| `restart_timeout 5 30` | bridge | Reconnect backoff, 5s doubling to 30s, jittered — so 200 sites do not reconnect in lockstep after a regional blip and stampede the cloud. |
| `notifications` + `notification_topic` | bridge | Mosquitto publishes `1`/`0` to `sites/<site>/bridge/state`. The cloud's only *direct* signal about WAN health, far faster than waiting for heartbeats to go stale. |
| `bridge_outgoing_retain` | bridge | Keeps the retain flag intact across the hop, so retained health stays retained in the cloud. |
| `bridge_attempt_unsubscribe false` | bridge | Does not unsubscribe on stop, preserving the queued session. |
| `persistence` / `persistence_location` / `autosave_interval` | all brokers | The store-and-forward buffer, on a Docker volume. |
| `max_queued_messages` (100 000) | all brokers | Sized from publish rate × worst-case outage. ~27h of backlog for one device at 1 msg/s. |
| `max_queued_bytes`, `max_inflight_messages`, `message_size_limit` | all brokers | Bound the damage a stuck or abusive client can do. |
| `queue_qos0_messages false` | all brokers | Everything meaningful is QoS 1; queueing QoS 0 would only buffer the inspection tool. |
| *(no)* `persistent_client_expiration` | all brokers | Deliberately unset — it would apply an expiry rule to telemetry and health. |
| `listener` × 2 (1883, 8883) | all brokers | 8883 TLS for real traffic; 1883 bound to container networks only, never published to the host, for local ops tooling. |
| `cafile` / `certfile` / `keyfile` / `tls_version` | all brokers | TLS against the private CA. |
| `bridge_cafile` / `bridge_insecure false` / `bridge_tls_version` | bridge | The bridge validates the cloud broker's chain and hostname. |
| `allow_anonymous false` + `password_file` | all brokers | No unauthenticated connections anywhere. |
| `acl_file` | all brokers | Per-site tenancy (§6). |
| `max_keepalive`, `max_connections` | all brokers | Connection hygiene. |
| `sys_interval`, `log_type`, `connection_messages` | all brokers | Observability. |

## 3. Eclipse Paho v5 client features

| API | Purpose |
|---|---|
| `MqttAsyncClient` | Non-blocking client; publishes never block the scheduler thread. |
| `MqttDefaultFilePersistence` / `MemoryPersistence` | **Client-side** store of messages published but not yet PUBACKed — not the store-and-forward buffer, which is the broker's `persistence_location`. Against a same-LAN broker the ack takes ~1ms, so it stays empty in practice and covers only a crash inside that window. Measured during a 35s outage: broker queue 83 messages, client store 0. |
| `setCleanStart` / `setSessionExpiryInterval` | Durable session (§1). |
| `setWill` | Last Will (§1). |
| `setAutomaticReconnect` + `setAutomaticReconnectDelay(1, 30)` | Paho's own backoff — **but** it only arms after the first successful connect, so initial connection is retried by `MqttConnection#scheduleConnect`. A service starting while its broker is still booting would otherwise stay dead forever. |
| `setSocketFactory` + PKCS12 truststore | TLS against the private CA, no JVM-wide trust changes. |
| `setHttpsHostnameVerificationEnabled(true)` | Certificate hostname checking actually on. |
| `MqttCallback#connectComplete` | Re-subscribes on **every** connect. The broker normally restores subscriptions, but not if the session was lost — and a silently unsubscribed consumer is the worst failure mode here: everything looks healthy and nothing arrives. |
| `MqttCallback#messageArrived` | Hands off to a single-threaded dispatcher (§7). |
| `setManualAcks(true)` + `messageArrivedComplete()` | Ack control is taken from Paho in **both** modes. Paho auto-acks as soon as its callback returns, which — with async dispatch — would ack before the listener ran. Now the ack follows the listener, so a failed handler is redelivered rather than lost. |
| **Dead-letter topic** | `mqtt.dead-letter.*`. Bounds the cost of the above: without it a message that can never succeed is redelivered forever and holds an inflight slot. A handler failure is retried `max-attempts` times then dead-lettered; an undecodable payload skips retries entirely. If the dead-letter publish itself fails the ack is withheld, because acking would destroy the last copy. |
| `MqttMessage#setProperties` → `setMessageExpiryInterval` | Command expiry (§1). |

## 4. Delivery-semantics handling

| Concern | Where | Handling |
|---|---|---|
| **Duplicates** | `CommandExecutor` | Bounded LRU of recent `commandId`s. A redelivered `PICK_ITEM` run twice puts two units in a tote meant to hold one — a shipping error nobody notices until the customer does. A repeat is answered `DUPLICATE`, not re-executed. Checked **before** expiry, so a redelivered-and-lapsed command reads as the duplicate it is. |
| **Stale commands** | `CommandExecutor`, `CommandRequest#isExpired` | Past `expiresAt` → answered `EXPIRED` and deliberately not run. A three-hour-old `PICK_ITEM` names a rerouted order and a restocked bin; a three-hour-old `RESUME_PICKING` restarts an arm in a cell someone is standing in. |
| **Replay / reordering** | `FleetRegistry#onTelemetry` | Sequence-number guard: a redelivery cannot overwrite newer state with older readings. |
| **Lost messages** | `FleetRegistry` | Sequence **gap** counting. QoS 1 tells you a message was delivered; only the sequence tells you one never was. |
| **Backlog vs live data** | `FleetRegistry` | Freshness judged on `capturedAt` (stamped at the edge), never arrival. A drained backlog arrives "now" but describes hours ago. |
| **Robot liveness** | `FleetRegistry` | Missed-heartbeat → `STALE`; an explicit Last Will `DOWN` always beats the heuristic. |
| **Safety interlock** | `CommandExecutor` | A `PICK_ITEM` for a `PAUSED` cell is `REJECTED`, never held to run later. |
| **Partial success** | `CommandExecutor` | A failed grasp returns `ACCEPTED` with `grasped=false` — a real outcome, not an error, so the cloud re-queues the pick instead of assuming it shipped. |

## 5. Security

| Feature | Detail |
|---|---|
| **Private CA + server certs** | Generated by `scripts/bootstrap.sh`; SANs cover both the compose service name and `localhost`. |
| **TLS everywhere** | Every listener and every bridge hop. `tls_version tlsv1.2`. |
| **PKCS12 truststore** | Java services verify the broker chain against it; no JVM-wide trust modification. |
| **Password authentication** | `allow_anonymous false`; random 24-char passwords per install, never committed. |
| **Per-site ACLs** | Site 1's credential is confined to `sites/site1/…`. |
| **Asymmetric bridge grants** | The bridge may *write* telemetry/health but only *read* commands — it cannot be granted `sites/site1/#` write, or a site could inject commands to its own devices and bypass the cloud's authorisation entirely. |
| **Read-only `observer`** | For the topic browser. Watches everything, publishes nothing; revocable without touching either service. |
| **Non-root containers** | uid/gid 10001. |
| **Secrets gitignored** | `.env`, `secrets/`, `certs/`, rendered bridge configs and `passwd` files. |

## 6. Spring Boot features

| Feature | Where |
|---|---|
| `@ConfigurationProperties` | `MqttClientProperties`, `EdgeProperties`, `CloudProperties` — typed config, `Duration` parsing, env-var overrides. |
| `SmartLifecycle` | `MqttConnection` — connects after the context is refreshed, disconnects cleanly before shutdown, with an explicit `getPhase()`. |
| `@Scheduled` | Telemetry and health tick loops, command-record eviction. |
| **Graceful shutdown** | `server.shutdown: graceful` + `timeout-per-shutdown-phase: 20s`, so the clean DISCONNECT gets out. |
| **Actuator** | `/actuator/health` (+ liveness/readiness probes), `/actuator/metrics`, `/actuator/prometheus`. |
| **Custom `HealthIndicator`** | `MqttHealthIndicator` — reports broker connection, disconnect count, last-connected time. |
| **Micrometer + Prometheus** | 15 custom meters: `mqtt.messages.{published,received,acknowledged,unacknowledged,dead_lettered}`, `mqtt.connected`, `mqtt.disconnects.total`, `mqtt.dispatch.errors`, `cloud.commands.{issued,answered,pending}`, `cloud.telemetry.{late,out_of_order,sequence_gaps}`, `cloud.robots.tracked`, `edge.commands.{executed,duplicate,expired}`. |
| `ObjectProvider` | Breaks a real bean cycle (connection ↔ handler) without a CGLIB proxy in every stack trace. |
| **Spring MVC REST** | `FleetController`; `202 Accepted` for command submission, because reachability is not knowable at that point. |
| **Jackson** | `JavaTimeModule` ISO-8601 instants, `non_null` inclusion. |
| **Records** | All DTOs and `MqttSendOptions`. |
| **Custom starter** | `mqtt-spring-boot-starter` — `@AutoConfiguration` registered via `AutoConfiguration.imports`, `@ConditionalOnClass` / `@ConditionalOnProperty` / `@ConditionalOnMissingBean` / `@ConditionalOnEnabledHealthIndicator`, BOM-imported rather than parent-inherited, optional actuator dependency. |
| **`@MqttListener` + `BeanPostProcessor`** | Annotation-driven subscriptions via `MethodIntrospector.selectMethods` + `AnnotatedElementUtils`, the same mechanism `@KafkaListener` uses. `AopUtils.getTargetClass` so annotations on proxied beans are still found. |
| **`MqttGateway`** | Outbound API: encode, QoS, retain and expiry in one call. |
| **`ApplicationContextRunner`** | Auto-configuration tested without starting a broker or a full `@SpringBootTest`. |
| **Configuration metadata processor** | `spring-boot-configuration-processor` generates IDE completion for `mqtt.*`. |

## 7. Concurrency

| Feature | Why |
|---|---|
| **Single-threaded dispatcher** | Preserves per-topic ordering (sequence numbers would interleave after a backlog drain) while getting work off Paho's receiver thread, which must never block. |
| **`CallerRunsPolicy` + bounded queue** | The backpressure valve. If handlers fall behind, Paho's thread does the work itself and stops reading — so the *broker's* queue absorbs the burst instead of this process growing an unbounded in-memory one. |
| **Handler exception isolation** | A throwing handler is caught and counted; it never kills delivery for other subscriptions. |
| **`ConcurrentHashMap#compute`** | Atomic read-modify-write of per-device state under concurrent ingest. |

## 8. Docker & Compose

| Feature | Purpose |
|---|---|
| **Multi-stage builds** | Maven layer for deps, JRE runtime image. |
| **Spring Boot layered jars** (`-Djarmode=tools extract --layers`) | Dependencies and application code in separate image layers — a code push ships a few hundred kB, not 60MB. Matters when shipping to gateways over the links this demo is about. |
| **Separate build contexts** | `apps/cloud-service` and `apps/edge-service`. An edge build cannot even read cloud code. |
| **Network isolation** | `edge-app` is attached only to its site LAN, so it *cannot* reach the cloud broker. The boundary is enforced by Docker, not convention. |
| **Named volumes** | Broker persistence and Paho client spools survive restarts. |
| **Health checks** | Brokers use `mosquitto_sub -E`, which proves listener + TLS + credentials + ACL — not merely that a port is open. Services use the actuator readiness probe. |
| **`depends_on: service_healthy`** | Ordered startup — except site brokers, which deliberately do *not* wait for the cloud. |
| **Compose profiles** | The UI is opt-in: `--profile ui`. |
| **`stop_grace_period: 30s`** | Time for the clean DISCONNECT. |
| **Resource limits + log rotation** | 512M cap, 10MB × 3 log files. |
| **YAML anchors** | `x-app-common`, `x-broker-common` — shared service config without repetition. |

## 9. Tooling

| Tool | Purpose |
|---|---|
| `scripts/bootstrap.sh` | PKI, random credentials, Java truststore, rendered bridge configs, preseeded UI connections. `--force` to regenerate. |
| `scripts/chaos.sh` | WAN outage simulation via `docker network disconnect`. `down` / `up` / `flap` / `status`. |
| `scripts/demo.sh` | Scripted end-to-end walkthrough of an outage. |
| **MQTT Explorer** (`--profile ui`) | Live topic tree, preseeded with all three brokers on a read-only account. |
| **Tests** | 46 tests (31 starter, 8 edge, 7 cloud): wildcard matching, expiry boundaries, topic scheme, replay/gap/staleness/LWT ingest rules. |

## 10. Deliberately *not* used

Worth stating, because each was considered:

| Not used | Why |
|---|---|
| **QoS 2** | Two extra round trips on the worst link, to solve a duplicate problem already solved idempotently. |
| **A shared module for the wire contract** | `Topics` and the payload records stay duplicated: sharing them turns every cloud release into a fleet-wide gateway release. The *plumbing* is shared as a starter, because infrastructure versions independently of the contract. |
| **Two-module starter split** (`-autoconfigure` + `-starter`) | That split lets a consumer take the auto-configuration without the opinionated dependencies. There is no such consumer here — the auto-configuration is meaningless without Paho — so it would be two poms doing one pom's job. Worth adding if this is ever published outside the org. |
| **`BeanFactoryPostProcessor` for listener discovery** | Runs before beans exist, so it must classpath-scan configured packages and cannot see `@Bean`-defined beans. A `BeanPostProcessor` is handed every bean the context actually creates. |
| **mTLS** | Server-auth TLS + passwords is the common production baseline. `require_certificate false` is one flag away from changing, and the bridges already carry a CA. |
| **`persistent_client_expiration`** | Would expire telemetry and health. Retired sites should have their session deleted explicitly, not by a timer that cannot tell "retired" from "offline". |
| **A hand-built dashboard** | An off-the-shelf MQTT client renders the topic tree better, and is one less thing to maintain. |
| **Broker WebSocket listeners** | MQTT Explorer proxies server-side, so the browser needs neither a WS listener nor the private CA. |
