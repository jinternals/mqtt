# mqtt-spring-boot-starter

Durable MQTT for Spring Boot: a Paho v5 client with TLS, Last Will,
reconnect-with-backoff, ordered dispatch, an actuator health indicator and
Micrometer metrics — configured from `mqtt.*` properties.

Annotate a method to receive, inject a gateway to send.

```java
@Component
class CommandListener {

    private final MqttGateway gateway;

    @MqttListener(topic = "sites/${app.site-id}/command/+/req")
    void onCommand(CommandRequest request, String topic) {
        gateway.send("sites/x/command/y/res", new CommandResponse(...));
    }
}
```

```yaml
mqtt:
  url: ssl://broker:8883
  client-id: gateway-site1          # required, no default
  username: edge-app
  password: ${MQTT_PASSWORD}
  persistence-directory: /var/lib/mqtt-client
  ssl:
    trust-store: /etc/mqtt/truststore.p12
    trust-store-password: ${TRUSTSTORE_PASSWORD}
```

The auto-configuration activates only when `mqtt.url` is set, and every bean is
`@ConditionalOnMissingBean`, so having it on the classpath costs nothing and any
piece can be replaced.

## Packages

```
com.jinternals.mqtt.spring
├── support/        MqttCodec, MqttPayloadConversionException
├── core/           MqttConnection, MqttGateway, MqttSendOptions, MqttSubscription,
│                   MqttSubscriptionSource, MqttMessageHandler, MqttAcknowledgement,
│                   MqttAckMode, MqttWill, MqttClientProperties, MqttTopicFilter,
│                   MqttDeadLetter, MqttPublishException
├── annotation/     @MqttListener
├── listener/       MqttListenerRegistry, MqttListenerAnnotationBeanPostProcessor
├── health/         MqttHealthIndicator
└── autoconfigure/  MqttAutoConfiguration, MqttHealthAutoConfiguration
```

Dependencies point strictly one way, with no cycles:

```
support  ←  core  ←  annotation  ←  listener
             ↑                          ↑
           health ─────────────── autoconfigure
```

`support` depends on nothing; nothing depends on `autoconfigure`, which is what
lets the library be wired by hand without Spring Boot's auto-configuration.

The one place that took work is `MqttConnection` needing whatever the annotation
scan found. Rather than `core` importing `listener` — a cycle, and the end of
`core` standing alone — `core` declares `MqttSubscriptionSource` and
`MqttListenerRegistry` implements it. Dependency inversion, not relocation.

## Three decisions worth knowing about

**`client-id` is required and has no default.** It must be stable across restarts
*and* unique across instances: the broker keys the persistent session off it, so a
value that changes on boot abandons every queued message, and a value shared by two
instances makes them evict each other in a reconnect loop. Defaulting it to
`spring.application.name` would be convenient right up until the same application is
deployed twice. A property whose wrong value fails invisibly should not have a
guessed default.

**Persistence is in-memory unless you name a directory.** A library cannot know
whether a path it picks is a mounted volume or a container's writable layer that
evaporates on restart — and a file store on ephemeral storage is worse than memory,
because it *looks* like durability. The default is honest and obviously non-durable,
and it warns at startup naming the property to set.

**Listeners are discovered by a `BeanPostProcessor`, not a
`BeanFactoryPostProcessor`.** A BFPP runs before any bean exists, so it has to
classpath-scan configured packages and cannot see beans defined by `@Bean` methods.
A BPP is handed every bean the context actually creates: no package configuration,
nothing missed. It is what `@KafkaListener` and `@RabbitListener` both do.

Discovered subscriptions go into `MqttListenerRegistry` rather than straight to the
connection — a post-processor that injected `MqttConnection` would force it into
existence before the context was ready. `MqttConnection` drains the registry in
`start()`, which the lifecycle processor runs once every singleton exists.

## `@MqttListener` signatures

**The first parameter is the payload.** Anything after it is resolved by *type*, so
order does not matter and there is nothing to memorise.

| Parameter | Meaning |
|---|---|
| 1st: `T` | decoded from JSON |
| 1st: `byte[]` | raw bytes |
| 1st: `String` | UTF-8 string |
| `String` (after the 1st) | the concrete topic — how you recover what `+` matched |
| `MqttAcknowledgement` | ack handle; only meaningful with `mqtt.manual-acks=true` |

`topic` is resolved against the `Environment`, so `${...}` placeholders work. An
unusable signature fails at startup, not on the first message.

## Acknowledgement: `mqtt.manual-acks`

The starter always takes ack control away from Paho. Paho's auto-ack fires as soon
as its callback returns, and this client hands work to a dispatch thread — so
leaving it on would acknowledge every message *before the listener had run*, making
QoS 1 at-least-once only as far as the library and at-most-once beyond it.

**`false` (default)** — the connection acknowledges after the listener returns
without throwing:

```java
@MqttListener(topic = "sites/+/telemetry/+")
void onTelemetry(Telemetry t) {
    registry.record(t);          // throws? → not acknowledged → broker redelivers
}
```

**`true`** — the listener owns it, for when "handled" means more than "the method
returned":

```java
@MqttListener(topic = "sites/+/telemetry/+")
void onTelemetry(Telemetry t, MqttAcknowledgement ack) {
    repository.save(t);          // durable somewhere else first
    ack.acknowledge();           // only now may the broker forget it
}
```

### Per-listener override

`mqtt.manual-acks` is the service-wide default; a single listener can opt out:

```java
@MqttListener(topic = "sites/+/telemetry/+", ackMode = AUTO)
void ingest(Telemetry t) { registry.record(t); }          // cheap, in-memory

@MqttListener(topic = "sites/+/health/+", ackMode = MANUAL)
void persist(RobotHealth h, MqttAcknowledgement ack) {
    repository.save(h);
    ack.acknowledge();                                     // only once durable
}
```

`INHERIT` (the default) takes the property, so a service with one policy sets one
property and never touches the annotation.

**There is one constraint, and it comes from MQTT rather than from this starter.**
`@KafkaListener` can vary ack mode freely because each listener gets its own
consumer. Here every listener shares one connection, and an acknowledgement applies
to the **message**, not to a subscription. If a wildcard let one message reach both
an `AUTO` and a `MANUAL` listener, the automatic acknowledgement would fire first
and silently cancel the manual one's control.

So listeners whose effective modes differ may not have **overlapping topic
filters**, and that is checked at startup by structural filter comparison, not left
to surface as lost messages:

```
@MqttListener auditListener#audit on 'sites/site1/#' is MANUAL, but it overlaps
'sites/+/telemetry/+' which is AUTO. One message can match both, and an
acknowledgement applies to the message rather than the subscription, so the
automatic one would cancel the manual one's control. Give them the same ackMode,
or topic filters that cannot both match.
```

Different modes on filters that *cannot* both match are fine — that is the point.

### Mode and signature must agree, and the starter checks at startup

`mqtt.manual-acks` and your listener signature are two halves of one decision. Get
them out of step and the runtime symptom is silent, so both mismatches are refused
when the context starts:

| `manual-acks` | ack parameter | outcome |
|---|---|---|
| `false` | absent | **The default.** Connection acks once the listener returns |
| `true` | present | Manual mode. The listener decides |
| `true` | *absent* | **Startup failure** — nothing would ever ack, and delivery would stop once the inflight window filled |
| `false` | *present* | **Startup failure** — the connection already acks on return, so acking early then throwing would lose the message while the log claimed it was withheld |

```
@MqttListener fleetListener#onCommandResponse takes no MqttAcknowledgement
parameter, but mqtt.manual-acks is true. Nothing would ever acknowledge these
messages and delivery would stall once the inflight window filled. Add an
MqttAcknowledgement parameter, or set mqtt.manual-acks=false to let the
connection acknowledge on return.
```

Both are the kind of bug that passes every test against a broker that never
redelivers, then goes quiet in production — so they are startup failures rather
than warnings.

### Three consequences worth knowing before choosing

- A withheld acknowledgement is redelivered on **session resume**, not immediately.
  A failed message can wait until the connection next cycles.
- Each unacknowledged message occupies a slot in the inflight window. Enough of them
  and delivery stalls — deliberate backpressure, but it is a stall.
- So a message that can *never* succeed must not be withheld forever — which is
  what the dead-letter topic is for.

Watch `mqtt.messages.unacknowledged` — non-zero means the broker is holding
messages for redelivery, which is the difference between "a handler logged an
error" and "delivery is backing up".

## Dead-letter topic

```yaml
mqtt:
  dead-letter:
    enabled: true
    topic: dlq/cloud-service
    max-attempts: 1        # 1 = no retry
```

**Off by default**, deliberately: a starter should not start publishing to a topic
nobody asked for. With it off, a failing listener is simply not acknowledged and
the broker redelivers — correct, but a message that can never succeed is retried
forever and holds an inflight slot until delivery stalls. Turning it on is how you
bound that.

What arrives on the topic:

```json
{
  "originalTopic": "sites/site1/telemetry/ghost",
  "subscription":  "sites/+/telemetry/+",
  "clientId":      "cloud-service",
  "failedAt":      "2026-09-08T03:26:09.651Z",
  "attempts":      1,
  "reason":        "PAYLOAD_UNDECODABLE",
  "error":         "JsonEOFException: Unexpected end-of-input…",
  "payload":       "{\"this\":\"is not a Telemetry\""
}
```

Enough to diagnose and replay without going back to the logs. The payload is kept
as **text** whenever the bytes are valid UTF-8, because the first thing anyone does
with a dead-letter topic is point a topic browser at it; binary falls back to
`payloadBase64`, and exactly one of the two is ever set.

Four decisions worth knowing:

- **Two failure kinds, treated oppositely.** A handler that throws might succeed on
  redelivery, so it is retried `max-attempts` times first. A payload that will not
  parse never will, so it is dead-lettered immediately — retrying it is pure waste.
- **If the dead-letter publish itself fails**, the acknowledgement is withheld.
  Acknowledging at that point would destroy the only remaining copy.
- **Dead letters are not retained.** A retained one would be replayed to every
  future subscriber of the topic, long after it was dealt with.
- **Retries are inline** on the dispatch thread, so they stall everything behind
  them. `max-attempts` is for a transient blip, not for waiting out a dependency.

Grant the producing client write access to the topic and nothing else — draining a
DLQ should be a deliberate act by an operator, not something the service does to
itself on a loop.

## Sending

```java
gateway.send(topic, reading);                                  // configured QoS, no retain
gateway.send(topic, health, MqttSendOptions.retain());         // last-known state
gateway.send(topic, command, MqttSendOptions.expiringIn(ttl)); // MQTT 5 message expiry
```

`send` returns once the **local** broker has accepted the message — not when anything
downstream consumed it. Over a store-and-forward topology that gap can be hours;
anything needing confirmation of arrival needs an acknowledgement on a reply topic.

Use `expiringIn` only for messages that become *wrong* with age, such as commands.
Never for telemetry or health, whose whole value is that they survive an outage.

## Properties

| Property | Default | Notes |
|---|---|---|
| `mqtt.url` | — | Activates the auto-configuration. |
| `mqtt.client-id` | — | Required. Stable and unique. |
| `mqtt.username` | — | Omit for anonymous brokers. |
| `mqtt.password` | — | From a secret store, never a checked-in file. |
| `mqtt.clean-start` | `false` | Durable session. |
| `mqtt.session-expiry` | `4294967295s` | Protocol max, i.e. never. |
| `mqtt.qos` | `1` | Default publish/subscribe QoS. |
| `mqtt.manual-acks` | `false` | `false` acks after the listener returns cleanly; `true` hands it to the listener. |
| `mqtt.dead-letter.enabled` | `false` | Publish unhandleable messages instead of retrying forever. |
| `mqtt.dead-letter.topic` | — | Required when enabled. |
| `mqtt.dead-letter.max-attempts` | `1` | Handler attempts before dead-lettering. Retries are inline. |
| `mqtt.dead-letter.qos` | `1` | A dead letter that is itself dropped defeats the purpose. |
| `mqtt.keep-alive` | `20s` | Short, to detect half-open sockets. |
| `mqtt.connection-timeout` | `10s` | |
| `mqtt.max-reconnect-delay` | `30s` | Backoff ceiling. |
| `mqtt.persistence-directory` | *(empty)* | Empty means in-memory. |
| `mqtt.ssl.trust-store` | — | PKCS12; empty disables TLS. |
| `mqtt.ssl.trust-store-password` | — | |

## Metrics and health

`mqtt.messages.published`, `mqtt.messages.received`, `mqtt.messages.acknowledged`,
`mqtt.messages.unacknowledged`, `mqtt.messages.dead_lettered`,
`mqtt.dispatch.errors`, `mqtt.connected`,
`mqtt.disconnects.total`.

`/actuator/health` reports the broker connection, honouring
`management.health.mqtt.enabled=false`. Actuator is an optional dependency: without
it on the classpath the health auto-configuration simply does not load.

## Tests

50 tests, all using `ApplicationContextRunner` — no broker is contacted.

```bash
mvn test
```
