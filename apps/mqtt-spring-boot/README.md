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
├── annotation/      @MqttListener
├── autoconfigure/   MqttAutoConfiguration, MqttHealthAutoConfiguration
├── core/            MqttConnection, MqttGateway, MqttSendOptions,
│                    MqttSubscription, MqttWill, MqttClientProperties,
│                    MqttPublishException
├── health/          MqttHealthIndicator
├── listener/        MqttListenerRegistry, MqttListenerAnnotationBeanPostProcessor
└── support/         MqttCodec
```

Dependencies point one way: `autoconfigure` → everything, `listener` → `core` +
`support` + `annotation`, `core` → `support`. Nothing depends on `autoconfigure`,
which is what lets the whole thing be used without Spring Boot's auto-config if
someone wants to wire it by hand.

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

Three consequences worth knowing before choosing:

- A withheld acknowledgement is redelivered on **session resume**, not immediately.
  A failed message can wait until the connection next cycles.
- Each unacknowledged message occupies a slot in the inflight window. Enough of them
  and delivery stalls — deliberate backpressure, but it is a stall.
- So a message that can *never* succeed must not be withheld forever. An undecodable
  payload is therefore logged, dropped **and acknowledged**: refusing to ack
  something unparseable just parks it in the window until delivery stops. Route
  genuine poison to a dead-letter topic from inside the listener rather than
  throwing on every redelivery.

Watch `mqtt.messages.unacknowledged` — non-zero means the broker is holding
messages for redelivery, which is the difference between "a handler logged an
error" and "delivery is backing up".

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
| `mqtt.username` / `password` | — | Omit for anonymous brokers. |
| `mqtt.clean-start` | `false` | Durable session. |
| `mqtt.session-expiry` | `4294967295s` | Protocol max, i.e. never. |
| `mqtt.qos` | `1` | Default publish/subscribe QoS. |
| `mqtt.manual-acks` | `false` | `false` acks after the listener returns cleanly; `true` hands it to the listener. |
| `mqtt.keep-alive` | `20s` | Short, to detect half-open sockets. |
| `mqtt.connection-timeout` | `10s` | |
| `mqtt.max-reconnect-delay` | `30s` | Backoff ceiling. |
| `mqtt.persistence-directory` | *(empty)* | Empty means in-memory. |
| `mqtt.ssl.trust-store` | — | PKCS12; empty disables TLS. |
| `mqtt.ssl.trust-store-password` | — | |

## Metrics and health

`mqtt.messages.published`, `mqtt.messages.received`, `mqtt.messages.acknowledged`,
`mqtt.messages.unacknowledged`, `mqtt.dispatch.errors`, `mqtt.connected`,
`mqtt.disconnects.total`.

`/actuator/health` reports the broker connection, honouring
`management.health.mqtt.enabled=false`. Actuator is an optional dependency: without
it on the classpath the health auto-configuration simply does not load.

## Tests

29 tests, all using `ApplicationContextRunner` — no broker is contacted.

```bash
mvn test
```
