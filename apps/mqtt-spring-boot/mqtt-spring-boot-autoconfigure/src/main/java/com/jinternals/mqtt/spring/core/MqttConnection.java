package com.jinternals.mqtt.spring.core;

import com.jinternals.mqtt.spring.support.MqttCodec;
import com.jinternals.mqtt.spring.support.MqttPayloadConversionException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClientPersistence;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.client.persist.MqttDefaultFilePersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.StringUtils;

/**
 * Owns the one MQTT connection this service has to its local broker.
 *
 * <p>Both services talk only to a broker on their own side of the WAN — the edge app to its site
 * broker, the cloud app to the cloud broker. Neither ever opens a long-haul MQTT connection, so
 * neither contains a line of code about the flaky link. That is the bridge's job.
 *
 * <p>Two things here are worth more than the rest of the class:
 *
 * <ol>
 *   <li><b>Initial connect is retried by us, not by Paho.</b> Paho's {@code automaticReconnect}
 *       only arms after the <em>first</em> successful connection. A service that starts while its
 *       broker is still booting — the normal case under compose or Kubernetes — would otherwise
 *       stay dead forever with automatic reconnect switched on.
 *   <li><b>Subscriptions are re-applied on every {@code connectComplete}.</b> With
 *       {@code cleanStart=false} the broker normally restores them, but it will not if the session
 *       expired or the broker lost its state, and a silently unsubscribed consumer is the worst
 *       failure mode in this system: everything looks healthy and no messages arrive.
 * </ol>
 */
public class MqttConnection implements SmartLifecycle, MqttCallback {

    private static final Logger log = LoggerFactory.getLogger(MqttConnection.class);

    private final MqttClientProperties props;
    private final List<MqttSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final MqttSubscriptionSource subscriptionSource;
    private final MqttCodec codec;
    private final MqttWill will;
    private final MqttAsyncClient client;
    private final ScheduledExecutorService reconnectScheduler;
    private final ThreadPoolExecutor dispatcher;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong lastConnectedAt = new AtomicLong(0);
    private final AtomicLong disconnectCount = new AtomicLong(0);

    private final Counter published;
    private final Counter received;
    private final Counter dispatchErrors;
    private final Counter acknowledged;
    private final Counter unacknowledged;
    private final Counter deadLettered;

    public MqttConnection(
            MqttClientProperties props,
            List<MqttSubscription> subscriptions,
            MqttSubscriptionSource subscriptionSource,
            MqttWill will,
            MqttCodec codec,
            MeterRegistry meters) {
        this.props = props;
        this.subscriptions.addAll(subscriptions);
        this.subscriptionSource = subscriptionSource;
        this.codec = codec;
        this.will = will;
        try {
            this.client =
                    new MqttAsyncClient(props.getUrl(), props.getClientId(), buildPersistence(props));
        } catch (MqttException e) {
            throw new IllegalStateException("Cannot create MQTT client for " + props.getUrl(), e);
        }
        this.client.setCallback(this);

        // Take acknowledgement control away from Paho, in BOTH modes. Paho's auto-ack fires the
        // moment its callback returns, and messageArrived() below only hands work to a dispatch
        // thread -- so leaving it on would acknowledge every message before the listener had run,
        // making QoS 1 at-least-once only as far as this library and at-most-once beyond it.
        this.client.setManualAcks(true);

        this.reconnectScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "mqtt-connect");
                            t.setDaemon(true);
                            return t;
                        });

        // Single thread on purpose: it keeps per-topic ordering intact (telemetry sequence numbers
        // would otherwise interleave after a backlog drain) while still getting work off Paho's
        // receiver thread, which must not block or the whole connection stalls.
        // CallerRuns is the backpressure valve: if handlers fall behind, Paho's thread does the
        // work itself and stops reading, which lets the broker's queue absorb the burst rather
        // than this process growing an unbounded in-memory one.
        this.dispatcher =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(1000),
                        r -> {
                            Thread t = new Thread(r, "mqtt-dispatch");
                            t.setDaemon(true);
                            return t;
                        },
                        new ThreadPoolExecutor.CallerRunsPolicy());

        this.published = Counter.builder("mqtt.messages.published").register(meters);
        this.received = Counter.builder("mqtt.messages.received").register(meters);
        this.dispatchErrors = Counter.builder("mqtt.dispatch.errors").register(meters);
        this.acknowledged = Counter.builder("mqtt.messages.acknowledged").register(meters);
        // Non-zero here means messages are being held by the broker for redelivery. Alert on it:
        // it is the difference between "a handler logged an error" and "delivery is backing up".
        this.unacknowledged = Counter.builder("mqtt.messages.unacknowledged").register(meters);
        this.deadLettered = Counter.builder("mqtt.messages.dead_lettered").register(meters);
        meters.gauge("mqtt.connected", this, c -> c.isConnected() ? 1d : 0d);
        meters.gauge("mqtt.disconnects.total", this, c -> (double) c.disconnectCount.get());
    }

    /**
     * File persistence when a directory is configured, memory persistence otherwise.
     *
     * <p>Memory persistence means in-flight QoS 1 messages are lost if this process dies before the
     * broker acknowledges them. That is a real trade and it is logged loudly at startup rather than
     * left for someone to discover during an incident.
     */
    private static MqttClientPersistence buildPersistence(MqttClientProperties props) {
        String dir = props.getPersistenceDirectory();
        if (!StringUtils.hasText(dir)) {
            log.warn(
                    "MQTT client persistence is IN MEMORY: in-flight QoS 1 messages are lost on"
                            + " restart. Set '{}.persistence-directory' to a mounted volume for"
                            + " store-and-forward durability.",
                    MqttClientProperties.PREFIX);
            return new MemoryPersistence();
        }
        try {
            // Paho does not create missing parents, and failing here with "directory not found" at
            // first publish is a much worse experience than failing at startup.
            Files.createDirectories(Path.of(dir));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create MQTT persistence directory " + dir, e);
        }
        log.info("MQTT client persistence directory: {}", dir);
        return new MqttDefaultFilePersistence(dir);
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        // Merged here rather than in the constructor: @MqttListener methods are discovered as beans
        // are initialised, which can happen after this bean is built. start() is run by the
        // lifecycle processor once every singleton exists, so by now the registry is complete.
        if (subscriptionSource != null) {
            subscriptions.addAll(subscriptionSource.subscriptions());
        }
        if (subscriptions.isEmpty()) {
            log.info("No MQTT subscriptions declared; this client will publish only.");
        }
        scheduleConnect(0);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        reconnectScheduler.shutdownNow();
        try {
            if (client.isConnected()) {
                // A clean DISCONNECT tells the broker NOT to fire our Last Will. Skipping this is
                // why so many deployments show phantom DOWN events on every rolling restart.
                client.disconnect().waitForCompletion(TimeUnit.SECONDS.toMillis(5));
            }
        } catch (MqttException e) {
            log.warn("Unclean MQTT disconnect: {}", e.getMessage());
        } finally {
            try {
                client.close(true);
            } catch (MqttException ignored) {
                // closing a client we are discarding anyway
            }
            dispatcher.shutdown();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /** Start late and stop early, so handlers are wired before messages start arriving. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    // ------------------------------------------------------------------ connecting

    private void scheduleConnect(long delaySeconds) {
        if (!running.get() || reconnectScheduler.isShutdown()) {
            return;
        }
        reconnectScheduler.schedule(this::attemptConnect, delaySeconds, TimeUnit.SECONDS);
    }

    private void attemptConnect() {
        if (!running.get()) {
            return;
        }
        try {
            log.info("Connecting to {} as clientId={}", props.getUrl(), props.getClientId());
            client.connect(buildOptions()).waitForCompletion(props.getConnectionTimeout().toMillis() * 2);
        } catch (Exception e) {
            long retryIn = Math.min(props.getMaxReconnectDelay().toSeconds(), 5);
            log.warn(
                    "Initial connect to {} failed ({}). Retrying in {}s.",
                    props.getUrl(),
                    e.getMessage(),
                    retryIn);
            scheduleConnect(retryIn);
        }
    }

    private MqttConnectionOptions buildOptions() throws Exception {
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setServerURIs(new String[] {props.getUrl()});
        options.setCleanStart(props.isCleanStart());
        options.setSessionExpiryInterval(
                Math.min(
                        props.getSessionExpiry().toSeconds(),
                        MqttClientProperties.MAX_SESSION_EXPIRY_SECONDS));
        options.setKeepAliveInterval((int) props.getKeepAlive().toSeconds());
        options.setConnectionTimeout((int) props.getConnectionTimeout().toSeconds());
        options.setAutomaticReconnect(true);
        options.setAutomaticReconnectDelay(1, (int) props.getMaxReconnectDelay().toSeconds());

        if (StringUtils.hasText(props.getUsername())) {
            options.setUserName(props.getUsername());
            options.setPassword(props.getPassword().getBytes(StandardCharsets.UTF_8));
        }
        if (StringUtils.hasText(props.getSsl().getTrustStore())) {
            options.setSocketFactory(buildSslContext().getSocketFactory());
            options.setHttpsHostnameVerificationEnabled(true);
        }
        if (will != null) {
            MqttMessage willMessage = new MqttMessage(will.payload());
            willMessage.setQos(will.qos());
            willMessage.setRetained(will.retained());
            options.setWill(will.topic(), willMessage);
            log.info("Last Will registered on {}", will.topic());
        }
        return options;
    }

    private SSLContext buildSslContext() throws Exception {
        KeyStore trust = KeyStore.getInstance("PKCS12");
        try (InputStream in = new FileInputStream(props.getSsl().getTrustStore())) {
            trust.load(in, props.getSsl().getTrustStorePassword().toCharArray());
        }
        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext ctx = SSLContext.getInstance("TLSv1.2");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    // ------------------------------------------------------------------ publishing

    public void publish(String topic, byte[] payload, int qos, boolean retained) {
        publish(topic, payload, qos, retained, null);
    }

    /**
     * @param expirySeconds MQTT 5 Message Expiry Interval, or {@code null} for no expiry.
     *     <p>This is a broker-enforced deadline: a message that has waited longer than this in a
     *     queue is discarded rather than delivered. It belongs on <b>commands only</b>. Telemetry
     *     and health must never carry one — the entire purpose of the store-and-forward bridge is
     *     that a reading captured during a six-hour outage still arrives, however late. Expiring
     *     them would silently punch holes in the historical record, which is the one thing the
     *     backlog exists to prevent. Commands are the opposite: a stale command is a hazard, not
     *     an asset.
     */
    public void publish(String topic, byte[] payload, int qos, boolean retained, Long expirySeconds) {
        try {
            MqttMessage message = new MqttMessage(payload);
            message.setQos(qos);
            message.setRetained(retained);
            if (expirySeconds != null) {
                MqttProperties properties = new MqttProperties();
                properties.setMessageExpiryInterval(expirySeconds);
                message.setProperties(properties);
            }
            client.publish(topic, message);
            published.increment();
        } catch (MqttException e) {
            // With a local broker and QoS 1 file persistence this is rare, and when it does happen
            // (client not connected, or the persistence store is full) it is not something a retry
            // in this method can fix. Surface it; the caller decides.
            throw new MqttPublishException(topic, e);
        }
    }

    public void publish(String topic, byte[] payload) {
        publish(topic, payload, props.getQos(), false);
    }

    // ------------------------------------------------------------------ callbacks

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        lastConnectedAt.set(System.currentTimeMillis());
        log.info("{} to {}", reconnect ? "Reconnected" : "Connected", serverURI);
        for (MqttSubscription sub : subscriptions) {
            try {
                client.subscribe(sub.topicFilter(), sub.qos());
                log.info("Subscribed {} qos={}", sub.topicFilter(), sub.qos());
            } catch (MqttException e) {
                log.error("Subscribe to {} failed; scheduling reconnect", sub.topicFilter(), e);
                scheduleConnect(5);
            }
        }
    }

    @Override
    public void disconnected(MqttDisconnectResponse response) {
        disconnectCount.incrementAndGet();
        log.warn(
                "Disconnected from broker: {}",
                response.getReasonString() != null
                        ? response.getReasonString()
                        : "reason code " + response.getReturnCode());
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        received.increment();

        // Captured before handing off: the dispatch thread needs them to acknowledge, and Paho may
        // reuse the MqttMessage instance once this method returns.
        int messageId = message.getId();
        int qos = message.getQos();
        byte[] payload = message.getPayload();

        // One acknowledgement per delivery, however many subscriptions match, and safe to call more
        // than once so a manual-mode listener retrying does not have to remember whether it did.
        AtomicBoolean acknowledged = new AtomicBoolean(false);
        MqttAcknowledgement ack = () -> acknowledge(messageId, qos, acknowledged);

        dispatcher.execute(
                () -> {
                    boolean safeToAcknowledge = true;
                    boolean listenerOwnsAck = false;
                    for (MqttSubscription sub : subscriptions) {
                        if (matches(sub.topicFilter(), topic)) {
                            listenerOwnsAck |= sub.isManual(props.isManualAcks());
                            safeToAcknowledge &= deliver(sub, topic, payload, ack);
                        }
                    }

                    if (listenerOwnsAck) {
                        // A manual listener matched. Startup validation guarantees no AUTO listener
                        // overlaps it, so there is no one here to acknowledge on its behalf.
                        return;
                    }
                    if (safeToAcknowledge) {
                        acknowledge(messageId, qos, acknowledged);
                    } else {
                        // Deliberately NOT acknowledged. The broker keeps it and redelivers on the
                        // next session resume, which is the only reason a failed handler is not
                        // silent data loss.
                        //
                        // This branch is reached only when dead-lettering is off or itself failed.
                        // Left unbounded it is a slow stall: each unacknowledged message holds an
                        // inflight slot, and a message that can never succeed holds one forever.
                        // Configure mqtt.dead-letter.* to bound it.
                        unacknowledged.increment();
                        log.warn(
                                "Not acknowledging message on {} — a handler failed and it was not"
                                        + " dead-lettered. It stays with the broker and will be"
                                        + " redelivered on session resume.",
                                topic);
                    }
                });
    }

    /**
     * Runs one subscription's handler.
     *
     * @return whether it is safe to acknowledge — true if the handler succeeded, or if the failure
     *     was recorded somewhere durable (the dead-letter topic) or is one that redelivery could
     *     never fix
     */
    private boolean deliver(MqttSubscription sub, String topic, byte[] payload, MqttAcknowledgement ack) {
        int maxAttempts = props.getDeadLetter().isEnabled()
                ? Math.max(1, props.getDeadLetter().getMaxAttempts())
                : 1;

        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                sub.handler().handle(topic, payload, ack);
                return true;
            } catch (MqttPayloadConversionException e) {
                // No retry: bytes that will not parse now will not parse on attempt two.
                dispatchErrors.increment();
                return deadLetter(sub, topic, payload, 1, MqttDeadLetter.Reason.PAYLOAD_UNDECODABLE, e)
                        // Even with no dead-letter topic this is acknowledged. Holding an
                        // unparseable message costs an inflight slot and buys nothing.
                        || true;
            } catch (RuntimeException e) {
                // Never let a handler bubble out: on the dispatch thread it would kill delivery for
                // every other subscription too.
                lastFailure = e;
                dispatchErrors.increment();
                log.error("Handler for {} failed on topic {} (attempt {}/{})",
                        sub.topicFilter(), topic, attempt, maxAttempts, e);
            }
        }
        return deadLetter(sub, topic, payload, maxAttempts, MqttDeadLetter.Reason.HANDLER_FAILED, lastFailure);
    }

    /**
     * Publishes a failed message to the dead-letter topic.
     *
     * @return whether the failure is now recorded somewhere durable. {@code false} means the caller
     *     must withhold the acknowledgement — dead-lettering is off, or the publish itself failed,
     *     and acknowledging would destroy the only remaining copy.
     */
    private boolean deadLetter(
            MqttSubscription sub,
            String topic,
            byte[] payload,
            int attempts,
            MqttDeadLetter.Reason reason,
            RuntimeException failure) {

        MqttClientProperties.DeadLetter config = props.getDeadLetter();
        if (!config.isEnabled() || !StringUtils.hasText(config.getTopic())) {
            return false;
        }
        try {
            String text = asUtf8(payload);
            MqttDeadLetter entry =
                    new MqttDeadLetter(
                            topic,
                            sub.topicFilter(),
                            props.getClientId(),
                            Instant.now(),
                            attempts,
                            reason,
                            describe(failure),
                            text,
                            text != null ? null : Base64.getEncoder().encodeToString(payload));

            // Not retained, deliberately: a retained dead letter would be replayed to every future
            // subscriber of the dead-letter topic, long after it had been dealt with.
            publish(config.getTopic(), codec.encode(entry), config.getQos(), false, null);
            deadLettered.increment();
            log.warn("Dead-lettered message from {} to {} after {} attempt(s): {}",
                    topic, config.getTopic(), attempts, reason);
            return true;
        } catch (RuntimeException e) {
            // The dead-letter publish itself failed. Withhold the acknowledgement so the broker
            // keeps the original — losing it here would be the worst outcome of all.
            log.error("Could not dead-letter message from {} to {}; withholding acknowledgement",
                    topic, config.getTopic(), e);
            return false;
        }
    }

    private static String asUtf8(byte[] payload) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(payload)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static String describe(Throwable failure) {
        if (failure == null) {
            return null;
        }
        Throwable root = failure.getCause() != null ? failure.getCause() : failure;
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    private void acknowledge(int messageId, int qos, AtomicBoolean alreadyAcknowledged) {
        if (qos == 0 || !alreadyAcknowledged.compareAndSet(false, true)) {
            // QoS 0 has nothing to acknowledge, and a second call is a no-op rather than an error.
            return;
        }
        try {
            client.messageArrivedComplete(messageId, qos);
            acknowledged.increment();
        } catch (MqttException e) {
            // The connection dropped before we could acknowledge. Correct outcome anyway: the
            // broker never saw an ack, so it still owns the message and will redeliver it.
            alreadyAcknowledged.set(false);
            log.warn("Could not acknowledge message {}: {}", messageId, e.getMessage());
        }
    }

    @Override
    public void deliveryComplete(IMqttToken token) {
        // QoS 1 PUBACK received. Nothing to do — Paho has already removed the message from the
        // persistence store at this point.
    }

    @Override
    public void mqttErrorOccurred(MqttException exception) {
        log.warn("MQTT protocol error: {}", exception.getMessage());
    }

    @Override
    public void authPacketArrived(int reasonCode, org.eclipse.paho.mqttv5.common.packet.MqttProperties properties) {
        // Only used by enhanced (SASL-style) authentication, which this demo does not enable.
    }

    // ------------------------------------------------------------------ accessors

    public boolean isConnected() {
        return client.isConnected();
    }

    public long getDisconnectCount() {
        return disconnectCount.get();
    }

    public long getLastConnectedAt() {
        return lastConnectedAt.get();
    }

    public String getClientId() {
        return props.getClientId();
    }

    public String getServerUri() {
        return props.getUrl();
    }

    /** MQTT topic-filter matching. See {@link MqttTopicFilter}. */
    static boolean matches(String filter, String topic) {
        return MqttTopicFilter.matches(filter, topic);
    }

}
