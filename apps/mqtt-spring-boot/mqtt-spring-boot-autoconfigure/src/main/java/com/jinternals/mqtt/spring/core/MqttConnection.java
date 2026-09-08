package com.jinternals.mqtt.spring.core;

import com.jinternals.mqtt.spring.annotation.MqttListener;
import com.jinternals.mqtt.spring.listener.MqttListenerRegistry;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
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
    private final MqttListenerRegistry listenerRegistry;
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

    public MqttConnection(
            MqttClientProperties props,
            List<MqttSubscription> subscriptions,
            MqttListenerRegistry listenerRegistry,
            MqttWill will,
            MeterRegistry meters) {
        this.props = props;
        this.subscriptions.addAll(subscriptions);
        this.listenerRegistry = listenerRegistry;
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
        if (listenerRegistry != null) {
            subscriptions.addAll(listenerRegistry.all());
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
                    boolean allHandlersSucceeded = true;
                    for (MqttSubscription sub : subscriptions) {
                        if (matches(sub.topicFilter(), topic)) {
                            try {
                                sub.handler().handle(topic, payload, ack);
                            } catch (RuntimeException e) {
                                // Never let a handler bubble out: on the dispatch thread it would
                                // kill delivery for every other subscription too.
                                allHandlersSucceeded = false;
                                dispatchErrors.increment();
                                log.error("Handler for {} failed on topic {}", sub.topicFilter(), topic, e);
                            }
                        }
                    }

                    if (props.isManualAcks()) {
                        return;
                    }
                    if (allHandlersSucceeded) {
                        acknowledge(messageId, qos, acknowledged);
                    } else {
                        // Deliberately NOT acknowledged. The broker keeps it and redelivers on the
                        // next session resume, which is the only reason a failed handler is not
                        // silent data loss.
                        //
                        // The cost is worth stating: a message that always fails is redelivered on
                        // every reconnect, and each unacknowledged message occupies a slot in the
                        // inflight window. Enough of them and delivery stalls. A handler that cannot
                        // succeed should catch its own exception and route the payload somewhere
                        // (a dead-letter topic, a quarantine table) rather than throwing forever.
                        unacknowledged.increment();
                        log.warn(
                                "Not acknowledging message on {} — a handler failed. It stays with"
                                        + " the broker and will be redelivered on session resume.",
                                topic);
                    }
                });
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

    /** MQTT topic-filter matching: {@code +} spans one level, {@code #} spans the rest. */
    static boolean matches(String filter, String topic) {
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        for (int i = 0; i < f.length; i++) {
            if (f[i].equals("#")) {
                return true;
            }
            if (i >= t.length) {
                return false;
            }
            if (!f[i].equals("+") && !f[i].equals(t[i])) {
                return false;
            }
        }
        return f.length == t.length;
    }

}
