package com.jinternals.mqtt.spring.core;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Connection settings for the local broker.
 *
 * <p>Deliberately named {@code MqttClientProperties} rather than {@code MqttProperties}: Paho v5
 * ships its own {@code org.eclipse.paho.mqttv5.common.packet.MqttProperties} and having two types
 * with the same simple name in one codebase is a recurring source of wrong-import bugs.
 */
@ConfigurationProperties(prefix = MqttClientProperties.PREFIX)
public class MqttClientProperties {

    /** Configuration prefix. Not {@code spring.*} — that namespace belongs to the framework. */
    public static final String PREFIX = "mqtt";

    /** MQTT 5 caps the Session Expiry Interval at a 32-bit second count. */
    public static final long MAX_SESSION_EXPIRY_SECONDS = 4294967295L;

    /**
     * Broker URI, e.g. {@code ssl://broker:8883} or {@code tcp://broker:1883}. The
     * auto-configuration does nothing at all unless this is set.
     */
    private String url;

    /**
     * Client identifier. Required — there is deliberately no default.
     *
     * <p>It must be both <b>stable across restarts</b> and <b>unique across instances</b>. The
     * broker keys the persistent session off it, so a value that changes on boot abandons every
     * queued message, and a value shared by two instances makes them evict each other in a
     * reconnect loop.
     *
     * <p>Defaulting it to {@code spring.application.name} would be convenient and wrong: the
     * moment the same application is deployed twice (two sites, two regions, two replicas) the
     * default silently collides. A property whose wrong value fails invisibly should not have a
     * guessed default.
     */
    private String clientId;

    private String username;
    private String password;

    /**
     * {@code false} asks the broker to resume our previous session — retaining subscriptions and
     * any QoS 1 messages queued for us while we were down. That is the whole point of running a
     * durable client; only set {@code true} for throwaway tooling.
     */
    private boolean cleanStart = false;

    /**
     * How long the broker keeps our session -- and everything queued for it -- after we
     * disconnect. Defaults to the protocol maximum, i.e. never.
     *
     * <p>This is intentional and it is not the same thing as message expiry. Anything queued for
     * this client while it is away is telemetry and health, and neither is ever allowed to expire:
     * a reading captured during an outage is the historical record however late it lands. The only
     * time-based expiry anywhere in this system is the per-command Message Expiry Interval.
     *
     * <p>MQTT 5 encodes this as a 32-bit second count, so {@code 4294967295s} (~136 years) is the
     * documented way to say "never"; larger values are clamped.
     */
    private Duration sessionExpiry = Duration.ofSeconds(MAX_SESSION_EXPIRY_SECONDS);

    /** Default QoS. 1 (at-least-once) everywhere: 0 loses data on a flaky link, 2 costs two extra
     * round trips per message on the link least able to afford them. */
    private int qos = 1;

    /**
     * Who decides when a received message is acknowledged.
     *
     * <p><b>false (default)</b> — the connection acknowledges automatically, but only <em>after</em>
     * the listener returns without throwing. A listener that fails is not acknowledged, so the
     * broker keeps the message and redelivers it when the session next resumes. This is what makes
     * QoS 1 mean at-least-once all the way to your code rather than only as far as the client
     * library.
     *
     * <p><b>true</b> — the listener owns it, via an {@link MqttAcknowledgement} parameter. Use this
     * when "handled" means something more than "the method returned": written to a database,
     * committed to another queue, accepted by a device. Acknowledging before that point is how
     * at-least-once quietly degrades to at-most-once.
     *
     * <p>Either way the starter takes acknowledgement control away from Paho. Paho's own auto-ack
     * fires as soon as its callback returns, and because this client hands work to a dispatch
     * thread, that would acknowledge before the listener had run at all.
     */
    private boolean manualAcks = false;

    public boolean isManualAcks() {
        return manualAcks;
    }

    public void setManualAcks(boolean manualAcks) {
        this.manualAcks = manualAcks;
    }

    /** Short, so a half-open socket is detected in seconds rather than at the OS TCP timeout. */
    private Duration keepAlive = Duration.ofSeconds(20);

    /** How long to wait for CONNACK before treating an attempt as failed and backing off. */
    private Duration connectionTimeout = Duration.ofSeconds(10);

    /** Reconnect backoff ceiling. Paho doubles from 1s up to this value. */
    private Duration maxReconnectDelay = Duration.ofSeconds(30);

    /**
     * Directory for the <b>client-side</b> store of in-flight QoS 1 messages. <b>Empty means
     * in-memory</b>, and that is the default.
     *
     * <h2>What this actually holds — and what it does not</h2>
     *
     * This is Paho's store of messages that have been published but not yet acknowledged by the
     * broker. If the process dies in that window, a restart with the same {@code client-id} and
     * {@code clean-start=false} re-delivers them. That window is the entire scope of this setting.
     *
     * <p>It is <b>not</b> the store-and-forward buffer, and the similarity to mosquitto's
     * {@code persistence_location} is a trap worth naming. When a client publishes to a broker on
     * its own machine, the acknowledgement comes back in about a millisecond, so this directory is
     * empty essentially always — it covers a crash inside that millisecond. The queue that absorbs
     * an outage lives in the <em>broker</em>, and is sized by the broker's own settings.
     *
     * <p>It would carry real traffic only if the client buffered while disconnected (Paho's
     * disconnected-buffer options, not enabled here) or published across an unreliable link
     * directly instead of through a local broker.
     *
     * <h2>Why there is no default path</h2>
     *
     * A library cannot know whether a path it picks is a mounted volume or a container's writable
     * layer that evaporates on restart — and a file store on ephemeral storage is worse than
     * memory, because it <em>looks</em> like durability. So the default is honest and obviously
     * non-durable, and naming a real volume is a deliberate act by whoever knows where one is.
     */
    private String persistenceDirectory = "";

    private final Ssl ssl = new Ssl();

    private final DeadLetter deadLetter = new DeadLetter();

    /**
     * Where messages go when they cannot be handled.
     *
     * <p>Off by default, and that default is deliberate: a starter should not begin publishing to a
     * topic nobody asked for. With it off, a failing listener simply is not acknowledged and the
     * broker redelivers on session resume — correct, but it means a message that can <em>never</em>
     * succeed is retried forever and holds an inflight slot until delivery stalls.
     *
     * <p>Turning it on is how you bound that: after {@code maxAttempts}, the message is published to
     * {@code topic} with its error and then acknowledged, so the queue drains and the failure
     * becomes an object you can inspect and replay rather than a log line and a stall.
     */
    public static class DeadLetter {

        private boolean enabled = false;

        /** Required when enabled. A single topic — the original topic travels in the payload. */
        private String topic;

        /** QoS 1: a dead letter that is itself dropped defeats the purpose. */
        private int qos = 1;

        /**
         * Handler attempts before dead-lettering. {@code 1} (default) means no retry.
         *
         * <p>Retries happen inline on the dispatch thread, so this stalls everything behind it for
         * the duration. Keep it small; it is here for a transient blip, not for waiting out a
         * dependency. An undecodable payload ignores this entirely — it cannot succeed on attempt
         * two either.
         */
        private int maxAttempts = 1;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getQos() {
            return qos;
        }

        public void setQos(int qos) {
            this.qos = qos;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }
    }

    public DeadLetter getDeadLetter() {
        return deadLetter;
    }

    public static class Ssl {
        /** PKCS12 truststore holding the private CA. Empty disables TLS (plaintext, dev only). */
        private String trustStore;

        private String trustStorePassword;

        public String getTrustStore() {
            return trustStore;
        }

        public void setTrustStore(String trustStore) {
            this.trustStore = trustStore;
        }

        public String getTrustStorePassword() {
            return trustStorePassword;
        }

        public void setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isCleanStart() {
        return cleanStart;
    }

    public void setCleanStart(boolean cleanStart) {
        this.cleanStart = cleanStart;
    }

    public Duration getSessionExpiry() {
        return sessionExpiry;
    }

    public void setSessionExpiry(Duration sessionExpiry) {
        this.sessionExpiry = sessionExpiry;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public Duration getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(Duration keepAlive) {
        this.keepAlive = keepAlive;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public Duration getMaxReconnectDelay() {
        return maxReconnectDelay;
    }

    public void setMaxReconnectDelay(Duration maxReconnectDelay) {
        this.maxReconnectDelay = maxReconnectDelay;
    }

    public String getPersistenceDirectory() {
        return persistenceDirectory;
    }

    public void setPersistenceDirectory(String persistenceDirectory) {
        this.persistenceDirectory = persistenceDirectory;
    }

    public Ssl getSsl() {
        return ssl;
    }
}
