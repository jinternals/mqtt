package com.jinternals.mqtt.spring.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.jinternals.mqtt.spring.annotation.MqttListener;
import com.jinternals.mqtt.spring.core.MqttAcknowledgement;
import com.jinternals.mqtt.spring.core.MqttSubscription;
import com.jinternals.mqtt.spring.support.MqttCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Discovery, payload conversion and placeholder resolution for {@link MqttListener}. */
class MqttListenerAnnotationBeanPostProcessorTest {

    private static final MqttAcknowledgement NOOP_ACK = () -> {};

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner().withUserConfiguration(Infrastructure.class);
    }

    /** The three beans the starter's auto-configuration would normally provide. */
    @Configuration(proxyBeanMethods = false)
    static class Infrastructure {

        @Bean
        MqttCodec mqttCodec() {
            return new MqttCodec(new ObjectMapper());
        }

        @Bean
        static MqttListenerRegistry mqttListenerRegistry() {
            return new MqttListenerRegistry();
        }

        @Bean
        static MqttListenerAnnotationBeanPostProcessor mqttListenerBpp(MqttListenerRegistry r) {
            return new MqttListenerAnnotationBeanPostProcessor(r);
        }
    }

    @Test
    @DisplayName("annotated methods become subscriptions, with ${...} resolved from the environment")
    void discoversAndResolvesPlaceholders() {
        runner().withPropertyValues("app.site-id=site7")
                .withUserConfiguration(TypedListener.class)
                .run(context -> {
                    MqttListenerRegistry registry = context.getBean(MqttListenerRegistry.class);
                    assertThat(registry.size()).isEqualTo(1);
                    MqttSubscription sub = registry.all().get(0);
                    assertThat(sub.topicFilter()).isEqualTo("sites/site7/command/+/req");
                    assertThat(sub.qos()).isEqualTo(1);
                });
    }

    @Test
    @DisplayName("JSON payload is decoded and the concrete topic is passed through")
    void decodesPayloadAndPassesTopic() {
        runner().withPropertyValues("app.site-id=site7")
                .withUserConfiguration(TypedListener.class)
                .run(context -> {
                    TypedListener bean = context.getBean(TypedListener.class);
                    MqttSubscription sub =
                            context.getBean(MqttListenerRegistry.class).all().get(0);

                    sub.handler()
                            .handle(
                                    "sites/site7/command/arm-01/req",
                                    "{\"name\":\"OPEN\"}".getBytes(StandardCharsets.UTF_8),
                                    NOOP_ACK);

                    assertThat(bean.received).hasSize(1);
                    assertThat(bean.received.get(0).name()).isEqualTo("OPEN");
                    assertThat(bean.topics).containsExactly("sites/site7/command/arm-01/req");
                });
    }

    @Test
    @DisplayName("a malformed payload is dropped, not propagated — one bad message cannot stop a feed")
    void malformedPayloadIsDropped() {
        runner().withPropertyValues("app.site-id=site7")
                .withUserConfiguration(TypedListener.class)
                .run(context -> {
                    TypedListener bean = context.getBean(TypedListener.class);
                    MqttSubscription sub =
                            context.getBean(MqttListenerRegistry.class).all().get(0);

                    sub.handler().handle("sites/site7/command/arm-01/req", "not json".getBytes(), NOOP_ACK);

                    assertThat(bean.received).isEmpty();
                });
    }

    @Test
    @DisplayName("byte[] and String payload types bypass JSON decoding")
    void rawPayloadTypes() {
        runner().withUserConfiguration(RawListener.class)
                .run(context -> {
                    RawListener bean = context.getBean(RawListener.class);
                    for (MqttSubscription sub :
                            context.getBean(MqttListenerRegistry.class).all()) {
                        sub.handler().handle("raw/x", "hello".getBytes(StandardCharsets.UTF_8), NOOP_ACK);
                    }
                    assertThat(bean.asString).isEqualTo("hello");
                    assertThat(bean.asBytes).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
                });
    }

    @Test
    @DisplayName("an unusable signature fails at startup, not on the first message")
    void rejectsBadSignature() {
        runner().withUserConfiguration(BadSignature.class)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    @DisplayName("an exception thrown by a listener is reported with its own cause")
    void unwrapsListenerException() {
        runner().withUserConfiguration(ThrowingListener.class)
                .run(context -> {
                    MqttSubscription sub =
                            context.getBean(MqttListenerRegistry.class).all().get(0);
                    assertThatThrownBy(() -> sub.handler().handle("boom/1", "{}".getBytes(), NOOP_ACK))
                            .isInstanceOf(IllegalStateException.class)
                            .hasRootCauseMessage("listener blew up");
                });
    }

    public record Command(String name) {}

    @Configuration(proxyBeanMethods = false)
    static class TypedListener {
        final List<Command> received = new ArrayList<>();
        final List<String> topics = new ArrayList<>();

        @MqttListener(topic = "sites/${app.site-id}/command/+/req")
        void onCommand(Command command, String topic) {
            received.add(command);
            topics.add(topic);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RawListener {
        String asString;
        byte[] asBytes;

        @MqttListener(topic = "raw/+")
        void onString(String payload) {
            this.asString = payload;
        }

        @MqttListener(topic = "raw/+")
        void onBytes(byte[] payload) {
            this.asBytes = payload;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class BadSignature {
        @MqttListener(topic = "bad/+")
        void noParameters() {}
    }

    @Configuration(proxyBeanMethods = false)
    static class ThrowingListener {
        @MqttListener(topic = "boom/+")
        void onMessage(Command command) {
            throw new IllegalArgumentException("listener blew up");
        }
    }

    @Test
    @DisplayName("an MqttAcknowledgement parameter is injected, and resolved by type not position")
    void injectsAcknowledgement() {
        runner().withUserConfiguration(AckListener.class)
                .run(context -> {
                    AckListener bean = context.getBean(AckListener.class);
                    java.util.concurrent.atomic.AtomicInteger acks = new java.util.concurrent.atomic.AtomicInteger();
                    for (MqttSubscription sub : context.getBean(MqttListenerRegistry.class).all()) {
                        sub.handler().handle("ack/x", "{\"name\":\"N\"}".getBytes(StandardCharsets.UTF_8), acks::incrementAndGet);
                    }
                    // Both signatures got a working handle: (payload, ack) and (payload, topic, ack).
                    assertThat(acks.get()).isEqualTo(2);
                    assertThat(bean.topicSeen).isEqualTo("ack/x");
                });
    }

    @Test
    @DisplayName("an undecodable payload is acknowledged — it can never succeed, so holding it stalls delivery")
    void undecodablePayloadIsAcknowledged() {
        runner().withPropertyValues("app.site-id=site7")
                .withUserConfiguration(TypedListener.class)
                .run(context -> {
                    java.util.concurrent.atomic.AtomicInteger acks = new java.util.concurrent.atomic.AtomicInteger();
                    MqttSubscription sub = context.getBean(MqttListenerRegistry.class).all().get(0);
                    sub.handler().handle("sites/site7/command/arm-01/req", "not json".getBytes(), acks::incrementAndGet);
                    assertThat(acks.get()).isEqualTo(1);
                });
    }

    @Test
    @DisplayName("an unsupported parameter type fails at startup, naming what is allowed")
    void rejectsUnsupportedParameter() {
        runner().withUserConfiguration(UnsupportedParam.class)
                .run(context -> assertThat(context).hasFailed());
    }

    @Configuration(proxyBeanMethods = false)
    static class AckListener {
        volatile String topicSeen;

        @MqttListener(topic = "ack/+")
        void withAckOnly(Command command, MqttAcknowledgement ack) {
            ack.acknowledge();
        }

        @MqttListener(topic = "ack/+")
        void withTopicAndAck(Command command, String topic, MqttAcknowledgement ack) {
            this.topicSeen = topic;
            ack.acknowledge();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UnsupportedParam {
        @MqttListener(topic = "bad/+")
        void wrongType(Command command, Integer nonsense) {}
    }
}
