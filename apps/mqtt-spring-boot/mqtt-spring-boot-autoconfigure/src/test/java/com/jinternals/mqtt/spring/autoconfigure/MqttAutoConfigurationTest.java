package com.jinternals.mqtt.spring.autoconfigure;

import com.jinternals.mqtt.spring.core.MqttClientProperties;
import com.jinternals.mqtt.spring.core.MqttConnection;
import com.jinternals.mqtt.spring.core.MqttSubscription;
import com.jinternals.mqtt.spring.core.MqttWill;
import com.jinternals.mqtt.spring.support.MqttCodec;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration behaviour, exercised with {@link ApplicationContextRunner} rather than a full
 * {@code @SpringBootTest} — no broker is contacted, so these run in milliseconds.
 *
 * <p>What is pinned here is the starter's <em>contract</em>: it stays out of the way when unused,
 * yields to any bean the application defines, and fails loudly rather than guessing a client id.
 */
class MqttAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(MqttAutoConfiguration.class));

    @Test
    @DisplayName("does nothing when mqtt.url is absent — the starter is inert until configured")
    void inertWithoutUrl() {
        runner.run(context -> assertThat(context).doesNotHaveBean(MqttConnection.class));
    }

    @Test
    @DisplayName("contributes a connection and codec once mqtt.url and client-id are set")
    void configuresConnection() {
        runner.withPropertyValues("mqtt.url=tcp://localhost:1883", "mqtt.client-id=test-client")
                .run(context -> {
                    assertThat(context).hasSingleBean(MqttConnection.class);
                    assertThat(context).hasSingleBean(MqttCodec.class);
                    assertThat(context.getBean(MqttConnection.class).getClientId())
                            .isEqualTo("test-client");
                });
    }

    @Test
    @DisplayName("fails with an actionable message when client-id is missing")
    void failsWithoutClientId() {
        runner.withPropertyValues("mqtt.url=tcp://localhost:1883")
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .rootCause()
                        .hasMessageContaining("mqtt.client-id")
                        .hasMessageContaining("stable across restarts"));
    }

    @Test
    @DisplayName("persistence is in-memory unless a directory is named, and says so")
    void memoryPersistenceByDefault() {
        runner.withPropertyValues("mqtt.url=tcp://localhost:1883", "mqtt.client-id=test-client")
                .run(context -> {
                    assertThat(context).hasSingleBean(MqttConnection.class);
                    assertThat(context.getBean(MqttClientProperties.class).getPersistenceDirectory())
                            .isEmpty();
                });
    }

    @Test
    @DisplayName("a configured persistence directory is created if missing")
    void createsPersistenceDirectory(@TempDir java.nio.file.Path tempDir) {
        java.nio.file.Path store = tempDir.resolve("nested/spool");
        runner.withPropertyValues(
                        "mqtt.url=tcp://localhost:1883",
                        "mqtt.client-id=test-client",
                        "mqtt.persistence-directory=" + store)
                .run(context -> {
                    assertThat(context).hasSingleBean(MqttConnection.class);
                    assertThat(store).exists().isDirectory();
                });
    }

    @Test
    @DisplayName("property defaults match the durability contract: never-expiring session, QoS 1")
    void durableDefaults() {
        runner.withPropertyValues("mqtt.url=tcp://localhost:1883", "mqtt.client-id=test-client")
                .run(context -> {
                    MqttClientProperties props = context.getBean(MqttClientProperties.class);
                    assertThat(props.isCleanStart()).isFalse();
                    assertThat(props.getQos()).isEqualTo(1);
                    assertThat(props.getSessionExpiry().toSeconds())
                            .isEqualTo(MqttClientProperties.MAX_SESSION_EXPIRY_SECONDS);
                });
    }

    @Test
    @DisplayName("an application's own beans win over the starter's")
    void backsOffForUserBeans() {
        runner.withPropertyValues("mqtt.url=tcp://localhost:1883", "mqtt.client-id=test-client")
                .withUserConfiguration(CustomCodecConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(MqttCodec.class);
                    assertThat(context.getBean(MqttCodec.class))
                            .isSameAs(CustomCodecConfig.INSTANCE);
                });
    }

    @Test
    @DisplayName("declared subscriptions and will are picked up from the context")
    void collectsSubscriptionsAndWill() {
        runner.withPropertyValues("mqtt.url=tcp://localhost:1883", "mqtt.client-id=test-client")
                .withUserConfiguration(SubscriptionsConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(MqttConnection.class);
                    assertThat(context.getBeansOfType(MqttSubscription.class)).hasSize(2);
                    assertThat(context).hasSingleBean(MqttWill.class);
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomCodecConfig {
        static final MqttCodec INSTANCE = new MqttCodec(new com.fasterxml.jackson.databind.ObjectMapper());

        @Bean
        MqttCodec mqttCodec() {
            return INSTANCE;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SubscriptionsConfig {
        @Bean
        MqttSubscription first() {
            return MqttSubscription.atLeastOnce("a/+", (t, p) -> {});
        }

        @Bean
        MqttSubscription second() {
            return MqttSubscription.atLeastOnce("b/#", (t, p) -> {});
        }

        @Bean
        MqttWill will() {
            return new MqttWill("status", new byte[] {1}, 1, true);
        }
    }
}
