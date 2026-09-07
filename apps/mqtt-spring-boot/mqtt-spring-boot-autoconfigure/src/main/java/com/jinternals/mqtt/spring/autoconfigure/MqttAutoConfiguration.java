package com.jinternals.mqtt.spring.autoconfigure;

import com.jinternals.mqtt.spring.core.MqttGateway;
import com.jinternals.mqtt.spring.listener.MqttListenerAnnotationBeanPostProcessor;
import com.jinternals.mqtt.spring.listener.MqttListenerRegistry;
import com.jinternals.mqtt.spring.core.MqttClientProperties;
import com.jinternals.mqtt.spring.core.MqttConnection;
import com.jinternals.mqtt.spring.core.MqttSubscription;
import com.jinternals.mqtt.spring.core.MqttWill;
import com.jinternals.mqtt.spring.support.MqttCodec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

/**
 * Auto-configuration for a durable MQTT client.
 *
 * <p>Activates only when {@code mqtt.url} is set, so simply having the starter on the classpath
 * costs a consuming application nothing. Everything it contributes is
 * {@link ConditionalOnMissingBean}, so any piece can be replaced by declaring your own.
 *
 * <p>What an application supplies:
 *
 * <ul>
 *   <li>{@code mqtt.*} properties — at minimum {@code url} and {@code client-id};
 *   <li>zero or more {@link MqttSubscription} beans, which are subscribed on every (re)connect;
 *   <li>optionally one {@link MqttWill} bean, installed as the Last Will.
 * </ul>
 *
 * <p>Ordered after Jackson so a consumer's customised {@code ObjectMapper} wins; a standalone one
 * is built only if the application has none.
 */
@AutoConfiguration(after = JacksonAutoConfiguration.class)
@ConditionalOnClass(MqttAsyncClient.class)
@ConditionalOnProperty(prefix = MqttClientProperties.PREFIX, name = "url")
@EnableConfigurationProperties(MqttClientProperties.class)
public class MqttAutoConfiguration {

    /**
     * JSON codec for payloads.
     *
     * <p>Falls back to a self-built mapper with JSR-310 registered, because a library must not
     * assume the consumer is a web application — but if the application defines an
     * {@code ObjectMapper}, that one is used so payloads match the rest of its serialisation.
     */
    @Bean
    @ConditionalOnMissingBean
    public MqttCodec mqttCodec(ObjectProvider<ObjectMapper> objectMapper) {
        return new MqttCodec(
                objectMapper.getIfAvailable(
                        () -> new ObjectMapper().registerModule(new JavaTimeModule())));
    }

    /**
     * The single broker connection.
     *
     * <p>Subscriptions and the Last Will are collected from the context, so this class needs no
     * knowledge of any application's topic scheme.
     *
     * <p>The meter registry is optional: metrics are a convenience, not a prerequisite, so an
     * application without Micrometer configured still gets a working client rather than a startup
     * failure.
     */
    @Bean
    @ConditionalOnMissingBean
    public MqttConnection mqttConnection(
            MqttClientProperties properties,
            ObjectProvider<MqttSubscription> subscriptions,
            MqttListenerRegistry listenerRegistry,
            ObjectProvider<MqttWill> will,
            ObjectProvider<MeterRegistry> meterRegistry) {

        if (!StringUtils.hasText(properties.getClientId())) {
            throw new IllegalStateException(
                    "'"
                            + MqttClientProperties.PREFIX
                            + ".client-id' must be set. It has no default on purpose: the broker keys"
                            + " the persistent session off it, so it must be stable across restarts"
                            + " and unique across instances.");
        }

        List<MqttSubscription> subs = subscriptions.orderedStream().toList();
        return new MqttConnection(
                properties,
                subs,
                listenerRegistry,
                will.getIfAvailable(),
                meterRegistry.getIfAvailable(SimpleMeterRegistry::new));
    }

    /** Outbound API: encode and publish. Symmetric with {@code @MqttListener} for inbound. */
    @Bean
    @ConditionalOnMissingBean
    public MqttGateway mqttGateway(
            MqttConnection connection, MqttCodec codec, MqttClientProperties properties) {
        return new MqttGateway(connection, codec, properties.getQos());
    }

    /**
     * Holds subscriptions discovered from {@code @MqttListener} methods until the connection starts.
     * Declared {@code static} along with the post-processor below, so that instantiating them early
     * does not drag the rest of the configuration into premature initialisation.
     */
    @Bean
    @ConditionalOnMissingBean
    public static MqttListenerRegistry mqttListenerRegistry() {
        return new MqttListenerRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public static MqttListenerAnnotationBeanPostProcessor mqttListenerAnnotationBeanPostProcessor(
            MqttListenerRegistry registry) {
        return new MqttListenerAnnotationBeanPostProcessor(registry);
    }
}
