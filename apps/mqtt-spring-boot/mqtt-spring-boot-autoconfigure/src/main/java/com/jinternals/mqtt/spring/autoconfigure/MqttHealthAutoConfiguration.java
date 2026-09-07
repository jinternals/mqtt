package com.jinternals.mqtt.spring.autoconfigure;

import com.jinternals.mqtt.spring.core.MqttConnection;
import com.jinternals.mqtt.spring.health.MqttHealthIndicator;

import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Contributes {@code /actuator/health} detail for the broker connection.
 *
 * <p>Split out from {@link MqttAutoConfiguration} so that actuator stays a genuinely optional
 * dependency: this class is only loaded when the consumer has actuator on the classpath, and even
 * then it honours {@code management.health.mqtt.enabled=false}.
 */
@AutoConfiguration(after = MqttAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
@ConditionalOnBean(MqttConnection.class)
@ConditionalOnEnabledHealthIndicator("mqtt")
public class MqttHealthAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "mqttHealthIndicator")
    public MqttHealthIndicator mqttHealthIndicator(MqttConnection connection) {
        return new MqttHealthIndicator(connection);
    }
}
