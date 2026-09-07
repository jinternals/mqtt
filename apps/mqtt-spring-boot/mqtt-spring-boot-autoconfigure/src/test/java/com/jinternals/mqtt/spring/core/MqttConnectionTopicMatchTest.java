package com.jinternals.mqtt.spring.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Topic-filter matching decides which handler sees which message. Getting it subtly wrong is the
 * kind of bug that shows up as "commands sometimes do not arrive" weeks later, so the wildcard
 * edge cases are pinned down here.
 */
class MqttConnectionTopicMatchTest {

    @ParameterizedTest(name = "{0} matches {1} -> {2}")
    @DisplayName("MQTT wildcard semantics")
    @CsvSource({
        // exact
        "sites/site1/command/arm-01/req, sites/site1/command/arm-01/req, true",
        // '+' spans exactly one level
        "sites/site1/command/+/req,       sites/site1/command/arm-01/req, true",
        "sites/site1/command/+/req,       sites/site2/command/arm-01/req, false",
        "sites/site1/command/+/req,       sites/site1/command/arm-01/res, false",
        // fleet-wide subscription across sites
        "sites/+/telemetry/+,             sites/site1/telemetry/arm-01,   true",
        "sites/+/telemetry/+,             sites/site2/telemetry/humanoid-12,  true",
        // '+' must not swallow multiple levels
        "sites/+/telemetry/+,             sites/site1/telemetry,           false",
        "sites/+/telemetry/+,             sites/site1/telemetry/a/b,       false",
        // the site prefix is what isolates a tenant -- '#' must stay inside it
        "sites/site1/#,                   sites/site1/health/arm-01,      true",
        "sites/site1/#,                   sites/site2/health/arm-01,      false",
        // a filter longer than the topic never matches
        "sites/site1/command/+/req,       sites/site1/command,             false",
        // sibling kinds must not collide
        "sites/+/health/+,                sites/site1/telemetry/arm-01,   false"
    })
    void matches(String filter, String topic, boolean expected) {
        assertThat(MqttConnection.matches(filter, topic)).isEqualTo(expected);
    }
}
