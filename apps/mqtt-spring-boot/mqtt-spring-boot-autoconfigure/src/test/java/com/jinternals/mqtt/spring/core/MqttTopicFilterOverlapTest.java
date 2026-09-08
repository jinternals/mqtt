package com.jinternals.mqtt.spring.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Overlap decides whether two listeners may hold different acknowledgement modes, so a wrong answer
 * here is either a spurious startup failure or a silently cancelled manual acknowledgement.
 */
class MqttTopicFilterOverlapTest {

    @ParameterizedTest(name = "{0} vs {1} -> {2}")
    @DisplayName("can two filters ever match the same topic?")
    @CsvSource({
        // identical, and plainly disjoint
        "sites/+/telemetry/+,  sites/+/telemetry/+,  true",
        "sites/+/telemetry/+,  sites/+/health/+,     false",
        "a/b/c,                a/b/d,                false",

        // '+' against a literal at the same level
        "sites/+/telemetry/+,  sites/site1/telemetry/arm-01, true",
        "sites/site1/#,        sites/site2/#,        false",
        "sites/site1/#,        sites/+/telemetry/+,  true",

        // '#' absorbs whatever remains
        "sites/#,              sites/site1/telemetry/arm-01, true",
        "#,                    anything/at/all,      true",
        "dlq/#,                sites/site1/health/x, false",

        // '#' also matches its own parent level, so these do overlap
        "sites/site1/#,        sites/site1,          true",
        "sites/#,              sites,                true",

        // differing depth without a '#' cannot overlap
        "sites/+/telemetry,    sites/+/telemetry/+,  false",
        "a/+,                  a/b/c,                false"
    })
    void overlap(String a, String b, boolean expected) {
        assertThat(MqttTopicFilter.overlap(a, b)).isEqualTo(expected);
        // Overlap is symmetric; asserting both ways guards the walk's exhaustion branches.
        assertThat(MqttTopicFilter.overlap(b, a)).isEqualTo(expected);
    }
}
