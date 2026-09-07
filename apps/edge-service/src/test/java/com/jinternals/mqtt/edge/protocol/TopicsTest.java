package com.jinternals.mqtt.edge.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * These strings must stay byte-identical to the bridge topic map in the site broker configs under
 * {@code broker/edge-siteN/config} and to the broker ACLs. A typo compiles fine and fails silently
 * at runtime as "messages just do not arrive".
 */
class TopicsTest {

    @Test
    @DisplayName("published topics are site-first and match the bridge topic map")
    void publishedTopics() {
        assertThat(Topics.telemetry("site1", "arm-01")).isEqualTo("sites/site1/telemetry/arm-01");
        assertThat(Topics.health("site1", "arm-01")).isEqualTo("sites/site1/health/arm-01");
        assertThat(Topics.commandResponse("site1", "arm-01"))
                .isEqualTo("sites/site1/command/arm-01/res");
    }

    @Test
    @DisplayName("the command filter is scoped to this site only")
    void commandFilterIsSiteScoped() {
        assertThat(Topics.commandRequestFilter("site1")).isEqualTo("sites/site1/command/+/req");
    }

    @Test
    @DisplayName("site and robot are parsed out of an inbound command topic")
    void parsesCommandTopic() {
        Topics.SiteRobot parsed = Topics.parseCommandTopic("sites/site2/command/humanoid-03/req");
        assertThat(parsed).isNotNull();
        assertThat(parsed.site()).isEqualTo("site2");
        assertThat(parsed.robotId()).isEqualTo("humanoid-03");
    }

    @Test
    @DisplayName("anything not matching the scheme yields null rather than throwing on the MQTT thread")
    void malformedTopicReturnsNull() {
        assertThat(Topics.parseCommandTopic("nonsense")).isNull();
        assertThat(Topics.parseCommandTopic("sites/site1/command")).isNull();
        // right shape, wrong leaf -- a response must never be parsed as a request
        assertThat(Topics.parseCommandTopic("sites/site1/command/arm-01/res")).isNull();
        // right shape, wrong kind
        assertThat(Topics.parseCommandTopic("sites/site1/telemetry/arm-01/req")).isNull();
        // missing the sites/ root
        assertThat(Topics.parseCommandTopic("site1/command/arm-01/req")).isNull();
    }
}
