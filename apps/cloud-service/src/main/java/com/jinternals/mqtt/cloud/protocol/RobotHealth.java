package com.jinternals.mqtt.cloud.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;

/**
 * Robot liveness, published <b>retained</b> so a freshly started cloud service — or a cloud broker
 * that has just come back — learns the state of every cell immediately, instead of waiting a full
 * heartbeat interval per robot to find out whether anything is picking.
 *
 * <p>The same topic carries the cell's Last Will. The broker publishes a {@code DOWN} variant if
 * the gateway's session ends without a clean disconnect, which overwrites the retained {@code UP} —
 * so the retained value is always the truth, never a stale success.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RobotHealth(
        String site,
        String robotId,
        HealthStatus status,
        Instant reportedAt,
        Long uptimeSeconds,
        /** Picks completed since boot. Pairs with the throughput reported in telemetry. */
        Long picksCompleted,
        /** {@code state}, {@code grasp}, {@code estop}, {@code batteryPercent}. */
        Map<String, String> checks,
        String detail) {

    /** The payload the broker retains on our behalf if we die without saying goodbye. */
    public static RobotHealth lastWill(String site, String robotId) {
        return new RobotHealth(
                site,
                robotId,
                HealthStatus.DOWN,
                null,
                null,
                null,
                null,
                "session ended without a clean disconnect (broker Last Will)");
    }
}
