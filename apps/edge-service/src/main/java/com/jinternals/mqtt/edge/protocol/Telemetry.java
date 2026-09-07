package com.jinternals.mqtt.edge.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;

/**
 * A picking-performance sample from one robot.
 *
 * <p>{@code capturedAt} is stamped in the cell, not on arrival in the cloud. After an uplink outage
 * the bridge drains hours of backlog in seconds, so arrival time is meaningless — throughput
 * dashboards, SLA reporting and per-robot performance analysis all have to key off the moment the
 * sample was taken.
 *
 * <p>{@code sequence} is a monotonic per-robot counter and is the cheapest way to detect a gap: if
 * the cloud sees 41 then 43, a sample was dropped, which no amount of QoS 1 delivery accounting
 * will tell you on its own. In a fulfilment centre that gap is missing pick counts, so it shows up
 * later as throughput that does not reconcile.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Telemetry(
        String site,
        String robotId,
        long sequence,
        Instant capturedAt,
        /**
         * {@code picksPerMinute}, {@code graspSuccessRate}, {@code cycleTimeMs},
         * {@code jointTempC}, {@code batteryPercent}.
         */
        Map<String, Double> metrics) {}
