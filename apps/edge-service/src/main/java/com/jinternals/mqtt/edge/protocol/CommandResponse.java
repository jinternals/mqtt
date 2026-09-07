package com.jinternals.mqtt.edge.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;

/** The edge's answer to a {@link CommandRequest}, correlated by {@code commandId}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CommandResponse(
        String commandId,
        String site,
        String robotId,
        Status status,
        Instant completedAt,
        /**
         * Wall-clock time from {@code CommandRequest.issuedAt} to execution at the edge. Across a
         * healthy bridge this is milliseconds; across one that has been queueing it can be hours,
         * and that difference is the single most useful number in this demo.
         */
        Long endToEndLatencyMillis,
        Map<String, String> result,
        String error) {

    public enum Status {
        ACCEPTED,
        /** Ran, but this was a duplicate delivery — replayed by the bridge after a reconnect. */
        DUPLICATE,
        /** Arrived after {@code expiresAt}; deliberately not executed. */
        EXPIRED,
        REJECTED,
        FAILED
    }
}
