package com.jinternals.mqtt.cloud.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;

/**
 * A command sent cloud → edge.
 *
 * <p>Three fields exist purely because the link is unreliable:
 *
 * <ul>
 *   <li>{@code commandId} — the correlation key. The response comes back on a different topic, and
 *       possibly minutes later, so there is nothing else to match it against.
 *   <li>{@code issuedAt} + {@code expiresAt} — a command that queued for three hours behind a dead
 *       WAN link may be actively dangerous to run on arrival (a PICK_ITEM issued during an
 *       outage, naming a bin that has since been restocked). The edge checks expiry before executing and rejects stale work explicitly, rather
 *       than the cloud pretending it was never sent.
 * </ul>
 *
 * <p>Note this is <em>at-least-once</em> delivery: QoS 1 plus a bridge that replays on reconnect
 * means a command can legitimately arrive twice. {@code commandId} is what makes the handler
 * idempotent.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CommandRequest(
        String commandId,
        String site,
        String robotId,
        CommandType type,
        Map<String, String> parameters,
        Instant issuedAt,
        /** Past this instant the edge refuses to execute and answers EXPIRED. */
        Instant expiresAt) {

    public boolean isExpired(Instant now) {
        return expiresAt != null && now.isAfter(expiresAt);
    }
}
