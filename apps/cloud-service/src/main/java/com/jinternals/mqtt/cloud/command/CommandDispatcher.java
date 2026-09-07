package com.jinternals.mqtt.cloud.command;

import com.jinternals.mqtt.cloud.config.CloudProperties;
import com.jinternals.mqtt.cloud.protocol.Telemetry;
import com.jinternals.mqtt.cloud.protocol.CommandRequest;
import com.jinternals.mqtt.cloud.protocol.CommandResponse;
import com.jinternals.mqtt.cloud.protocol.CommandType;
import com.jinternals.mqtt.cloud.protocol.Topics;
import com.jinternals.mqtt.spring.core.MqttGateway;
import com.jinternals.mqtt.spring.core.MqttSendOptions;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Sends commands to sites and matches the answers back up.
 *
 * <p>The API this backs is asynchronous, and it has to be. There is no request/response over a link
 * that may be down: a publish here means "the cloud broker has durably accepted this and will hand
 * it to the site whenever the site next appears" — which could be in 4ms or in four hours. An API
 * that blocked waiting for the robot would just be a timeout generator.
 *
 * <p>So {@code dispatch} returns as soon as the broker has the message, and the caller polls or
 * subscribes for the result.
 */
@Service
public class CommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    private final MqttGateway gateway;
    private final CloudProperties props;
    private final Map<String, PendingCommand> pending = new ConcurrentHashMap<>();

    private final Counter issued;
    private final Counter answered;

    public CommandDispatcher(
            MqttGateway gateway, CloudProperties props, MeterRegistry meters) {
        this.gateway = gateway;
        this.props = props;
        this.issued = Counter.builder("cloud.commands.issued").register(meters);
        this.answered = Counter.builder("cloud.commands.answered").register(meters);
        meters.gauge("cloud.commands.pending", pending, Map::size);
    }

    public CommandRequest dispatch(
            String site, String robotId, CommandType type, Map<String, String> parameters, Duration ttl) {

        Instant now = Instant.now();
        Duration effectiveTtl = ttl != null ? ttl : props.getDefaultCommandTtl();

        CommandRequest request =
                new CommandRequest(
                        UUID.randomUUID().toString(),
                        site,
                        robotId,
                        type,
                        parameters,
                        now,
                        now.plus(effectiveTtl));

        // QoS 1, not retained. Retaining a command would be a genuine hazard: every future
        // subscriber on that topic -- including the site's own bridge after a reconnect -- would
        // receive and execute it again.
        //
        // The TTL is enforced twice, on purpose:
        //   * as an MQTT 5 Message Expiry Interval, so the CLOUD BROKER drops the command from an
        //     offline site's queue once it lapses and never spends bandwidth shipping it. Note the
        //     property itself does not survive the 3.1.1 bridge hop -- but it does not need to,
        //     because the queue that holds a command during an outage is the cloud broker's;
        //   * as `expiresAt` inside the payload, which the edge re-checks on arrival, because
        //     broker-level expiry is a delivery deadline and says nothing about how long the
        //     message then sat behind other work at the site.
        // Telemetry and health deliberately carry neither -- see MqttConnection#publish.
        gateway.send(
                Topics.commandRequest(site, robotId),
                request,
                MqttSendOptions.expiringIn(effectiveTtl));

        pending.put(request.commandId(), new PendingCommand(request, now));
        issued.increment();
        log.info("Issued {} {} to {}/{} (ttl {})", type, request.commandId(), site, robotId, effectiveTtl);
        return request;
    }

    /** Called by {@code FleetListener} for every command result that arrives from any site. */
    public void onResponse(CommandResponse response) {
        PendingCommand entry = pending.get(response.commandId());
        if (entry == null) {
            // Normal after a cloud-service restart: the site answered a command issued by the
            // previous process. Worth logging, not worth alarming about.
            log.info("Response for unknown command {} ({})", response.commandId(), response.status());
            return;
        }
        entry.response = response;
        entry.completedAt = Instant.now();
        answered.increment();
        log.info("Command {} -> {} after {}ms end-to-end",
                response.commandId(), response.status(), response.endToEndLatencyMillis());
    }

    public PendingCommandView find(String commandId) {
        PendingCommand entry = pending.get(commandId);
        return entry == null ? null : entry.toView(props.getDefaultCommandTtl());
    }

    public List<PendingCommandView> recent() {
        List<PendingCommandView> out = new ArrayList<>();
        for (PendingCommand c : pending.values()) {
            out.add(c.toView(props.getDefaultCommandTtl()));
        }
        out.sort(Comparator.comparing(PendingCommandView::issuedAt).reversed());
        return out;
    }

    /** Drops old command records so a long-running process does not grow without bound. */
    @Scheduled(fixedDelay = 60_000)
    public void evictExpired() {
        Instant cutoff = Instant.now().minus(props.getCommandRetention());
        pending.values().removeIf(c -> c.issuedAt.isBefore(cutoff));
    }

    private static final class PendingCommand {
        final CommandRequest request;
        final Instant issuedAt;
        volatile CommandResponse response;
        volatile Instant completedAt;

        PendingCommand(CommandRequest request, Instant issuedAt) {
            this.request = request;
            this.issuedAt = issuedAt;
        }

        PendingCommandView toView(Duration defaultTtl) {
            String state;
            if (response != null) {
                state = response.status().name();
            } else if (request.isExpired(Instant.now())) {
                // The TTL lapsed before the site ever picked it up. The site will still receive it
                // when it reconnects, and will answer EXPIRED -- which is the honest outcome.
                state = "EXPIRED_UNDELIVERED";
            } else {
                state = "PENDING";
            }
            return new PendingCommandView(
                    request.commandId(),
                    request.site(),
                    request.robotId(),
                    request.type(),
                    state,
                    issuedAt,
                    request.expiresAt(),
                    completedAt,
                    response);
        }
    }

    public record PendingCommandView(
            String commandId,
            String site,
            String robotId,
            CommandType type,
            String state,
            Instant issuedAt,
            Instant expiresAt,
            Instant completedAt,
            CommandResponse response) {}
}
