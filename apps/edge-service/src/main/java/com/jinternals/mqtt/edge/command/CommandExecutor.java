package com.jinternals.mqtt.edge.command;

import com.jinternals.mqtt.edge.config.EdgeProperties;
import com.jinternals.mqtt.edge.robot.RobotRegistry;
import com.jinternals.mqtt.edge.robot.RobotState;
import com.jinternals.mqtt.edge.health.HealthPublisher;
import com.jinternals.mqtt.edge.telemetry.TelemetryPublisher;
import com.jinternals.mqtt.edge.protocol.CommandRequest;
import com.jinternals.mqtt.edge.protocol.CommandResponse;
import com.jinternals.mqtt.edge.protocol.Topics;
import com.jinternals.mqtt.spring.annotation.MqttListener;
import com.jinternals.mqtt.spring.core.MqttGateway;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Executes commands that arrive from the cloud and answers on the {@code .../res} topic.
 *
 * <p>This class is where the consequences of an unreliable link actually land in application code,
 * and there are exactly two of them:
 *
 * <ol>
 *   <li><b>Duplicates.</b> QoS 1 is at-least-once, and a bridge that reconnects mid-flight will
 *       redeliver. Anything with a side effect must be idempotent; here that is a bounded set of
 *       recently-seen {@code commandId}s. A command seen twice is answered {@code DUPLICATE} and
 *       not re-executed.
 *   <li><b>Staleness.</b> A command can sit in the cloud broker's queue for hours while a site is
 *       dark and then arrive all at once. Executing a three-hour-old "open the valve" because it
 *       was technically delivered is how store-and-forward systems hurt people. Anything past its
 *       {@code expiresAt} is answered {@code EXPIRED} and deliberately not run.
 * </ol>
 */
@Component
public class CommandExecutor {

    private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);

    private final RobotRegistry registry;
    private final TelemetryPublisher telemetryPublisher;
    private final HealthPublisher healthPublisher;
    private final MqttGateway gateway;
    private final String siteId;

    /** LRU of recently executed command ids. Bounded so a long-lived gateway cannot leak. */
    private final Set<String> executed;

    private final Counter executedCounter;
    private final Counter duplicateCounter;
    private final Counter expiredCounter;

    public CommandExecutor(
            RobotRegistry registry,
            TelemetryPublisher telemetryPublisher,
            HealthPublisher healthPublisher,
            MqttGateway gateway,
            EdgeProperties props,
            MeterRegistry meters) {
        this.registry = registry;
        this.telemetryPublisher = telemetryPublisher;
        this.healthPublisher = healthPublisher;
        this.gateway = gateway;
        this.siteId = props.getSiteId();

        int window = props.getCommandDedupeWindow();
        this.executed =
                Collections.synchronizedSet(
                        Collections.newSetFromMap(
                                new LinkedHashMap<String, Boolean>(window * 2, 0.75f, false) {
                                    @Override
                                    protected boolean removeEldestEntry(Map.Entry<String, Boolean> e) {
                                        return size() > window;
                                    }
                                }));

        this.executedCounter = Counter.builder("edge.commands.executed").register(meters);
        this.duplicateCounter = Counter.builder("edge.commands.duplicate").register(meters);
        this.expiredCounter = Counter.builder("edge.commands.expired").register(meters);
    }

    /**
     * Every command addressed to this site. The topic filter is resolved from configuration, so the
     * same image serves any site.
     *
     * <p>The second parameter is the concrete topic the message arrived on — the only way to
     * recover which robot the {@code +} matched.
     */
    @MqttListener(topic = "sites/${app.edge.site-id}/command/+/req", qos = 1)
    public void onCommand(CommandRequest request, String topic) {
        Topics.SiteRobot addressed = Topics.parseCommandTopic(topic);
        if (addressed == null || !siteId.equals(addressed.site())) {
            log.warn("Command on {} is malformed or not for this site, dropping", topic);
            return;
        }

        Instant now = Instant.now();
        long queuedMillis =
                request.issuedAt() == null ? 0 : Duration.between(request.issuedAt(), now).toMillis();

        CommandResponse response = handle(request, addressed.robotId(), now, queuedMillis);
        respond(addressed.robotId(), response);
    }

    private CommandResponse handle(
            CommandRequest request, String robotId, Instant now, long queuedMillis) {

        if (!registry.contains(robotId) && !HealthPublisher.GATEWAY_ROBOT_ID.equals(robotId)) {
            return reply(request, robotId, CommandResponse.Status.REJECTED, queuedMillis, null,
                    "unknown picking cell " + robotId + " at " + siteId);
        }

        // Ordering matters: check duplicates BEFORE expiry. A redelivered command that has since
        // expired should still be reported as the duplicate it is, otherwise the cloud sees a
        // confusing EXPIRED for a command it already has an ACCEPTED for.
        if (!executed.add(request.commandId())) {
            duplicateCounter.increment();
            log.info("Command {} already executed ({} redelivery), not repeating",
                    request.commandId(), request.type());
            return reply(request, robotId, CommandResponse.Status.DUPLICATE, queuedMillis, null,
                    "already executed at this site");
        }

        if (request.isExpired(now)) {
            expiredCounter.increment();
            log.warn("Command {} ({}) expired after {}ms queued; refusing to execute",
                    request.commandId(), request.type(), queuedMillis);
            return reply(request, robotId, CommandResponse.Status.EXPIRED, queuedMillis, null,
                    "expired at " + request.expiresAt() + ", queued " + queuedMillis + "ms");
        }

        try {
            Map<String, String> result = execute(request, robotId);
            executedCounter.increment();
            log.info("Executed {} {} for {} (queued {}ms)",
                    request.type(), request.commandId(), robotId, queuedMillis);
            return reply(request, robotId, CommandResponse.Status.ACCEPTED, queuedMillis, result, null);
        } catch (IllegalArgumentException e) {
            return reply(request, robotId, CommandResponse.Status.REJECTED, queuedMillis, null,
                    e.getMessage());
        } catch (RuntimeException e) {
            log.error("Command {} failed", request.commandId(), e);
            return reply(request, robotId, CommandResponse.Status.FAILED, queuedMillis, null,
                    e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private Map<String, String> execute(CommandRequest request, String robotId) {
        Map<String, String> result = new LinkedHashMap<>();
        switch (request.type()) {
            case PING -> result.put("pong", Instant.now().toString());

            case PICK_ITEM -> {
                RobotState robot = requireRobot(robotId);
                if (robot.mode() == RobotState.Mode.PAUSED) {
                    // Refused, not queued. A paused cell usually has a person in it; holding the
                    // request to run "later" is how you get a surprise arm movement.
                    throw new IllegalArgumentException(
                            "cell is PAUSED; resume it before sending picks");
                }
                String sku = required(request, "sku");
                String sourceBin = required(request, "sourceBin");
                String targetTote = required(request, "targetTote");

                robot.setMode(RobotState.Mode.PICKING);
                boolean grasped = robot.attemptPick();

                result.put("sku", sku);
                result.put("sourceBin", sourceBin);
                result.put("targetTote", targetTote);
                result.put("grasped", String.valueOf(grasped));
                result.put("picksCompleted", String.valueOf(robot.picksCompleted()));
                if (!grasped) {
                    // A failed grasp is a real outcome, not an error: the item slipped. The cloud
                    // needs to know so it can re-queue the pick rather than assume it shipped.
                    result.put("detail", "grasp failed, item not transferred");
                }
            }

            case PAUSE_PICKING -> {
                RobotState robot = requireRobot(robotId);
                robot.setMode(RobotState.Mode.PAUSED);
                result.put("mode", robot.mode().name());
            }

            case RESUME_PICKING -> {
                RobotState robot = requireRobot(robotId);
                robot.setMode(RobotState.Mode.PICKING);
                result.put("mode", robot.mode().name());
            }

            case SET_TELEMETRY_INTERVAL -> {
                RobotState robot = requireRobot(robotId);
                String raw = required(request, "intervalSeconds");
                long seconds;
                try {
                    seconds = Long.parseLong(raw);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("intervalSeconds is not a number: " + raw);
                }
                if (seconds < 1 || seconds > 3600) {
                    throw new IllegalArgumentException("intervalSeconds must be 1..3600, got " + seconds);
                }
                robot.setInterval(Duration.ofSeconds(seconds));
                result.put("intervalSeconds", String.valueOf(seconds));
            }

            case REPORT_HEALTH -> {
                if (HealthPublisher.GATEWAY_ROBOT_ID.equals(robotId)) {
                    healthPublisher.publishGatewayHealth();
                } else {
                    healthPublisher.publishRobotHealth(requireRobot(robotId));
                }
                result.put("reported", "true");
            }
        }
        return result;
    }

    private static String required(CommandRequest request, String parameter) {
        String value = request.parameters() == null ? null : request.parameters().get(parameter);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(request.type() + " requires parameter '" + parameter + "'");
        }
        return value;
    }

    private RobotState requireRobot(String robotId) {
        RobotState robot = registry.get(robotId);
        if (robot == null) {
            throw new IllegalArgumentException(unknownRobotMessage(robotId));
        }
        return robot;
    }

    private String unknownRobotMessage(String robotId) {
        return "command targets " + robotId + ", which is not a picking cell at " + siteId;
    }

    private CommandResponse reply(
            CommandRequest request,
            String robotId,
            CommandResponse.Status status,
            long latencyMillis,
            Map<String, String> result,
            String error) {
        return new CommandResponse(
                request.commandId(),
                siteId,
                robotId,
                status,
                Instant.now(),
                latencyMillis,
                result,
                error);
    }

    private void respond(String robotId, CommandResponse response) {
        try {
            // Not retained: a stale command result replayed to a new subscriber would be
            // actively misleading. No expiry either — a late answer is still an answer.
            gateway.send(Topics.commandResponse(siteId, robotId), response);
        } catch (RuntimeException e) {
            log.error("Could not publish response for {}: {}", response.commandId(), e.getMessage());
        }
    }
}
