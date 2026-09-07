package com.jinternals.mqtt.cloud.api;

import com.jinternals.mqtt.cloud.command.CommandDispatcher;
import com.jinternals.mqtt.cloud.fleet.FleetRegistry;
import com.jinternals.mqtt.cloud.protocol.CommandType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The operator-facing API.
 *
 * <p>Command submission returns {@code 202 Accepted}, not {@code 200}. That status code is the
 * contract: the cloud broker has durably taken the command, and nothing more is promised. Whether
 * the site is reachable right now is not knowable at this point and pretending otherwise would put
 * a lie in the API.
 */
@RestController
@RequestMapping("/api/v1")
public class FleetController {

    private final FleetRegistry registry;
    private final CommandDispatcher dispatcher;

    public FleetController(FleetRegistry registry, CommandDispatcher dispatcher) {
        this.registry = registry;
        this.dispatcher = dispatcher;
    }

    /** Last-known state of every robot across every site. */
    @GetMapping("/fleet")
    public List<FleetRegistry.RobotSnapshot> fleet() {
        return registry.snapshot();
    }

    /** WAN link state per site, straight from mosquitto's bridge notifications. */
    @GetMapping("/bridges")
    public List<FleetRegistry.BridgeSnapshot> bridges() {
        return registry.bridgeSnapshot();
    }

    @PostMapping("/sites/{site}/robots/{robotId}/commands")
    public ResponseEntity<CommandDispatcher.PendingCommandView> issue(
            @PathVariable String site,
            @PathVariable String robotId,
            @RequestBody CommandBody body) {

        Duration ttl = body.ttlSeconds() == null ? null : Duration.ofSeconds(body.ttlSeconds());
        var request = dispatcher.dispatch(site, robotId, body.type(), body.parameters(), ttl);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(dispatcher.find(request.commandId()));
    }

    @GetMapping("/commands")
    public List<CommandDispatcher.PendingCommandView> commands() {
        return dispatcher.recent();
    }

    @GetMapping("/commands/{commandId}")
    public ResponseEntity<CommandDispatcher.PendingCommandView> command(@PathVariable String commandId) {
        var view = dispatcher.find(commandId);
        return view == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(view);
    }

    /**
     * @param ttlSeconds how long the command stays valid while queued for an offline site. Omit for
     *                   the configured default.
     */
    public record CommandBody(CommandType type, Map<String, String> parameters, Long ttlSeconds) {}
}
