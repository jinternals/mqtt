package com.jinternals.mqtt.edge.robot;

import com.jinternals.mqtt.edge.config.EdgeProperties;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/** The picking cells this gateway speaks for. */
@Component
public class RobotRegistry {

    private final Map<String, RobotState> robots = new LinkedHashMap<>();

    public RobotRegistry(EdgeProperties props) {
        for (String id : props.getRobots()) {
            robots.put(id, new RobotState(id, props.getTelemetryInterval()));
        }
    }

    public Collection<RobotState> all() {
        return robots.values();
    }

    public RobotState get(String robotId) {
        return robots.get(robotId);
    }

    public boolean contains(String robotId) {
        return robots.containsKey(robotId);
    }
}
