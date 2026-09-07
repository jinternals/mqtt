package com.jinternals.mqtt.cloud.protocol;

/** Robot liveness as seen from the cloud. */
public enum HealthStatus {
    /** Heartbeat received recently and self-reported checks are passing. */
    UP,
    /** Reachable, but the robot is reporting a degraded subsystem. */
    DEGRADED,
    /**
     * Published by the broker as a Last Will and Testament, not by the robot. If we ever receive
     * this we know the robot's session ended without a clean disconnect.
     */
    DOWN
}
