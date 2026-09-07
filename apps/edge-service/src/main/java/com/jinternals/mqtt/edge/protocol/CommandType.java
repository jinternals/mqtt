package com.jinternals.mqtt.edge.protocol;

/** The commands a picking cell knows how to run. */
public enum CommandType {

    /**
     * Pick one item. Parameters: {@code sku}, {@code sourceBin}, {@code targetTote}.
     *
     * <p>This is the command that makes expiry matter. A pick request that queued behind a dead
     * uplink for three hours refers to an order that has almost certainly been rerouted, cancelled
     * or picked by hand — and the bin it names has been restocked with something else since. Running
     * it on arrival puts the wrong item in a tote that has already shipped.
     */
    PICK_ITEM,

    /** Stop taking new picks after the current one finishes. Safe to apply late. */
    PAUSE_PICKING,

    /**
     * Resume picking.
     *
     * <p>The genuinely dangerous one to execute late: a cell is usually paused because a person is
     * working in it. "Resume" delivered hours after it was issued, to a cell someone is standing
     * in, is how a store-and-forward system hurts somebody. It expires like everything else.
     */
    RESUME_PICKING,

    /** Change the telemetry publish cadence. Parameter: {@code intervalSeconds}. */
    SET_TELEMETRY_INTERVAL,

    /** Report health immediately, out of band from the normal heartbeat. */
    REPORT_HEALTH,

    /** Round-trip probe used to measure real end-to-end latency through the bridge. */
    PING
}
