package com.jinternals.mqtt.edge.robot;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stands in for a real picking cell — a robotic arm or a humanoid working a pick face.
 *
 * <p>Holds the bits the rest of the service reasons about: a monotonic telemetry sequence, a
 * runtime-adjustable publish cadence, and a small state machine. The state machine exists because
 * commands are not all unconditionally applicable: a {@code PICK_ITEM} that arrives while the cell
 * is paused must be refused rather than quietly executed.
 */
public class RobotState {

    /** Where the cell is right now. Reported in health, and checked before executing a pick. */
    public enum Mode {
        /** Powered and ready, nothing in progress. */
        IDLE,
        /** Actively working picks. */
        PICKING,
        /** Deliberately stopped — usually because a person is in the cell. */
        PAUSED
    }

    private final String robotId;
    private final AtomicLong sequence = new AtomicLong(0);
    private final AtomicLong picksCompleted = new AtomicLong(0);
    private final AtomicLong graspFailures = new AtomicLong(0);

    private volatile Duration interval;
    private volatile long nextDueAtMillis;
    private volatile Mode mode = Mode.PICKING;

    private double jointTempC = 38 + ThreadLocalRandom.current().nextDouble(-3, 3);
    private double batteryPercent = 80 + ThreadLocalRandom.current().nextDouble(-10, 15);

    public RobotState(String robotId, Duration interval) {
        this.robotId = robotId;
        this.interval = interval;
        this.nextDueAtMillis = System.currentTimeMillis();
    }

    /**
     * One pick attempt. Returns whether the grasp succeeded — real picking is not 100%, and a
     * success rate that silently reads 1.0 is the sort of metric nobody trusts.
     */
    public synchronized boolean attemptPick() {
        boolean grasped = ThreadLocalRandom.current().nextDouble() > 0.04;
        if (grasped) {
            picksCompleted.incrementAndGet();
        } else {
            graspFailures.incrementAndGet();
        }
        // Working the arm heats the joints and drains the pack.
        jointTempC += ThreadLocalRandom.current().nextDouble(0.0, 0.3);
        batteryPercent = Math.max(5, batteryPercent - ThreadLocalRandom.current().nextDouble(0.0, 0.05));
        return grasped;
    }

    /** A telemetry sample. Random-walked so a drained backlog charts like real data. */
    public synchronized Map<String, Double> sample() {
        if (mode == Mode.PICKING) {
            jointTempC += ThreadLocalRandom.current().nextDouble(-0.2, 0.25);
            batteryPercent = Math.max(5, batteryPercent - ThreadLocalRandom.current().nextDouble(0, 0.08));
        } else {
            // Idle cells cool down and stop draining.
            jointTempC -= ThreadLocalRandom.current().nextDouble(0, 0.3);
        }

        long total = picksCompleted.get() + graspFailures.get();
        double successRate = total == 0 ? 1.0 : (double) picksCompleted.get() / total;

        Map<String, Double> metrics = new LinkedHashMap<>();
        metrics.put("picksPerMinute", mode == Mode.PICKING ? round(9 + ThreadLocalRandom.current().nextDouble(0, 5)) : 0.0);
        metrics.put("graspSuccessRate", round(successRate));
        metrics.put("cycleTimeMs", round(4200 + ThreadLocalRandom.current().nextDouble(-600, 900)));
        metrics.put("jointTempC", round(jointTempC));
        metrics.put("batteryPercent", round(batteryPercent));
        return metrics;
    }

    private static double round(double v) {
        return Math.round(v * 100d) / 100d;
    }

    public boolean isDue(long nowMillis) {
        return nowMillis >= nextDueAtMillis;
    }

    public void markPublished(long nowMillis) {
        nextDueAtMillis = nowMillis + interval.toMillis();
    }

    public long nextSequence() {
        return sequence.incrementAndGet();
    }

    public long messagesPublished() {
        return sequence.get();
    }

    public long picksCompleted() {
        return picksCompleted.get();
    }

    public String robotId() {
        return robotId;
    }

    public Duration interval() {
        return interval;
    }

    public void setInterval(Duration interval) {
        this.interval = interval;
        this.nextDueAtMillis = System.currentTimeMillis() + interval.toMillis();
    }

    public Mode mode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public double batteryPercent() {
        return batteryPercent;
    }
}
