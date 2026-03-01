package com.skyblockflipper.backend.service.market.polling;

public class BazaarPollState {

    public enum Phase {
        SLEEP,
        PROBE,
        COMMIT_IN_FLIGHT
    }

    private long lastSeenLastUpdated = -1L;
    private long ewmaPeriodMs = 60_000L;
    private long nextExpectedAtMs = -1L;
    private Phase phase = Phase.SLEEP;
    private long probeIntervalMs = 1_000L;
    private int probeBackoffStep = 0;

    public synchronized long getLastSeenLastUpdated() {
        return lastSeenLastUpdated;
    }

    public synchronized void setLastSeenLastUpdated(long lastSeenLastUpdated) {
        this.lastSeenLastUpdated = lastSeenLastUpdated;
    }

    public synchronized long getEwmaPeriodMs() {
        return ewmaPeriodMs;
    }

    public synchronized void setEwmaPeriodMs(long ewmaPeriodMs) {
        this.ewmaPeriodMs = ewmaPeriodMs;
    }

    public synchronized long getNextExpectedAtMs() {
        return nextExpectedAtMs;
    }

    public synchronized void setNextExpectedAtMs(long nextExpectedAtMs) {
        this.nextExpectedAtMs = nextExpectedAtMs;
    }

    public synchronized Phase getPhase() {
        return phase;
    }

    public synchronized void setPhase(Phase phase) {
        this.phase = phase;
    }

    public synchronized long getProbeIntervalMs() {
        return probeIntervalMs;
    }

    public synchronized void setProbeIntervalMs(long probeIntervalMs) {
        this.probeIntervalMs = probeIntervalMs;
    }

    public synchronized int getProbeBackoffStep() {
        return probeBackoffStep;
    }

    public synchronized void setProbeBackoffStep(int probeBackoffStep) {
        this.probeBackoffStep = probeBackoffStep;
    }
}
