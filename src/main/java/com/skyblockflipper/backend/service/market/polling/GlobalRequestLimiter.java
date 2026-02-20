package com.skyblockflipper.backend.service.market.polling;

public class GlobalRequestLimiter {

    private final long intervalNanos;
    private long nextAllowedNano;

    public GlobalRequestLimiter(double requestsPerSecond) {
        double requestsPerSecond1 = Math.max(0.1d, requestsPerSecond);
        this.intervalNanos = Math.max(1L, Math.round(1_000_000_000d / requestsPerSecond1));
    }

    public synchronized long reserveDelayMillis() {
        long now = System.nanoTime();
        long earliest = Math.max(now, nextAllowedNano);
        nextAllowedNano = earliest + intervalNanos;
        long delayNanos = Math.max(0L, earliest - now);
        return Math.round(delayNanos / 1_000_000d);
    }
}
