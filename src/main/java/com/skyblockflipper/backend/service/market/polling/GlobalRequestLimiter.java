package com.skyblockflipper.backend.service.market.polling;

public class GlobalRequestLimiter {

    private final long intervalNanos;
    private long nextAllowedNano;

    public GlobalRequestLimiter(double requestsPerSecond) {
        double requestsPerSecond1 = Math.max(0.1d, requestsPerSecond);
        this.intervalNanos = Math.max(1L, Math.round(1_000_000_000d / requestsPerSecond1));
        this.nextAllowedNano = System.nanoTime() - this.intervalNanos;
    }

    public synchronized long reserveDelayMillis() {
        long now = System.nanoTime();
        if (now < nextAllowedNano) {
            long delayNanos = nextAllowedNano - now;
            return nanosToMillisCeil(delayNanos);
        }
        nextAllowedNano = now + intervalNanos;
        return 0L;
    }

    private long nanosToMillisCeil(long nanos) {
        if (nanos <= 0L) {
            return 0L;
        }
        return (nanos + 999_999L) / 1_000_000L;
    }
}
