package com.skyblockflipper.backend.service.market.rollup;

public enum MarketBucketGranularity {
    ONE_MINUTE("1m", 60_000L),
    TWO_HOURS("2h", 7_200_000L),
    ONE_DAY("1d", 86_400_000L);

    private final String code;
    private final long durationMillis;

    MarketBucketGranularity(String code, long durationMillis) {
        this.code = code;
        this.durationMillis = durationMillis;
    }

    public String code() {
        return code;
    }

    public long durationMillis() {
        return durationMillis;
    }

    public int expectedSampleCount(long cadenceMillis) {
        long safeCadence = Math.max(1L, cadenceMillis);
        return (int) Math.max(1L, durationMillis / safeCadence);
    }
}
