package com.skyblockflipper.backend.service.market;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "config.snapshot.retention")
public class SnapshotRetentionProperties {

    private long rawWindowSeconds = 90L;
    private long minuteTierUpperSeconds = 30L * 60L;
    private long twoHourTierUpperSeconds = 12L * 60L * 60L;
    private long minuteIntervalSeconds = 60L;
    private long twoHourIntervalSeconds = 2L * 60L * 60L;

    public long getRawWindowSeconds() {
        return rawWindowSeconds;
    }

    public void setRawWindowSeconds(long rawWindowSeconds) {
        this.rawWindowSeconds = rawWindowSeconds;
    }

    public long getMinuteTierUpperSeconds() {
        return minuteTierUpperSeconds;
    }

    public void setMinuteTierUpperSeconds(long minuteTierUpperSeconds) {
        this.minuteTierUpperSeconds = minuteTierUpperSeconds;
    }

    public long getTwoHourTierUpperSeconds() {
        return twoHourTierUpperSeconds;
    }

    public void setTwoHourTierUpperSeconds(long twoHourTierUpperSeconds) {
        this.twoHourTierUpperSeconds = twoHourTierUpperSeconds;
    }

    public long getMinuteIntervalSeconds() {
        return minuteIntervalSeconds;
    }

    public void setMinuteIntervalSeconds(long minuteIntervalSeconds) {
        this.minuteIntervalSeconds = minuteIntervalSeconds;
    }

    public long getTwoHourIntervalSeconds() {
        return twoHourIntervalSeconds;
    }

    public void setTwoHourIntervalSeconds(long twoHourIntervalSeconds) {
        this.twoHourIntervalSeconds = twoHourIntervalSeconds;
    }
}
