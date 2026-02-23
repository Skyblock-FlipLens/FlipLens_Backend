package com.skyblockflipper.backend.service.flipping.storage;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "config.flip.storage")
public class FlipStorageProperties {

    private boolean dualWriteEnabled = true;
    private boolean readFromNew = false;
    private boolean legacyWriteEnabled = true;
    private double trendRelativeThreshold = 0.05D;
    private double trendScoreDeltaThreshold = 3.0D;
    private int paritySampleSize = 20;

    public boolean isDualWriteEnabled() {
        return dualWriteEnabled;
    }

    public void setDualWriteEnabled(boolean dualWriteEnabled) {
        this.dualWriteEnabled = dualWriteEnabled;
    }

    public boolean isReadFromNew() {
        return readFromNew;
    }

    public void setReadFromNew(boolean readFromNew) {
        this.readFromNew = readFromNew;
    }

    public boolean isLegacyWriteEnabled() {
        return legacyWriteEnabled;
    }

    public void setLegacyWriteEnabled(boolean legacyWriteEnabled) {
        this.legacyWriteEnabled = legacyWriteEnabled;
    }

    public double getTrendRelativeThreshold() {
        return trendRelativeThreshold;
    }

    public void setTrendRelativeThreshold(double trendRelativeThreshold) {
        this.trendRelativeThreshold = trendRelativeThreshold;
    }

    public double getTrendScoreDeltaThreshold() {
        return trendScoreDeltaThreshold;
    }

    public void setTrendScoreDeltaThreshold(double trendScoreDeltaThreshold) {
        this.trendScoreDeltaThreshold = trendScoreDeltaThreshold;
    }

    public int getParitySampleSize() {
        return paritySampleSize;
    }

    public void setParitySampleSize(int paritySampleSize) {
        this.paritySampleSize = paritySampleSize;
    }
}
