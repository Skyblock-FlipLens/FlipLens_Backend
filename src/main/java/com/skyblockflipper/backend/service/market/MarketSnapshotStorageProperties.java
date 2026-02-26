package com.skyblockflipper.backend.service.market;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "config.snapshot.storage")
public class MarketSnapshotStorageProperties {

    private boolean persistRawMarketSnapshot = true;
    private boolean persistAhAggregates = true;
    private boolean persistBzAggregates = true;

    public static MarketSnapshotStorageProperties rawOnlyDefaults() {
        MarketSnapshotStorageProperties properties = new MarketSnapshotStorageProperties();
        properties.setPersistRawMarketSnapshot(true);
        properties.setPersistAhAggregates(false);
        properties.setPersistBzAggregates(false);
        return properties;
    }

    public boolean isPersistRawMarketSnapshot() {
        return persistRawMarketSnapshot;
    }

    public void setPersistRawMarketSnapshot(boolean persistRawMarketSnapshot) {
        this.persistRawMarketSnapshot = persistRawMarketSnapshot;
    }

    public boolean isPersistAhAggregates() {
        return persistAhAggregates;
    }

    public void setPersistAhAggregates(boolean persistAhAggregates) {
        this.persistAhAggregates = persistAhAggregates;
    }

    public boolean isPersistBzAggregates() {
        return persistBzAggregates;
    }

    public void setPersistBzAggregates(boolean persistBzAggregates) {
        this.persistBzAggregates = persistBzAggregates;
    }
}
