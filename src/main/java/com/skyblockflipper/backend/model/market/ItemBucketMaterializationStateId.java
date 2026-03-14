package com.skyblockflipper.backend.model.market;

import java.io.Serializable;
import java.util.Objects;

public class ItemBucketMaterializationStateId implements Serializable {

    private static final long serialVersionUID = 1L;

    private long bucketStartEpochMillis;
    private String bucketGranularity;
    private String marketType;

    public ItemBucketMaterializationStateId() {
    }

    public ItemBucketMaterializationStateId(long bucketStartEpochMillis, String bucketGranularity, String marketType) {
        this.bucketStartEpochMillis = bucketStartEpochMillis;
        this.bucketGranularity = bucketGranularity;
        this.marketType = marketType;
    }

    public long getBucketStartEpochMillis() {
        return bucketStartEpochMillis;
    }

    public String getBucketGranularity() {
        return bucketGranularity;
    }

    public String getMarketType() {
        return marketType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ItemBucketMaterializationStateId that)) {
            return false;
        }
        return bucketStartEpochMillis == that.bucketStartEpochMillis
                && Objects.equals(bucketGranularity, that.bucketGranularity)
                && Objects.equals(marketType, that.marketType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketStartEpochMillis, bucketGranularity, marketType);
    }
}
