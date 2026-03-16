package com.skyblockflipper.backend.model.market;

import java.io.Serializable;
import java.util.Objects;

public class AhItemBucketRollupId implements Serializable {

    private static final long serialVersionUID = 1L;

    private long bucketStartEpochMillis;
    private String bucketGranularity;
    private String itemKey;

    public AhItemBucketRollupId() {
    }

    public AhItemBucketRollupId(long bucketStartEpochMillis, String bucketGranularity, String itemKey) {
        this.bucketStartEpochMillis = bucketStartEpochMillis;
        this.bucketGranularity = bucketGranularity;
        this.itemKey = itemKey;
    }

    public long getBucketStartEpochMillis() {
        return bucketStartEpochMillis;
    }

    public String getBucketGranularity() {
        return bucketGranularity;
    }

    public String getItemKey() {
        return itemKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AhItemBucketRollupId that)) {
            return false;
        }
        return bucketStartEpochMillis == that.bucketStartEpochMillis
                && Objects.equals(bucketGranularity, that.bucketGranularity)
                && Objects.equals(itemKey, that.itemKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketStartEpochMillis, bucketGranularity, itemKey);
    }
}
