package com.skyblockflipper.backend.model.market;

import java.io.Serializable;
import java.util.Objects;

public class BzItemBucketRollupId implements Serializable {

    private static final long serialVersionUID = 1L;

    private long bucketStartEpochMillis;
    private String bucketGranularity;
    private String productId;

    public BzItemBucketRollupId() {
    }

    public BzItemBucketRollupId(long bucketStartEpochMillis, String bucketGranularity, String productId) {
        this.bucketStartEpochMillis = bucketStartEpochMillis;
        this.bucketGranularity = bucketGranularity;
        this.productId = productId;
    }

    public long getBucketStartEpochMillis() {
        return bucketStartEpochMillis;
    }

    public String getBucketGranularity() {
        return bucketGranularity;
    }

    public String getProductId() {
        return productId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BzItemBucketRollupId that)) {
            return false;
        }
        return bucketStartEpochMillis == that.bucketStartEpochMillis
                && Objects.equals(bucketGranularity, that.bucketGranularity)
                && Objects.equals(productId, that.productId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketStartEpochMillis, bucketGranularity, productId);
    }
}
