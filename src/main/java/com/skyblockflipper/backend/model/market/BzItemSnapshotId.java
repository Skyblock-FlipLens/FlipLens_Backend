package com.skyblockflipper.backend.model.market;

import java.io.Serializable;
import java.util.Objects;

public class BzItemSnapshotId implements Serializable {

    private long snapshotTs;
    private String productId;

    public BzItemSnapshotId() {
    }

    public BzItemSnapshotId(long snapshotTs, String productId) {
        this.snapshotTs = snapshotTs;
        this.productId = productId;
    }

    public long getSnapshotTs() {
        return snapshotTs;
    }

    public String getProductId() {
        return productId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BzItemSnapshotId that)) {
            return false;
        }
        return snapshotTs == that.snapshotTs && Objects.equals(productId, that.productId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotTs, productId);
    }
}
