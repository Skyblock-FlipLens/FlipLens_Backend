package com.skyblockflipper.backend.model.market;

import java.io.Serializable;
import java.util.Objects;

public class AhItemSnapshotId implements Serializable {

    private long snapshotTs;
    private String itemKey;

    public AhItemSnapshotId() {
    }

    public AhItemSnapshotId(long snapshotTs, String itemKey) {
        this.snapshotTs = snapshotTs;
        this.itemKey = itemKey;
    }

    public long getSnapshotTs() {
        return snapshotTs;
    }

    public String getItemKey() {
        return itemKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AhItemSnapshotId that)) {
            return false;
        }
        return snapshotTs == that.snapshotTs && Objects.equals(itemKey, that.itemKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotTs, itemKey);
    }
}
