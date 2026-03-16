package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;

import java.util.function.Consumer;
import java.util.List;

public interface BzItemSnapshotBatchRepository {

    int[] insertIgnoreBatch(List<BzItemSnapshotEntity> snapshots);

    void scanBucketRows(long fromInclusive, long toExclusive, int fetchSize, Consumer<BzItemSnapshotEntity> consumer);
}
