package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;

import java.util.function.Consumer;
import java.util.List;

public interface AhItemSnapshotBatchRepository {

    int[] insertIgnoreBatch(List<AhItemSnapshotEntity> snapshots);

    void scanBucketRows(long fromInclusive, long toExclusive, int fetchSize, Consumer<AhItemSnapshotEntity> consumer);
}
