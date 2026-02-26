package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;

import java.util.List;

public interface AhItemSnapshotBatchRepository {

    int[] insertIgnoreBatch(List<AhItemSnapshotEntity> snapshots);
}
