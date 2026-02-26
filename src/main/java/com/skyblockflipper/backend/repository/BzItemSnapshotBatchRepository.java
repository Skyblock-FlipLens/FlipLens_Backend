package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;

import java.util.List;

public interface BzItemSnapshotBatchRepository {

    int[] insertIgnoreBatch(List<BzItemSnapshotEntity> snapshots);
}
