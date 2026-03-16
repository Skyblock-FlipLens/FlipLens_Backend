package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.AhItemBucketRollupEntity;
import com.skyblockflipper.backend.model.market.AhItemBucketRollupId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

public interface AhItemBucketRollupRepository extends JpaRepository<AhItemBucketRollupEntity, AhItemBucketRollupId> {

    @Transactional
    long deleteByBucketStartEpochMillisAndBucketGranularity(long bucketStartEpochMillis, String bucketGranularity);
}
