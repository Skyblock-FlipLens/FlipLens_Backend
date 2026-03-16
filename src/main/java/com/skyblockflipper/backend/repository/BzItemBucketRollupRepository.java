package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.BzItemBucketRollupEntity;
import com.skyblockflipper.backend.model.market.BzItemBucketRollupId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

public interface BzItemBucketRollupRepository extends JpaRepository<BzItemBucketRollupEntity, BzItemBucketRollupId> {

    @Transactional
    long deleteByBucketStartEpochMillisAndBucketGranularity(long bucketStartEpochMillis, String bucketGranularity);
}
