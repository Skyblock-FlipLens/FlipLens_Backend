package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.BzItemAnomalySegmentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

public interface BzItemAnomalySegmentRepository extends JpaRepository<BzItemAnomalySegmentEntity, Long> {

    @Transactional
    long deleteByBucketStartEpochMillisAndBucketGranularity(long bucketStartEpochMillis, String bucketGranularity);
}
