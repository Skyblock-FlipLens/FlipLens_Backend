package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.AhItemAnomalySegmentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

public interface AhItemAnomalySegmentRepository extends JpaRepository<AhItemAnomalySegmentEntity, Long> {

    @Transactional
    long deleteByBucketStartEpochMillisAndBucketGranularity(long bucketStartEpochMillis, String bucketGranularity);
}
