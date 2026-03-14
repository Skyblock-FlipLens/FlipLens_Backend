package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.RetainedMarketSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface RetainedMarketSnapshotRepository extends JpaRepository<RetainedMarketSnapshotEntity, UUID> {

    Optional<RetainedMarketSnapshotEntity> findTopByOrderBySnapshotTimestampEpochMillisDesc();

    Optional<RetainedMarketSnapshotEntity> findTopBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisDesc(
            long snapshotTimestampEpochMillis
    );

    List<RetainedMarketSnapshotEntity> findBySnapshotTimestampEpochMillisBetweenOrderBySnapshotTimestampEpochMillisAsc(
            long startInclusiveEpochMillis,
            long endInclusiveEpochMillis
    );

    List<RetainedMarketSnapshotEntity> findBySnapshotTimestampEpochMillisIn(Collection<Long> snapshotTimestampEpochMillis);
}
