package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface MarketSnapshotRepository extends JpaRepository<MarketSnapshotEntity, UUID> {

    Optional<MarketSnapshotEntity> findTopByOrderBySnapshotTimestampEpochMillisDesc();

    Optional<MarketSnapshotEntity> findTopBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisDesc(long snapshotTimestampEpochMillis);
}
