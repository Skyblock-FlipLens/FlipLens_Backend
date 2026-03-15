package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.MarketSnapshotArchiveStateEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;

public interface MarketSnapshotArchiveStateRepository extends JpaRepository<MarketSnapshotArchiveStateEntity, String> {

    MarketSnapshotArchiveStateEntity findBySourcePartition(String sourcePartition);

    long countByParentTableAndPartitionDayUtcAndFinalizedTrueAndFailedFalse(String parentTable, LocalDate partitionDayUtc);
}
