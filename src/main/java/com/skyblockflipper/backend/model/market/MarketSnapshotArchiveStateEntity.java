package com.skyblockflipper.backend.model.market;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(
        name = "market_snapshot_archive_state",
        indexes = {
                @Index(name = "idx_market_snapshot_archive_state_parent_day", columnList = "parent_table, partition_day_utc")
        }
)
public class MarketSnapshotArchiveStateEntity {

    @Id
    @Column(name = "source_partition", nullable = false, length = 255)
    private String sourcePartition;

    @Column(name = "parent_table", nullable = false, length = 255)
    private String parentTable;

    @Column(name = "partition_day_utc", nullable = false)
    private LocalDate partitionDayUtc;

    @Column(name = "raw_row_count", nullable = false)
    private long rawRowCount;

    @Column(name = "retained_snapshot_id")
    private UUID retainedSnapshotId;

    @Column(name = "retained_snapshot_timestamp_epoch_millis")
    private Long retainedSnapshotTimestampEpochMillis;

    @Column(name = "finalized", nullable = false)
    private boolean finalized;

    @Column(name = "failed", nullable = false)
    private boolean failed;

    @Column(name = "finalized_at_epoch_millis")
    private Long finalizedAtEpochMillis;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
