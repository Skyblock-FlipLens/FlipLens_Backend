package com.skyblockflipper.backend.model.market;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@IdClass(ItemBucketMaterializationStateId.class)
@Table(
        name = "item_bucket_materialization_state",
        indexes = {
                @Index(name = "idx_item_bucket_materialization_state_partition", columnList = "source_partition, market_type"),
                @Index(name = "idx_item_bucket_materialization_state_unfinalized", columnList = "finalized, failed, market_type, bucket_granularity, bucket_start_epoch_millis")
        }
)
public class ItemBucketMaterializationStateEntity {

    @Id
    @Column(name = "bucket_start_epoch_millis", nullable = false)
    private long bucketStartEpochMillis;

    @Id
    @Column(name = "bucket_granularity", nullable = false, length = 16)
    private String bucketGranularity;

    @Id
    @Column(name = "market_type", nullable = false, length = 8)
    private String marketType;

    @Column(name = "bucket_end_epoch_millis", nullable = false)
    private long bucketEndEpochMillis;

    @Column(name = "source_partition", nullable = false)
    private String sourcePartition;

    @Column(name = "finalized", nullable = false)
    private boolean finalized;

    @Column(name = "failed", nullable = false)
    private boolean failed;

    @Column(name = "raw_row_count", nullable = false)
    private long rawRowCount;

    @Column(name = "rollup_row_count", nullable = false)
    private long rollupRowCount;

    @Column(name = "anomaly_row_count", nullable = false)
    private long anomalyRowCount;

    @Column(name = "finalized_at_epoch_millis")
    private Long finalizedAtEpochMillis;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
