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
@IdClass(AhItemBucketRollupId.class)
@Table(
        name = "ah_item_bucket_rollup",
        indexes = {
                @Index(name = "idx_ah_item_bucket_rollup_item_bucket", columnList = "item_key, bucket_granularity, bucket_start_epoch_millis")
        }
)
public class AhItemBucketRollupEntity {

    @Id
    @Column(name = "bucket_start_epoch_millis", nullable = false)
    private long bucketStartEpochMillis;

    @Id
    @Column(name = "bucket_granularity", nullable = false, length = 16)
    private String bucketGranularity;

    @Id
    @Column(name = "item_key", nullable = false)
    private String itemKey;

    @Column(name = "bucket_end_epoch_millis", nullable = false)
    private long bucketEndEpochMillis;

    @Column(name = "sample_count", nullable = false)
    private int sampleCount;

    @Column(name = "valid_sample_count", nullable = false)
    private int validSampleCount;

    @Column(name = "anomaly_sample_count", nullable = false)
    private int anomalySampleCount;

    @Column(name = "partial", nullable = false)
    private boolean partial;

    @Column(name = "representative_snapshot_ts")
    private Long representativeSnapshotTs;

    @Column(name = "median_bin_lowest5_mean")
    private Double medianBinLowest5Mean;

    @Column(name = "median_bin_p50")
    private Double medianBinP50;

    @Column(name = "median_bin_p95")
    private Double medianBinP95;

    @Column(name = "median_bid_p50")
    private Double medianBidP50;

    @Column(name = "median_bin_count")
    private Double medianBinCount;

    @Column(name = "median_ending_soon_count")
    private Double medianEndingSoonCount;

    @Column(name = "p10_bin_p50")
    private Double p10BinP50;

    @Column(name = "p25_bin_p50")
    private Double p25BinP50;

    @Column(name = "p75_bin_p50")
    private Double p75BinP50;

    @Column(name = "p90_bin_p50")
    private Double p90BinP50;

    @Column(name = "min_bin_p50")
    private Double minBinP50;

    @Column(name = "max_bin_p50")
    private Double maxBinP50;

    @Column(name = "winsorized_avg_bin_p50")
    private Double winsorizedAvgBinP50;

    @Column(name = "first_snapshot_ts")
    private Long firstSnapshotTs;

    @Column(name = "last_snapshot_ts")
    private Long lastSnapshotTs;

    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
