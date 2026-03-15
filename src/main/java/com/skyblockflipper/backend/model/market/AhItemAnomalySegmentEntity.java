package com.skyblockflipper.backend.model.market;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(
        name = "ah_item_anomaly_segment",
        indexes = {
                @Index(name = "idx_ah_item_anomaly_segment_item_bucket", columnList = "item_key, bucket_granularity, segment_start_epoch_millis")
        }
)
public class AhItemAnomalySegmentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "bucket_start_epoch_millis", nullable = false)
    private long bucketStartEpochMillis;

    @Column(name = "bucket_end_epoch_millis", nullable = false)
    private long bucketEndEpochMillis;

    @Column(name = "bucket_granularity", nullable = false, length = 16)
    private String bucketGranularity;

    @Column(name = "item_key", nullable = false)
    private String itemKey;

    @Column(name = "segment_start_epoch_millis", nullable = false)
    private long segmentStartEpochMillis;

    @Column(name = "segment_end_epoch_millis", nullable = false)
    private long segmentEndEpochMillis;

    @Column(name = "representative_snapshot_ts")
    private Long representativeSnapshotTs;

    @Column(name = "peak_snapshot_ts")
    private Long peakSnapshotTs;

    @Column(name = "sample_count", nullable = false)
    private int sampleCount;

    @Column(name = "anomaly_score", nullable = false)
    private double anomalyScore;

    @Column(name = "reason_code", nullable = false, length = 64)
    private String reasonCode;

    @Column(name = "fragmented", nullable = false)
    private boolean fragmented;

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

    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
