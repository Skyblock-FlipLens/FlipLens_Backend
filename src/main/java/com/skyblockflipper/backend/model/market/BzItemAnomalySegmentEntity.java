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
        name = "bz_item_anomaly_segment",
        indexes = {
                @Index(name = "idx_bz_item_anomaly_segment_product_bucket", columnList = "product_id, bucket_granularity, segment_start_epoch_millis")
        }
)
public class BzItemAnomalySegmentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "bucket_start_epoch_millis", nullable = false)
    private long bucketStartEpochMillis;

    @Column(name = "bucket_end_epoch_millis", nullable = false)
    private long bucketEndEpochMillis;

    @Column(name = "bucket_granularity", nullable = false, length = 16)
    private String bucketGranularity;

    @Column(name = "product_id", nullable = false)
    private String productId;

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

    @Column(name = "median_buy_price")
    private Double medianBuyPrice;

    @Column(name = "median_sell_price")
    private Double medianSellPrice;

    @Column(name = "median_mid_price")
    private Double medianMidPrice;

    @Column(name = "median_spread")
    private Double medianSpread;

    @Column(name = "median_buy_volume")
    private Double medianBuyVolume;

    @Column(name = "median_sell_volume")
    private Double medianSellVolume;

    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
