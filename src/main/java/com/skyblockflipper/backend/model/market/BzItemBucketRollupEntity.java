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
@IdClass(BzItemBucketRollupId.class)
@Table(
        name = "bz_item_bucket_rollup",
        indexes = {
                @Index(name = "idx_bz_item_bucket_rollup_product_bucket", columnList = "product_id, bucket_granularity, bucket_start_epoch_millis")
        }
)
public class BzItemBucketRollupEntity {

    @Id
    @Column(name = "bucket_start_epoch_millis", nullable = false)
    private long bucketStartEpochMillis;

    @Id
    @Column(name = "bucket_granularity", nullable = false, length = 16)
    private String bucketGranularity;

    @Id
    @Column(name = "product_id", nullable = false)
    private String productId;

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

    @Column(name = "median_buy_price")
    private Double medianBuyPrice;

    @Column(name = "median_sell_price")
    private Double medianSellPrice;

    @Column(name = "median_mid_price")
    private Double medianMidPrice;

    @Column(name = "median_spread")
    private Double medianSpread;

    @Column(name = "p10_mid_price")
    private Double p10MidPrice;

    @Column(name = "p25_mid_price")
    private Double p25MidPrice;

    @Column(name = "p75_mid_price")
    private Double p75MidPrice;

    @Column(name = "p90_mid_price")
    private Double p90MidPrice;

    @Column(name = "min_mid_price")
    private Double minMidPrice;

    @Column(name = "max_mid_price")
    private Double maxMidPrice;

    @Column(name = "winsorized_avg_mid_price")
    private Double winsorizedAvgMidPrice;

    @Column(name = "median_buy_volume")
    private Double medianBuyVolume;

    @Column(name = "median_sell_volume")
    private Double medianSellVolume;

    @Column(name = "min_liquidity")
    private Double minLiquidity;

    @Column(name = "max_liquidity")
    private Double maxLiquidity;

    @Column(name = "first_snapshot_ts")
    private Long firstSnapshotTs;

    @Column(name = "last_snapshot_ts")
    private Long lastSnapshotTs;

    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
