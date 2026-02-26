package com.skyblockflipper.backend.model.market;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;

@Getter
@Entity
@IdClass(AhItemSnapshotId.class)
@Table(
        name = "ah_item_snapshot",
        indexes = {
                @Index(name = "idx_ah_item_snapshot_item_ts", columnList = "item_key, snapshot_ts"),
                @Index(name = "idx_ah_item_snapshot_ts", columnList = "snapshot_ts")
        }
)
public class AhItemSnapshotEntity {

    @Id
    @Column(name = "snapshot_ts", nullable = false)
    private long snapshotTs;

    @Id
    @Column(name = "item_key", nullable = false)
    private String itemKey;

    @Column(name = "bin_lowest")
    private Long binLowest;

    @Column(name = "bin_lowest5_mean")
    private Long binLowest5Mean;

    @Column(name = "bin_p50")
    private Long binP50;

    @Column(name = "bin_p95")
    private Long binP95;

    @Column(name = "bin_count", nullable = false)
    private int binCount;

    @Column(name = "bid_p50")
    private Long bidP50;

    @Column(name = "ending_soon_count", nullable = false)
    private int endingSoonCount;

    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    protected AhItemSnapshotEntity() {
    }

    public AhItemSnapshotEntity(long snapshotTs,
                                String itemKey,
                                Long binLowest,
                                Long binLowest5Mean,
                                Long binP50,
                                Long binP95,
                                int binCount,
                                Long bidP50,
                                int endingSoonCount) {
        this.snapshotTs = snapshotTs;
        this.itemKey = itemKey;
        this.binLowest = binLowest;
        this.binLowest5Mean = binLowest5Mean;
        this.binP50 = binP50;
        this.binP95 = binP95;
        this.binCount = binCount;
        this.bidP50 = bidP50;
        this.endingSoonCount = endingSoonCount;
        this.createdAtEpochMillis = System.currentTimeMillis();
    }
}
