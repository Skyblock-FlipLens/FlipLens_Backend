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
@IdClass(BzItemSnapshotId.class)
@Table(
        name = "bz_item_snapshot",
        indexes = {
                @Index(name = "idx_bz_item_snapshot_item_ts", columnList = "product_id, snapshot_ts"),
                @Index(name = "idx_bz_item_snapshot_ts", columnList = "snapshot_ts")
        }
)
public class BzItemSnapshotEntity {

    @Id
    @Column(name = "snapshot_ts", nullable = false)
    private long snapshotTs;

    @Id
    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(name = "buy_price")
    private Double buyPrice;

    @Column(name = "sell_price")
    private Double sellPrice;

    @Column(name = "buy_volume")
    private Long buyVolume;

    @Column(name = "sell_volume")
    private Long sellVolume;

    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    protected BzItemSnapshotEntity() {
    }

    public BzItemSnapshotEntity(long snapshotTs,
                                String productId,
                                Double buyPrice,
                                Double sellPrice,
                                Long buyVolume,
                                Long sellVolume) {
        this.snapshotTs = snapshotTs;
        this.productId = productId;
        this.buyPrice = buyPrice;
        this.sellPrice = sellPrice;
        this.buyVolume = buyVolume;
        this.sellVolume = sellVolume;
        this.createdAtEpochMillis = System.currentTimeMillis();
    }
}
