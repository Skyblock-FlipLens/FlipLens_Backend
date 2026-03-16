package com.skyblockflipper.backend.model.market;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Entity
@Table(
        name = "market_snapshot_retained",
        indexes = {
                @Index(
                        name = "idx_market_snapshot_retained_snapshot_ts_epoch_millis",
                        columnList = "snapshot_timestamp_epoch_millis"
                )
        }
)
public class RetainedMarketSnapshotEntity {

    @Id
    private UUID id;

    @Setter
    @Column(name = "snapshot_timestamp_epoch_millis", nullable = false, unique = true)
    private long snapshotTimestampEpochMillis;

    @Setter
    @Column(name = "auction_count", nullable = false)
    private int auctionCount;

    @Setter
    @Column(name = "bazaar_product_count", nullable = false)
    private int bazaarProductCount;

    @Setter
    @Column(name = "auctions_json", nullable = false, columnDefinition = "text")
    private String auctionsJson;

    @Setter
    @Column(name = "bazaar_products_json", nullable = false, columnDefinition = "text")
    private String bazaarProductsJson;

    @Setter
    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    @Setter
    @Column(name = "retained_at_epoch_millis", nullable = false)
    private long retainedAtEpochMillis;

    protected RetainedMarketSnapshotEntity() {
    }

    public RetainedMarketSnapshotEntity(UUID id,
                                        long snapshotTimestampEpochMillis,
                                        int auctionCount,
                                        int bazaarProductCount,
                                        String auctionsJson,
                                        String bazaarProductsJson,
                                        long createdAtEpochMillis,
                                        long retainedAtEpochMillis) {
        this.id = id;
        this.snapshotTimestampEpochMillis = snapshotTimestampEpochMillis;
        this.auctionCount = auctionCount;
        this.bazaarProductCount = bazaarProductCount;
        this.auctionsJson = auctionsJson;
        this.bazaarProductsJson = bazaarProductsJson;
        this.createdAtEpochMillis = createdAtEpochMillis;
        this.retainedAtEpochMillis = retainedAtEpochMillis;
    }
}
