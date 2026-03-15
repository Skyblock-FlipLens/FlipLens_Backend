package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.RetainedMarketSnapshotEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.util.UUID;

public interface MarketSnapshotHistoryRepository extends Repository<RetainedMarketSnapshotEntity, UUID> {

    @Query(value = """
            select
                cast(snapshots.id as varchar) as idText,
                snapshots.snapshot_timestamp_epoch_millis as snapshotTimestampEpochMillis,
                snapshots.auction_count as auctionCount,
                snapshots.bazaar_product_count as bazaarProductCount,
                snapshots.created_at_epoch_millis as createdAtEpochMillis
            from (
                select
                    m.id,
                    m.snapshot_timestamp_epoch_millis,
                    m.auction_count,
                    m.bazaar_product_count,
                    m.created_at_epoch_millis
                from market_snapshot m

                union all

                select
                    r.id,
                    r.snapshot_timestamp_epoch_millis,
                    r.auction_count,
                    r.bazaar_product_count,
                    r.created_at_epoch_millis
                from market_snapshot_retained r
                where not exists (
                    select 1
                    from market_snapshot m
                    where m.snapshot_timestamp_epoch_millis = r.snapshot_timestamp_epoch_millis
                )
            ) snapshots
            order by snapshots.snapshot_timestamp_epoch_millis desc
            fetch first 1 rows only
            """,
            nativeQuery = true)
    MarketSnapshotSummaryProjection findLatestCombinedSnapshotSummary();

    @Query(value = """
            select
                cast(snapshots.id as varchar) as idText,
                snapshots.snapshot_timestamp_epoch_millis as snapshotTimestampEpochMillis,
                snapshots.auction_count as auctionCount,
                snapshots.bazaar_product_count as bazaarProductCount,
                snapshots.created_at_epoch_millis as createdAtEpochMillis
            from (
                select
                    m.id,
                    m.snapshot_timestamp_epoch_millis,
                    m.auction_count,
                    m.bazaar_product_count,
                    m.created_at_epoch_millis
                from market_snapshot m

                union all

                select
                    r.id,
                    r.snapshot_timestamp_epoch_millis,
                    r.auction_count,
                    r.bazaar_product_count,
                    r.created_at_epoch_millis
                from market_snapshot_retained r
                where not exists (
                    select 1
                    from market_snapshot m
                    where m.snapshot_timestamp_epoch_millis = r.snapshot_timestamp_epoch_millis
                )
            ) snapshots
            order by snapshots.snapshot_timestamp_epoch_millis desc
            """,
            countQuery = """
            select count(*)
            from (
                select m.snapshot_timestamp_epoch_millis
                from market_snapshot m

                union all

                select r.snapshot_timestamp_epoch_millis
                from market_snapshot_retained r
                where not exists (
                    select 1
                    from market_snapshot m
                    where m.snapshot_timestamp_epoch_millis = r.snapshot_timestamp_epoch_millis
                )
            ) snapshots
            """,
            nativeQuery = true)
    Page<MarketSnapshotSummaryProjection> findCombinedSnapshotSummaries(Pageable pageable);

    interface MarketSnapshotSummaryProjection {
        String getIdText();

        long getSnapshotTimestampEpochMillis();

        int getAuctionCount();

        int getBazaarProductCount();

        long getCreatedAtEpochMillis();
    }
}
