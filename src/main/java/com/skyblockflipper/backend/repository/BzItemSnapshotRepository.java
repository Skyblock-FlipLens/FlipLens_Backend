package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.BzItemSnapshotId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;

public interface BzItemSnapshotRepository extends JpaRepository<BzItemSnapshotEntity, BzItemSnapshotId> {

    @Modifying
    @Transactional
    @Query(value = """
            insert into bz_item_snapshot (
                snapshot_ts,
                product_id,
                buy_price,
                sell_price,
                buy_volume,
                sell_volume,
                created_at_epoch_millis
            ) values (
                :snapshotTs,
                :productId,
                :buyPrice,
                :sellPrice,
                :buyVolume,
                :sellVolume,
                :createdAtEpochMillis
            )
            on conflict (snapshot_ts, product_id) do nothing
            """, nativeQuery = true)
    int insertIgnore(@Param("snapshotTs") long snapshotTs,
                     @Param("productId") String productId,
                     @Param("buyPrice") Double buyPrice,
                     @Param("sellPrice") Double sellPrice,
                     @Param("buyVolume") Long buyVolume,
                     @Param("sellVolume") Long sellVolume,
                     @Param("createdAtEpochMillis") long createdAtEpochMillis);

    List<BzItemSnapshotEntity> findByProductIdAndSnapshotTsBetweenOrderBySnapshotTsAsc(String productId,
                                                                                          long fromInclusive,
                                                                                        long toInclusive);

    List<BzItemSnapshotEntity> findBySnapshotTsBetweenAndProductIdInOrderBySnapshotTsAsc(long fromInclusive,
                                                                                           long toInclusive,
                                                                                           Collection<String> productIds);

    List<BzItemSnapshotEntity> findBySnapshotTsInAndProductIdInOrderBySnapshotTsAsc(Collection<Long> snapshotTs,
                                                                                     Collection<String> productIds);

    @Query(value = """
            select min(snapshot_ts)
            from bz_item_snapshot
            where snapshot_ts between :fromInclusive and :toInclusive
            group by (snapshot_ts / 86400000)
            order by min(snapshot_ts)
            """, nativeQuery = true)
    List<Long> findFirstSnapshotTsPerDayBetween(@Param("fromInclusive") long fromInclusive,
                                                @Param("toInclusive") long toInclusive);

    @Modifying
    @Transactional
    @Query(value = "delete from bz_item_snapshot where snapshot_ts < :cutoffEpochMillis", nativeQuery = true)
    int deleteOlderThan(@Param("cutoffEpochMillis") long cutoffEpochMillis);
}
