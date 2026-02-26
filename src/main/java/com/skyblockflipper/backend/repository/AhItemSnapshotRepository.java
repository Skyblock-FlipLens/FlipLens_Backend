package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.AhItemSnapshotId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;

public interface AhItemSnapshotRepository extends JpaRepository<AhItemSnapshotEntity, AhItemSnapshotId>, AhItemSnapshotBatchRepository {

    @Modifying
    @Transactional
    @Query(value = """
            insert into ah_item_snapshot (
                snapshot_ts,
                item_key,
                bin_lowest,
                bin_lowest5_mean,
                bin_p50,
                bin_p95,
                bin_count,
                bid_p50,
                ending_soon_count,
                created_at_epoch_millis
            ) values (
                :snapshotTs,
                :itemKey,
                :binLowest,
                :binLowest5Mean,
                :binP50,
                :binP95,
                :binCount,
                :bidP50,
                :endingSoonCount,
                :createdAtEpochMillis
            )
            on conflict (snapshot_ts, item_key) do nothing
            """, nativeQuery = true)
    int insertIgnore(@Param("snapshotTs") long snapshotTs,
                     @Param("itemKey") String itemKey,
                     @Param("binLowest") Long binLowest,
                     @Param("binLowest5Mean") Long binLowest5Mean,
                     @Param("binP50") Long binP50,
                     @Param("binP95") Long binP95,
                     @Param("binCount") int binCount,
                     @Param("bidP50") Long bidP50,
                     @Param("endingSoonCount") int endingSoonCount,
                     @Param("createdAtEpochMillis") long createdAtEpochMillis);

    List<AhItemSnapshotEntity> findByItemKeyAndSnapshotTsBetweenOrderBySnapshotTsAsc(String itemKey,
                                                                                       long fromInclusive,
                                                                                      long toInclusive);

    List<AhItemSnapshotEntity> findBySnapshotTsBetweenAndItemKeyInOrderBySnapshotTsAsc(long fromInclusive,
                                                                                         long toInclusive,
                                                                                         Collection<String> itemKeys);

    List<AhItemSnapshotEntity> findBySnapshotTsInAndItemKeyInOrderBySnapshotTsAsc(Collection<Long> snapshotTs,
                                                                                   Collection<String> itemKeys);

    @Query(value = """
            select min(snapshot_ts)
            from ah_item_snapshot
            where snapshot_ts between :fromInclusive and :toInclusive
            group by (snapshot_ts / 86400000)
            order by min(snapshot_ts)
            """, nativeQuery = true)
    List<Long> findFirstSnapshotTsPerDayBetween(@Param("fromInclusive") long fromInclusive,
                                                @Param("toInclusive") long toInclusive);

    @Modifying
    @Transactional
    @Query(value = "delete from ah_item_snapshot where snapshot_ts < :cutoffEpochMillis", nativeQuery = true)
    int deleteOlderThan(@Param("cutoffEpochMillis") long cutoffEpochMillis);
}
