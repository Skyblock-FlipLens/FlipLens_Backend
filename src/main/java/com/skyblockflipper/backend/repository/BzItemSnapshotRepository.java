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

public interface BzItemSnapshotRepository extends JpaRepository<BzItemSnapshotEntity, BzItemSnapshotId>, BzItemSnapshotBatchRepository {

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
