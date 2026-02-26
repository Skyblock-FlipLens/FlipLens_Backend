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

public interface AhItemSnapshotRepository extends JpaRepository<AhItemSnapshotEntity, AhItemSnapshotId> {

    boolean existsBySnapshotTs(long snapshotTs);

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
