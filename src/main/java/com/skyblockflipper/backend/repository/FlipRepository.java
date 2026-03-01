package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.repository.query.Param;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface FlipRepository extends JpaRepository<Flip, UUID> {
    Page<Flip> findAllByFlipType(FlipType flipType, Pageable pageable);
    List<Flip> findAllByFlipType(FlipType flipType);

    Page<Flip> findAllBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis, Pageable pageable);

    List<Flip> findAllBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    Page<Flip> findAllByFlipTypeAndSnapshotTimestampEpochMillis(FlipType flipType,
                                                                long snapshotTimestampEpochMillis,
                                                                Pageable pageable);

    boolean existsBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    void deleteBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    @Query("select max(f.snapshotTimestampEpochMillis) from Flip f where f.snapshotTimestampEpochMillis is not null")
    Optional<Long> findMaxSnapshotTimestampEpochMillis();

    List<Flip> findByFlipTypeAndSnapshotTimestampEpochMillis(FlipType flipType, long snapshotTimestampEpochMillis);

    @Query("select f.flipType, count(f) from Flip f where f.snapshotTimestampEpochMillis = :snapshotEpochMillis group by f.flipType")
    List<Object[]> countByFlipTypeForSnapshot(@Param("snapshotEpochMillis") long snapshotEpochMillis);

    @Query("select f.id from Flip f")
    Page<UUID> findAllIds(Pageable pageable);

    @Query("select f.id from Flip f where f.flipType = :flipType")
    Page<UUID> findIdsByFlipType(@Param("flipType") FlipType flipType, Pageable pageable);

    @Query("select f.id from Flip f where f.snapshotTimestampEpochMillis = :snapshotTimestampEpochMillis")
    Page<UUID> findIdsBySnapshotTimestampEpochMillis(@Param("snapshotTimestampEpochMillis") long snapshotTimestampEpochMillis,
                                                     Pageable pageable);

    @Query("""
            select f.id from Flip f
            where f.flipType = :flipType
              and f.snapshotTimestampEpochMillis = :snapshotTimestampEpochMillis
            """)
    Page<UUID> findIdsByFlipTypeAndSnapshotTimestampEpochMillis(@Param("flipType") FlipType flipType,
                                                                @Param("snapshotTimestampEpochMillis") long snapshotTimestampEpochMillis,
                                                                Pageable pageable);

    @Query("""
            select distinct f from Flip f
            left join fetch f.steps
            left join fetch f.constraints
            where f.id in :ids
            """)
    List<Flip> findAllByIdInWithDetails(@Param("ids") Collection<UUID> ids);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query(value = """
            delete from flip_step
            where flip_id in (:flipIds)
            """, nativeQuery = true)
    int deleteStepRowsByFlipIdIn(@Param("flipIds") Collection<UUID> flipIds);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query(value = """
            delete from flip_constraints
            where flip_id in (:flipIds)
            """, nativeQuery = true)
    int deleteConstraintRowsByFlipIdIn(@Param("flipIds") Collection<UUID> flipIds);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            delete from Flip f
            where f.id in :flipIds
            """)
    int deleteByIdIn(@Param("flipIds") Collection<UUID> flipIds);

    @Query(value = """
            select f.id
            from flip f
            where f.snapshot_timestamp_epoch_millis in (:timestamps)
              and not exists (
                    select 1
                    from market_snapshot ms
                    where ms.snapshot_timestamp_epoch_millis = f.snapshot_timestamp_epoch_millis
              )
            limit :limit
            """, nativeQuery = true)
    List<UUID> findOrphanFlipIdsBySnapshotTimestampEpochMillisIn(@Param("timestamps") Collection<Long> timestamps,
                                                                  @Param("limit") int limit);
}
