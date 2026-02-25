package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.flippingstorage.FlipTrendSegmentEntity;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;

public interface FlipTrendSegmentRepository extends JpaRepository<FlipTrendSegmentEntity, Long> {

    @Query("""
            SELECT f
            FROM FlipTrendSegmentEntity f
            WHERE f.flipKey IN :flipKeys
              AND f.validToSnapshotEpochMillis = (
                    SELECT MAX(innerSeg.validToSnapshotEpochMillis)
                    FROM FlipTrendSegmentEntity innerSeg
                    WHERE innerSeg.flipKey = f.flipKey
              )
            ORDER BY f.flipKey ASC
            """)
    List<FlipTrendSegmentEntity> findLatestByFlipKeyIn(@Param("flipKeys") Collection<String> flipKeys);

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM FlipTrendSegmentEntity f
            WHERE f.validFromSnapshotEpochMillis = :validFrom
              AND f.validToSnapshotEpochMillis = :validTo
            """)
    int deleteByValidityWindow(@Param("validFrom") long validFromSnapshotEpochMillis,
                               @Param("validTo") long validToSnapshotEpochMillis);
}
