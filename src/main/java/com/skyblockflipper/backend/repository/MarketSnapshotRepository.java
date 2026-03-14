package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface MarketSnapshotRepository extends JpaRepository<MarketSnapshotEntity, UUID> {

    Optional<MarketSnapshotEntity> findTopByOrderBySnapshotTimestampEpochMillisDesc();

    Optional<MarketSnapshotEntity> findTopBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisDesc(long snapshotTimestampEpochMillis);

    Optional<MarketSnapshotEntity> findTopBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThanOrderBySnapshotTimestampEpochMillisAsc(
            long fromInclusive,
            long toExclusive
    );

    List<MarketSnapshotEntity> findBySnapshotTimestampEpochMillisBetweenOrderBySnapshotTimestampEpochMillisAsc(
            long startInclusiveEpochMillis,
            long endInclusiveEpochMillis
    );

    List<MarketSnapshotEntity> findBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisAsc(
            long snapshotTimestampEpochMillis
    );

    long countBySnapshotTimestampEpochMillisGreaterThanEqualAndSnapshotTimestampEpochMillisLessThan(
            long fromInclusive,
            long toExclusive
    );

    @Query("""
            select m.id as id, m.snapshotTimestampEpochMillis as snapshotTimestampEpochMillis
            from MarketSnapshotEntity m
            where m.snapshotTimestampEpochMillis <= :snapshotTimestampEpochMillis
            order by m.snapshotTimestampEpochMillis asc
            """)
    List<MarketSnapshotCompactionCandidate> findCompactionCandidates(
            @Param("snapshotTimestampEpochMillis") long snapshotTimestampEpochMillis
    );
}
