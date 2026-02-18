package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;
import java.util.UUID;

public interface FlipRepository extends JpaRepository<Flip, UUID> {
    Page<Flip> findAllByFlipType(FlipType flipType, Pageable pageable);

    Page<Flip> findAllBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis, Pageable pageable);

    Page<Flip> findAllByFlipTypeAndSnapshotTimestampEpochMillis(FlipType flipType,
                                                                long snapshotTimestampEpochMillis,
                                                                Pageable pageable);

    boolean existsBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    void deleteBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    @Query("select max(f.snapshotTimestampEpochMillis) from Flip f where f.snapshotTimestampEpochMillis is not null")
    Optional<Long> findMaxSnapshotTimestampEpochMillis();
}
