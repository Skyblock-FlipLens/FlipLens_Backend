package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.flippingstorage.FlipCurrentEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface FlipCurrentRepository extends JpaRepository<FlipCurrentEntity, String> {

    List<FlipCurrentEntity> findAllByFlipType(FlipType flipType);

    Page<FlipCurrentEntity> findAllByFlipType(FlipType flipType, Pageable pageable);

    Optional<FlipCurrentEntity> findByStableFlipId(UUID stableFlipId);

    boolean existsBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    void deleteBySnapshotTimestampEpochMillis(long snapshotTimestampEpochMillis);

    @Query("select max(fc.snapshotTimestampEpochMillis) from FlipCurrentEntity fc")
    Optional<Long> findMaxSnapshotTimestampEpochMillis();

    @Query("select fc.flipType as flipType, count(fc) as count from FlipCurrentEntity fc group by fc.flipType")
    List<FlipTypeCountProjection> countByFlipType();

    interface FlipTypeCountProjection {
        FlipType getFlipType();
        long getCount();
    }
}
