package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.flippingstorage.FlipTrendSegmentEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;

public interface FlipTrendSegmentRepository extends JpaRepository<FlipTrendSegmentEntity, Long> {

    List<FlipTrendSegmentEntity> findByFlipKeyInOrderByFlipKeyAscValidToSnapshotEpochMillisDesc(Collection<String> flipKeys);
}
