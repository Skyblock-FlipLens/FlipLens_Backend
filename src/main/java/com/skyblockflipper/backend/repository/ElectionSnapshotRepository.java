package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.ElectionSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface ElectionSnapshotRepository extends JpaRepository<ElectionSnapshot, UUID> {

    Optional<ElectionSnapshot> findFirstByFetchedAtLessThanEqualOrderByFetchedAtDesc(Instant asOfTimestamp);
}
