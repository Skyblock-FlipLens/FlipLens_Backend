package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.flippingstorage.FlipDefinitionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface FlipDefinitionRepository extends JpaRepository<FlipDefinitionEntity, String> {

    Optional<FlipDefinitionEntity> findByStableFlipId(UUID stableFlipId);

    @Query(value = "select pg_advisory_xact_lock(:lockKey)", nativeQuery = true)
    Long acquireTransactionScopedWriteLock(@Param("lockKey") long lockKey);
}
