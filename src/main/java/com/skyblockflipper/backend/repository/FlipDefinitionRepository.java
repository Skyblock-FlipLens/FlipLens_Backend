package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.flippingstorage.FlipDefinitionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface FlipDefinitionRepository extends JpaRepository<FlipDefinitionEntity, String> {

    Optional<FlipDefinitionEntity> findByStableFlipId(UUID stableFlipId);
}
