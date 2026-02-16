package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.UUID;

public interface FlipRepository extends JpaRepository<Flip, UUID> {
    Page<Flip> findAllByFlipType(FlipType flipType, Pageable pageable);
}
