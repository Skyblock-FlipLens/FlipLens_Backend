package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Service
public class FlipReadService {

    private final FlipRepository flipRepository;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;

    public FlipReadService(FlipRepository flipRepository, UnifiedFlipDtoMapper unifiedFlipDtoMapper) {
        this.flipRepository = flipRepository;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
    }

    public Page<UnifiedFlipDto> listFlips(FlipType flipType, Pageable pageable) {
        Page<Flip> flips = flipType == null
                ? flipRepository.findAll(pageable)
                : flipRepository.findAllByFlipType(flipType, pageable);
        return flips.map(unifiedFlipDtoMapper::toDto);
    }

    public Optional<UnifiedFlipDto> findFlipById(UUID id) {
        return flipRepository.findById(id).map(unifiedFlipDtoMapper::toDto);
    }
}
