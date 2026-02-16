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
    private final FlipCalculationContextService flipCalculationContextService;

    public FlipReadService(FlipRepository flipRepository,
                           UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                           FlipCalculationContextService flipCalculationContextService) {
        this.flipRepository = flipRepository;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
    }

    public Page<UnifiedFlipDto> listFlips(FlipType flipType, Pageable pageable) {
        FlipCalculationContext context = flipCalculationContextService.loadCurrentContext();
        Page<Flip> flips = flipType == null
                ? flipRepository.findAll(pageable)
                : flipRepository.findAllByFlipType(flipType, pageable);
        return flips.map(flip -> unifiedFlipDtoMapper.toDto(flip, context));
    }

    public Optional<UnifiedFlipDto> findFlipById(UUID id) {
        FlipCalculationContext context = flipCalculationContextService.loadCurrentContext();
        return flipRepository.findById(id).map(flip -> unifiedFlipDtoMapper.toDto(flip, context));
    }
}
