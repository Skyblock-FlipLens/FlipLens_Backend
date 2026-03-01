package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.dto.UnifiedFlipDto;

import java.util.UUID;

final class UnifiedFlipDtoIdMapper {

    private UnifiedFlipDtoIdMapper() {
    }

    static UnifiedFlipDto withId(UnifiedFlipDto dto, UUID id) {
        if (dto == null) {
            return null;
        }
        return new UnifiedFlipDto(
                id,
                dto.flipType(),
                dto.inputItems(),
                dto.outputItems(),
                dto.requiredCapital(),
                dto.expectedProfit(),
                dto.roi(),
                dto.roiPerHour(),
                dto.durationSeconds(),
                dto.fees(),
                dto.liquidityScore(),
                dto.riskScore(),
                dto.snapshotTimestamp(),
                dto.partial(),
                dto.partialReasons(),
                dto.steps(),
                dto.constraints()
        );
    }
}
