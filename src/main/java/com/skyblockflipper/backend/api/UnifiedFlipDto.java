package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import com.skyblockflipper.backend.model.Flipping.Enums.DurationType;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Enums.SchedulingPolicy;
import com.skyblockflipper.backend.model.Flipping.Enums.StepResource;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record UnifiedFlipDto(
        UUID id,
        FlipType flipType,
        List<ItemStackDto> inputItems,
        List<ItemStackDto> outputItems,
        Long requiredCapital,
        Long expectedProfit,
        Double roi,
        Double roiPerHour,
        Long durationSeconds,
        Long fees,
        Double liquidityScore,
        Double riskScore,
        Instant snapshotTimestamp,
        boolean partial,
        List<String> partialReasons,
        List<StepDto> steps,
        List<ConstraintDto> constraints
) {
    public UnifiedFlipDto {
        partialReasons = partialReasons == null ? List.of() : List.copyOf(partialReasons);
    }

    public record ItemStackDto(
            String itemId,
            int amount
    ) {
    }

    public record StepDto(
            StepType type,
            DurationType durationType,
            Long baseDurationSeconds,
            Double durationFactor,
            StepResource resource,
            int resourceUnits,
            SchedulingPolicy schedulingPolicy,
            String paramsJson
    ) {
    }

    public record ConstraintDto(
            ConstraintType type,
            String stringValue,
            Integer intValue,
            Long longValue
    ) {
    }
}
