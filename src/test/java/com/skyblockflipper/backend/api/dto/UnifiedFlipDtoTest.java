package com.skyblockflipper.backend.api.dto;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnifiedFlipDtoTest {

    @Test
    void listFieldsUseDefensiveCopiesAndNullSafeDefaults() {
        List<UnifiedFlipDto.ItemStackDto> inputItems = new ArrayList<>();
        inputItems.add(new UnifiedFlipDto.ItemStackDto("ITEM_A", 1));
        List<UnifiedFlipDto.ItemStackDto> outputItems = new ArrayList<>();
        outputItems.add(new UnifiedFlipDto.ItemStackDto("ITEM_B", 2));
        List<String> reasons = new ArrayList<>();
        reasons.add("MISSING_MARKET_SNAPSHOT");
        List<UnifiedFlipDto.StepDto> steps = new ArrayList<>();
        steps.add(new UnifiedFlipDto.StepDto(null, null, 1L, null, null, 0, null, null));
        List<UnifiedFlipDto.ConstraintDto> constraints = new ArrayList<>();
        constraints.add(new UnifiedFlipDto.ConstraintDto(null, null, null, null));

        UnifiedFlipDto dto = new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.BAZAAR,
                inputItems,
                outputItems,
                100L,
                10L,
                0.1D,
                1.0D,
                3600L,
                1L,
                0.5D,
                0.5D,
                Instant.parse("2026-02-16T00:00:00Z"),
                true,
                reasons,
                steps,
                constraints
        );

        inputItems.add(new UnifiedFlipDto.ItemStackDto("MUTATED_INPUT", 3));
        outputItems.add(new UnifiedFlipDto.ItemStackDto("MUTATED_OUTPUT", 4));
        reasons.add("MUTATED_AFTER_CONSTRUCTION");
        steps.add(new UnifiedFlipDto.StepDto(null, null, 2L, null, null, 0, null, null));
        constraints.add(new UnifiedFlipDto.ConstraintDto(null, null, null, 1L));

        assertEquals(1, dto.inputItems().size());
        assertEquals(1, dto.outputItems().size());
        assertEquals(List.of("MISSING_MARKET_SNAPSHOT"), dto.partialReasons());
        assertEquals(1, dto.steps().size());
        assertEquals(1, dto.constraints().size());
        assertThrows(UnsupportedOperationException.class, () -> dto.inputItems().add(new UnifiedFlipDto.ItemStackDto("NOPE", 1)));
        assertThrows(UnsupportedOperationException.class, () -> dto.outputItems().add(new UnifiedFlipDto.ItemStackDto("NOPE", 1)));
        assertThrows(UnsupportedOperationException.class, () -> dto.partialReasons().add("NOPE"));
        assertThrows(UnsupportedOperationException.class, () -> dto.steps().add(new UnifiedFlipDto.StepDto(null, null, 1L, null, null, 0, null, null)));
        assertThrows(UnsupportedOperationException.class, () -> dto.constraints().add(new UnifiedFlipDto.ConstraintDto(null, null, null, null)));
    }

    @Test
    void listFieldsDefaultToEmptyWhenNull() {
        UnifiedFlipDto dto = new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.BAZAAR,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Instant.parse("2026-02-16T00:00:00Z"),
                false,
                null,
                null,
                null
        );

        assertTrue(dto.inputItems().isEmpty());
        assertTrue(dto.outputItems().isEmpty());
        assertTrue(dto.partialReasons().isEmpty());
        assertTrue(dto.steps().isEmpty());
        assertTrue(dto.constraints().isEmpty());
    }
}

