package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UnifiedFlipDtoTest {

    @Test
    void partialReasonsUsesDefensiveCopy() {
        List<String> reasons = new ArrayList<>();
        reasons.add("MISSING_MARKET_SNAPSHOT");

        UnifiedFlipDto dto = new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.BAZAAR,
                List.of(),
                List.of(),
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
                List.of(),
                List.of()
        );

        reasons.add("MUTATED_AFTER_CONSTRUCTION");
        assertEquals(List.of("MISSING_MARKET_SNAPSHOT"), dto.partialReasons());
        assertThrows(UnsupportedOperationException.class, () -> dto.partialReasons().add("NOPE"));
    }
}
