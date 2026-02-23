package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class UnifiedFlipDtoIdMapperTest {

    @Test
    void withIdReplacesOnlyIdAndKeepsAllOtherFields() {
        UnifiedFlipDto original = new UnifiedFlipDto(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                FlipType.BAZAAR,
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_SUGAR", 1)),
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_SUGAR_CANE", 2)),
                1_000_000L,
                250_000L,
                0.25D,
                1.0D,
                120L,
                1_000L,
                80D,
                10D,
                Instant.parse("2026-02-20T12:00:00Z"),
                false,
                List.of("none"),
                List.of(),
                List.of()
        );
        UUID replacementId = UUID.fromString("22222222-2222-2222-2222-222222222222");

        UnifiedFlipDto remapped = UnifiedFlipDtoIdMapper.withId(original, replacementId);

        assertNotNull(remapped);
        assertEquals(replacementId, remapped.id());
        assertEquals(original.flipType(), remapped.flipType());
        assertEquals(original.inputItems(), remapped.inputItems());
        assertEquals(original.outputItems(), remapped.outputItems());
        assertEquals(original.requiredCapital(), remapped.requiredCapital());
        assertEquals(original.expectedProfit(), remapped.expectedProfit());
        assertEquals(original.roi(), remapped.roi());
        assertEquals(original.roiPerHour(), remapped.roiPerHour());
        assertEquals(original.durationSeconds(), remapped.durationSeconds());
        assertEquals(original.fees(), remapped.fees());
        assertEquals(original.liquidityScore(), remapped.liquidityScore());
        assertEquals(original.riskScore(), remapped.riskScore());
        assertEquals(original.snapshotTimestamp(), remapped.snapshotTimestamp());
        assertEquals(original.partial(), remapped.partial());
        assertEquals(original.partialReasons(), remapped.partialReasons());
        assertEquals(original.steps(), remapped.steps());
        assertEquals(original.constraints(), remapped.constraints());
    }

    @Test
    void withIdReturnsNullForNullDto() {
        assertNull(UnifiedFlipDtoIdMapper.withId(null, UUID.randomUUID()));
    }
}

