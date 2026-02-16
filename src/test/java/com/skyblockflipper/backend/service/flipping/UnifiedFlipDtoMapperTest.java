package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class UnifiedFlipDtoMapperTest {

    private final UnifiedFlipDtoMapper mapper = new UnifiedFlipDtoMapper(new ObjectMapper());

    @Test
    void mapsCoreUnifiedFieldsFromFlip() {
        UUID id = UUID.randomUUID();
        Flip flip = new Flip(
                id,
                FlipType.FORGE,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"itemId\":\"ENCHANTED_DIAMOND\",\"amount\":2}"),
                        Step.forForgeFixed(3600L),
                        Step.forSellMarketBased(15L, "{\"itemId\":\"REFINED_DIAMOND\",\"amount\":1}")
                ),
                "REFINED_DIAMOND",
                List.of(Constraint.minCapital(150_000L))
        );

        UnifiedFlipDto dto = mapper.toDto(flip);

        assertEquals(id, dto.id());
        assertEquals(FlipType.FORGE, dto.flipType());
        assertEquals(3_645L, dto.durationSeconds());
        assertEquals(150_000L, dto.requiredCapital());
        assertNull(dto.expectedProfit());
        assertNull(dto.roi());
        assertNull(dto.roiPerHour());
        assertEquals(1, dto.inputItems().size());
        assertEquals("ENCHANTED_DIAMOND", dto.inputItems().getFirst().itemId());
        assertEquals(2, dto.inputItems().getFirst().amount());
        assertEquals(1, dto.outputItems().size());
        assertEquals("REFINED_DIAMOND", dto.outputItems().getFirst().itemId());
        assertEquals(2, dto.outputItems().getFirst().amount());
        assertEquals(3, dto.steps().size());
        assertEquals(1, dto.constraints().size());
    }

    @Test
    void returnsNullForNullFlip() {
        assertNull(mapper.toDto(null));
    }
}
