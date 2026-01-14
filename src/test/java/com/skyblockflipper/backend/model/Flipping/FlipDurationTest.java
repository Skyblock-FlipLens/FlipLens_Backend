package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlipDurationTest {

    @Test
    void calculatesActivePassiveAndTotalDurations() {
        Step buy = Step.forBuyMarketBased(30L, "{\"itemId\":\"HAY_BLOCK\",\"amount\":8}");
        Step craft = Step.forCraftInstant(10L);
        Step forge = Step.forForgeFixed(60L);

        Flip flip = new Flip(null, FlipType.CRAFTING, List.of(buy, craft, forge), "HAY_BALE", List.of());

        assertEquals(Duration.ofSeconds(100L), flip.getTotalDuration());
        assertEquals(Duration.ofSeconds(40L), flip.getActiveDuration());
        assertEquals(Duration.ofSeconds(60L), flip.getPassiveDuration());
    }
}
