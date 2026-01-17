package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.DurationType;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlipTest {

    @Test
    void durationsSplitActiveAndPassive() {
        Step buy = new Step(null, StepType.BUY, DurationType.MARKET_BASED, 30L, null,
                null, 0, null, null);
        Step craft = Step.forCraftInstant(10L);
        Step forge = Step.forForgeFixed(60L);

        Flip flip = new Flip(null, FlipType.CRAFTING, List.of(buy, craft, forge), "item", List.of());

        assertEquals(Duration.ofSeconds(100L), flip.getTotalDuration());
        assertEquals(Duration.ofSeconds(40L), flip.getActiveDuration());
        assertEquals(Duration.ofSeconds(60L), flip.getPassiveDuration());
        assertTrue(flip.requiresForgeSlot());
    }

    @Test
    void requiresForgeSlotFalseWithoutForgeStep() {
        Step buy = new Step(null, StepType.BUY, DurationType.MARKET_BASED, 30L, null,
                null, 0, null, null);
        Step craft = Step.forCraftInstant(10L);

        Flip flip = new Flip(null, FlipType.CRAFTING, List.of(buy, craft), "item", List.of());

        assertFalse(flip.requiresForgeSlot());
    }


    @Test
    void setStepsAndConstraintsHandleNulls() {
        Flip flip = new Flip(null, FlipType.CRAFTING, null, "item", null);

        assertEquals(0, flip.getSteps().size());
        assertEquals(0, flip.getConstraints().size());

        flip.setSteps(null);
        flip.setConstraints(null);

        assertEquals(0, flip.getSteps().size());
        assertEquals(0, flip.getConstraints().size());
    }

    @Test
    void constructorCopiesStepsList() {
        Step buy = new Step(null, StepType.BUY, DurationType.MARKET_BASED, 30L, null,
                null, 0, null, null);
        List<Step> steps = new java.util.ArrayList<>(List.of(buy));

        Flip flip = new Flip(null, FlipType.CRAFTING, steps, "item", List.of());
        steps.add(Step.forCraftInstant(10L));

        assertEquals(1, flip.getSteps().size());
    }

    @Test
    void totalDurationSkipsNullBaseSeconds() {
        Step craft = new Step(null, StepType.CRAFT, DurationType.FIXED, null, null,
                null, 0, null, null);
        Step forge = Step.forForgeFixed(60L);

        Flip flip = new Flip(null, FlipType.CRAFTING, List.of(craft, forge), "item", List.of());

        assertEquals(Duration.ofSeconds(60L), flip.getTotalDuration());
    }

}