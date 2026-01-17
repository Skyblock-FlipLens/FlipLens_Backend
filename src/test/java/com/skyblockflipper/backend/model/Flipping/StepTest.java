package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.DurationType;
import com.skyblockflipper.backend.model.Flipping.Enums.SchedulingPolicy;
import com.skyblockflipper.backend.model.Flipping.Enums.StepResource;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StepTest {

    @Test
    void validateDefaultsMarketBasedBaseSeconds() {
        Step step = new Step(null, StepType.BUY, DurationType.MARKET_BASED, null, null,
                StepResource.NONE, 0, SchedulingPolicy.BEST_EFFORT, null);

        ReflectionTestUtils.invokeMethod(step, "validate");

        assertEquals(30L, step.getBaseDurationSeconds());
    }

    @Test
    void validateRejectsInvalidForgeStep() {
        Step step = new Step(null, StepType.FORGE, DurationType.INSTANT, 5L, null,
                StepResource.NONE, 0, SchedulingPolicy.NONE, null);

        assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(step, "validate"));
    }


    @Test
    void validateDefaultsNullResourceAndScheduling() {
        Step step = new Step(null, StepType.CRAFT, DurationType.FIXED, 5L, null,
                null, 0, null, null);

        ReflectionTestUtils.invokeMethod(step, "validate");

        assertEquals(StepResource.NONE, step.getResource());
        assertEquals(SchedulingPolicy.NONE, step.getSchedulingPolicy());
    }

    @Test
    void validateRejectsNegativeResourceUnits() {
        Step step = new Step(null, StepType.CRAFT, DurationType.FIXED, 5L, null,
                StepResource.NONE, -1, SchedulingPolicy.NONE, null);

        assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(step, "validate"));
    }

    @Test
    void validateRejectsMissingBaseDurationForFixed() {
        Step step = new Step(null, StepType.CRAFT, DurationType.FIXED, null, null,
                StepResource.NONE, 0, SchedulingPolicy.NONE, null);

        assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(step, "validate"));
    }

    @Test
    void validateRejectsNegativeBaseDurationForFixed() {
        Step step = new Step(null, StepType.CRAFT, DurationType.FIXED, -1L, null,
                StepResource.NONE, 0, SchedulingPolicy.NONE, null);

        assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(step, "validate"));
    }

    @Test
    void validateRejectsBuySellInvalidDuration() {
        Step step = new Step(null, StepType.BUY, DurationType.FIXED, 5L, null,
                StepResource.NONE, 0, SchedulingPolicy.BEST_EFFORT, null);

        assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(step, "validate"));
    }

    @Test
    void validateRejectsBuySellWithResource() {
        Step step = new Step(null, StepType.SELL, DurationType.INSTANT, 1L, null,
                StepResource.FORGE_SLOT, 1, SchedulingPolicy.BEST_EFFORT, null);

        assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(step, "validate"));
    }

    @Test
    void validateRejectsBuySellWithWrongScheduling() {
        Step step = new Step(null, StepType.BUY, DurationType.INSTANT, 1L, null,
                StepResource.NONE, 0, SchedulingPolicy.NONE, null);

        assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(step, "validate"));
    }

    @Test
    void factoryMethodsSetExpectedDefaults() {
        Step buy = Step.forBuyMarketBased(25L, "{}");
        Step sell = Step.forSellMarketBased(25L, "{}");
        Step forge = Step.forForgeFixed(120L);
        Step craft = Step.forCraftInstant(2L);

        assertEquals(StepType.BUY, buy.getType());
        assertEquals(DurationType.MARKET_BASED, buy.getDurationType());
        assertEquals(StepType.SELL, sell.getType());
        assertEquals(StepType.FORGE, forge.getType());
        assertEquals(StepType.CRAFT, craft.getType());
    }
}
