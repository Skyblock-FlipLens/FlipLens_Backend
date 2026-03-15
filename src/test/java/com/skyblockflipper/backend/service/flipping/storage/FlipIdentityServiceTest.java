package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlipIdentityServiceTest {

    @Test
    void deriveIgnoresNullConstraintsInCanonicalization() {
        FlipIdentityService service = new FlipIdentityService(new ObjectMapper());
        List<Constraint> constraints = new ArrayList<>();
        constraints.add(null);
        constraints.add(Constraint.minCapital(1_000_000L));
        Flip flip = new Flip(
                null,
                FlipType.BAZAAR,
                List.of(Step.forBuyMarketBased(30L, "{\"side\":\"buy\"}")),
                "ENCHANTED_SUGAR",
                constraints
        );

        FlipIdentityService.Identity identity = assertDoesNotThrow(() -> service.derive(flip));
        int constraintCount = new ObjectMapper().readTree(identity.constraintsJson()).size();

        assertEquals(1, constraintCount);
    }

    @Test
    void deriveCanonicalizesJsonOrderingAndNormalizesStrings() throws Exception {
        FlipIdentityService service = new FlipIdentityService(new ObjectMapper());
        List<Step> stepsWithNull = new ArrayList<>();
        stepsWithNull.add(null);
        stepsWithNull.add(Step.forBuyMarketBased(30L, "{\"b\":2,\"a\":1}"));
        stepsWithNull.add(Step.forWaitFixed(60L));
        Flip first = new Flip(
                null,
                FlipType.BAZAAR,
                stepsWithNull,
                " ENCHANTED_SUGAR ",
                List.of(
                        Constraint.recipeUnlocked(" RECIPE_A "),
                        Constraint.minForgeSlots(2),
                        Constraint.minCapital(1_000_000L)
                )
        );
        Flip logicallyEquivalent = new Flip(
                null,
                FlipType.BAZAAR,
                List.of(
                        Step.forBuyMarketBased(30L, "{\"a\":1,\"b\":2}"),
                        Step.forWaitFixed(60L)
                ),
                "ENCHANTED_SUGAR",
                List.of(
                        Constraint.minCapital(1_000_000L),
                        Constraint.minForgeSlots(2),
                        Constraint.recipeUnlocked("RECIPE_A")
                )
        );

        FlipIdentityService.Identity firstIdentity = service.derive(first);
        FlipIdentityService.Identity secondIdentity = service.derive(logicallyEquivalent);
        ObjectMapper objectMapper = new ObjectMapper();

        assertEquals(firstIdentity.flipKey(), secondIdentity.flipKey());
        assertEquals(firstIdentity.stableFlipId(), secondIdentity.stableFlipId());
        assertEquals("v1_c46ce0bceaa038e93d74251455446fb62c0cb1c9cb6c19d6b85a7402f0715370", firstIdentity.flipKey());
        assertEquals(UUID.fromString("1cb6f02b-eca6-3b2c-8969-cb9f05115fd7"), firstIdentity.stableFlipId());
        assertEquals("ENCHANTED_SUGAR", firstIdentity.resultItemId());
        assertEquals(
                "[{\"baseDurationSeconds\":30,\"durationFactor\":null,\"durationType\":\"MARKET_BASED\",\"paramsJson\":\"{\\\"a\\\":1,\\\"b\\\":2}\",\"resource\":\"NONE\",\"resourceUnits\":0,\"schedulingPolicy\":\"BEST_EFFORT\",\"type\":\"BUY\"},{\"baseDurationSeconds\":60,\"durationFactor\":null,\"durationType\":\"FIXED\",\"paramsJson\":\"\",\"resource\":\"NONE\",\"resourceUnits\":0,\"schedulingPolicy\":\"NONE\",\"type\":\"WAIT\"}]",
                firstIdentity.stepsJson()
        );
        assertEquals(
                "[{\"intValue\":null,\"longValue\":1000000,\"stringValue\":null,\"type\":\"MIN_CAPITAL\"},{\"intValue\":2,\"longValue\":null,\"stringValue\":null,\"type\":\"MIN_FORGE_SLOTS\"},{\"intValue\":null,\"longValue\":null,\"stringValue\":\"RECIPE_A\",\"type\":\"RECIPE_UNLOCKED\"}]",
                firstIdentity.constraintsJson()
        );
        assertEquals("{\"a\":1,\"b\":2}", objectMapper.readTree(firstIdentity.stepsJson()).get(0).get("paramsJson").asText());
        assertEquals("MIN_CAPITAL", objectMapper.readTree(firstIdentity.constraintsJson()).get(0).get("type").asText());
        assertEquals("MIN_FORGE_SLOTS", objectMapper.readTree(firstIdentity.constraintsJson()).get(1).get("type").asText());
        assertEquals("RECIPE_UNLOCKED", objectMapper.readTree(firstIdentity.constraintsJson()).get(2).get("type").asText());
    }

    @Test
    void deriveFallsBackToTrimmedRawJsonWhenParamsAreInvalid() {
        FlipIdentityService service = new FlipIdentityService(new ObjectMapper());
        Flip first = new Flip(
                null,
                FlipType.CRAFTING,
                List.of(Step.forBuyMarketBased(30L, "  not-json  ")),
                "GOLD_INGOT",
                List.of()
        );
        Flip second = new Flip(
                null,
                FlipType.CRAFTING,
                List.of(Step.forBuyMarketBased(30L, "not-json")),
                "GOLD_INGOT",
                List.of()
        );

        FlipIdentityService.Identity firstIdentity = service.derive(first);
        FlipIdentityService.Identity secondIdentity = service.derive(second);

        assertEquals(firstIdentity.flipKey(), secondIdentity.flipKey());
        assertTrue(firstIdentity.stepsJson().contains("not-json"));
        assertNotEquals("", firstIdentity.flipKey());
    }
}

