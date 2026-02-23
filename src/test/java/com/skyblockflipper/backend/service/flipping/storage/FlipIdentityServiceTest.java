package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FlipIdentityServiceTest {

    @Test
    void deriveIgnoresNullConstraintsInCanonicalization() throws Exception {
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
}

