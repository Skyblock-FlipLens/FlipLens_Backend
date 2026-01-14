package com.skyblockflipper.backend.model.Flipping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConstraintTest {

    @Test
    void supportsEqualityForTypedValues() {
        Constraint first = Constraint.minCapital(500_000L);
        Constraint second = Constraint.minCapital(500_000L);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }
}
