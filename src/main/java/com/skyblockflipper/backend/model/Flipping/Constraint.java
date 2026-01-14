package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class Constraint implements Serializable {

    @Enumerated(EnumType.STRING)
    @Column(name = "constraint_type", nullable = false)
    private ConstraintType type;

    @Column(name = "string_value")
    private String stringValue;

    @Column(name = "int_value")
    private Integer intValue;

    @Column(name = "long_value")
    private Long longValue;

    protected Constraint() {
    }

    private Constraint(ConstraintType type, String stringValue, Integer intValue, Long longValue) {
        this.type = Objects.requireNonNull(type, "type");
        this.stringValue = stringValue;
        this.intValue = intValue;
        this.longValue = longValue;
    }

    public static Constraint minForgeSlots(int slots) {
        return new Constraint(ConstraintType.MIN_FORGE_SLOTS, null, slots, null);
    }

    public static Constraint recipeUnlocked(String recipeId) {
        return new Constraint(ConstraintType.RECIPE_UNLOCKED, recipeId, null, null);
    }

    public static Constraint minCapital(long coins) {
        return new Constraint(ConstraintType.MIN_CAPITAL, null, null, coins);
    }

    public ConstraintType getType() {
        return type;
    }

    public String getStringValue() {
        return stringValue;
    }

    public Integer getIntValue() {
        return intValue;
    }

    public Long getLongValue() {
        return longValue;
    }

    @Override
    public String toString() {
        return "Constraint{" +
                "type=" + type +
                ", stringValue='" + stringValue + '\'' +
                ", intValue=" + intValue +
                ", longValue=" + longValue +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Constraint that = (Constraint) o;
        return type == that.type
                && Objects.equals(stringValue, that.stringValue)
                && Objects.equals(intValue, that.intValue)
                && Objects.equals(longValue, that.longValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, stringValue, intValue, longValue);
    }
}
