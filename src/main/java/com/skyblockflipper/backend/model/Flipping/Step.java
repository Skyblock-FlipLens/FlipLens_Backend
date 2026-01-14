package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.DurationType;
import com.skyblockflipper.backend.model.Flipping.Enums.SchedulingPolicy;
import com.skyblockflipper.backend.model.Flipping.Enums.StepResource;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;
import java.util.UUID;

@Getter
@Entity
@Table(name = "flip_step")
public class Step {

    private static final long DEFAULT_MARKET_BASED_SECONDS = 30L;

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Setter
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private StepType type;

    @Setter
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DurationType durationType;

    /**
     * Base duration in seconds depending on {@link #durationType}:
     * FIXED/INSTANT expects a concrete duration, MARKET_BASED is a fallback if no liquidity stats exist.
     */
    @Setter
    @Column(name = "base_duration_seconds")
    private Long baseDurationSeconds;

    @Setter
    @Column(name = "duration_factor")
    private Double durationFactor;

    @Setter
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private StepResource resource = StepResource.NONE;

    @Setter
    @Column(nullable = false)
    private int resourceUnits = 0;

    @Setter
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SchedulingPolicy schedulingPolicy = SchedulingPolicy.NONE;

    @Setter
    @Column(columnDefinition = "text")
    private String paramsJson;

    protected Step() {
    }

    public Step(UUID id, StepType type, DurationType durationType, Long baseDurationSeconds, Double durationFactor,
                StepResource resource, int resourceUnits, SchedulingPolicy schedulingPolicy, String paramsJson) {
        this.id = id;
        this.type = type;
        this.durationType = durationType;
        this.baseDurationSeconds = baseDurationSeconds;
        this.durationFactor = durationFactor;
        this.resource = resource == null ? StepResource.NONE : resource;
        this.resourceUnits = resourceUnits;
        this.schedulingPolicy = schedulingPolicy == null ? SchedulingPolicy.NONE : schedulingPolicy;
        this.paramsJson = paramsJson;
    }

    public static Step forBuyMarketBased(long baseSeconds, String paramsJson) {
        return new Step(null, StepType.BUY, DurationType.MARKET_BASED, baseSeconds, null,
                StepResource.NONE, 0, SchedulingPolicy.BEST_EFFORT, paramsJson);
    }

    public static Step forSellMarketBased(long baseSeconds, String paramsJson) {
        return new Step(null, StepType.SELL, DurationType.MARKET_BASED, baseSeconds, null,
                StepResource.NONE, 0, SchedulingPolicy.BEST_EFFORT, paramsJson);
    }

    public static Step forForgeFixed(long durationSeconds) {
        return new Step(null, StepType.FORGE, DurationType.FIXED, durationSeconds, null,
                StepResource.FORGE_SLOT, 1, SchedulingPolicy.LIMITED_BY_RESOURCE, null);
    }

    public static Step forCraftInstant(long seconds) {
        return new Step(null, StepType.CRAFT, DurationType.INSTANT, seconds, null,
                StepResource.NONE, 0, SchedulingPolicy.NONE, null);
    }

    @PrePersist
    @PreUpdate
    private void validate() {
        if (type == null) {
            throw new IllegalStateException("Step type must be provided.");
        }
        if (durationType == null) {
            throw new IllegalStateException("Duration type must be provided.");
        }
        if (resource == null) {
            resource = StepResource.NONE;
        }
        if (schedulingPolicy == null) {
            schedulingPolicy = SchedulingPolicy.NONE;
        }
        if (resourceUnits < 0) {
            throw new IllegalStateException("Resource units must be >= 0.");
        }

        if (durationType == DurationType.MARKET_BASED) {
            if (baseDurationSeconds == null) {
                baseDurationSeconds = DEFAULT_MARKET_BASED_SECONDS;
            }
        } else {
            if (baseDurationSeconds == null) {
                throw new IllegalStateException("Base duration seconds must be provided for FIXED or INSTANT steps.");
            }
            if (baseDurationSeconds < 0) {
                throw new IllegalStateException("Base duration seconds must be >= 0.");
            }
        }

        if (type == StepType.FORGE) {
            if (durationType != DurationType.FIXED) {
                throw new IllegalStateException("Forge steps must use FIXED duration.");
            }
            if (resource != StepResource.FORGE_SLOT) {
                throw new IllegalStateException("Forge steps must use FORGE_SLOT resource.");
            }
            if (resourceUnits < 1) {
                throw new IllegalStateException("Forge steps must use at least one resource unit.");
            }
            if (schedulingPolicy != SchedulingPolicy.LIMITED_BY_RESOURCE) {
                throw new IllegalStateException("Forge steps must be limited by resource scheduling.");
            }
        }

        if (type == StepType.BUY || type == StepType.SELL) {
            if (durationType != DurationType.MARKET_BASED && durationType != DurationType.INSTANT) {
                throw new IllegalStateException("Buy/Sell steps must use MARKET_BASED or INSTANT duration.");
            }
            if (resource != StepResource.NONE) {
                throw new IllegalStateException("Buy/Sell steps cannot require a resource.");
            }
            if (schedulingPolicy != SchedulingPolicy.BEST_EFFORT) {
                throw new IllegalStateException("Buy/Sell steps must use BEST_EFFORT scheduling.");
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Step step = (Step) o;
        return Objects.equals(id, step.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
