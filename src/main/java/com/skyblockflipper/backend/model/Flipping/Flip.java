package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.Table;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "flip")
public class Flip {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Enumerated(EnumType.STRING)
    private FlipType flipType;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "flip_id", nullable = false)
    @OrderColumn(name = "step_order")
    private List<Step> steps = new ArrayList<>();

    @Column(nullable = false)
    private String resultItemId;

    @ElementCollection
    private List<Constraint> constraints = new ArrayList<>();

    protected Flip() {
    }

    public Flip(UUID id, FlipType flipType, List<Step> steps, String resultItemId, List<Constraint> constraints) {
        this.id = id;
        this.flipType = flipType;
        if (steps != null) {
            this.steps = new ArrayList<>(steps);
        }
        this.resultItemId = resultItemId;
        if (constraints != null) {
            this.constraints = new ArrayList<>(constraints);
        }
    }

    public UUID getId() {
        return id;
    }

    public FlipType getFlipType() {
        return flipType;
    }

    public void setFlipType(FlipType flipType) {
        this.flipType = flipType;
    }

    public List<Step> getSteps() {
        return steps;
    }

    public void setSteps(List<Step> steps) {
        this.steps = steps == null ? new ArrayList<>() : new ArrayList<>(steps);
    }

    public String getResultItemId() {
        return resultItemId;
    }

    public void setResultItemId(String resultItemId) {
        this.resultItemId = resultItemId;
    }

    public List<Constraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<Constraint> constraints) {
        this.constraints = constraints == null ? new ArrayList<>() : new ArrayList<>(constraints);
    }

    public Duration getTotalDuration() {
        return sumDurationSeconds(steps);
    }

    public Duration getActiveDuration() {
        return sumDurationSeconds(steps.stream().filter(this::isActiveStep).toList());
    }

    public Duration getPassiveDuration() {
        return sumDurationSeconds(steps.stream().filter(step -> !isActiveStep(step)).toList());
    }

    public boolean requiresForgeSlot() {
        return steps.stream().anyMatch(step -> step.getType() == StepType.FORGE);
    }

    private boolean isActiveStep(Step step) {
        return step.getType() != StepType.FORGE;
    }

    private Duration sumDurationSeconds(List<Step> steps) {
        long totalSeconds = 0L;
        for (Step step : steps) {
            Long baseSeconds = step.getBaseDurationSeconds();
            if (baseSeconds != null) {
                totalSeconds += baseSeconds;
            }
        }
        return Duration.ofSeconds(totalSeconds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Flip flip = (Flip) o;
        return Objects.equals(id, flip.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
