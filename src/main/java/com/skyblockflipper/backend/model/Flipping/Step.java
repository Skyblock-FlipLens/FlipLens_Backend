package com.skyblockflipper.backend.model.Flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.DurationType;
import com.skyblockflipper.backend.model.Flipping.Enums.SchedulingPolicy;
import com.skyblockflipper.backend.model.Flipping.Enums.StepResource;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import jakarta.persistence.*;

import java.util.UUID;

@Entity
@Table(name = "flip_step")
public class Step {

    @Id
    @GeneratedValue
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private StepType type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DurationType durationType;

    @Column(name = "base_duration_seconds")
    private Long baseDurationSeconds;

    @Column(name = "duration_factor")
    private Double durationFactor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private StepResource resource = StepResource.NONE;

    @Column(nullable = false)
    private int resourceUnits = 0;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SchedulingPolicy schedulingPolicy = SchedulingPolicy.NONE;

    @Column(columnDefinition = "jsonb")
    private String paramsJson;
}

