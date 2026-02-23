package com.skyblockflipper.backend.model.flippingstorage;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@Entity
@Table(
        name = "flip_definition",
        indexes = {
                @Index(name = "idx_flip_definition_stable_flip_id", columnList = "stable_flip_id"),
                @Index(name = "idx_flip_definition_flip_type", columnList = "flip_type")
        }
)
public class FlipDefinitionEntity {

    @Id
    @Column(name = "flip_key", nullable = false, length = 128)
    private String flipKey;

    @Column(name = "stable_flip_id", nullable = false, unique = true)
    private UUID stableFlipId;

    @Enumerated(EnumType.STRING)
    @Column(name = "flip_type", nullable = false)
    private FlipType flipType;

    @Column(name = "result_item_id", nullable = false)
    private String resultItemId;

    @Column(name = "steps_json", nullable = false, columnDefinition = "text")
    private String stepsJson;

    @Column(name = "constraints_json", nullable = false, columnDefinition = "text")
    private String constraintsJson;

    @Column(name = "key_version", nullable = false)
    private int keyVersion;

    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
