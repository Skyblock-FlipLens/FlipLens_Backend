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
        name = "flip_current",
        indexes = {
                @Index(name = "idx_flip_current_snapshot_ts_epoch_millis", columnList = "snapshot_timestamp_epoch_millis"),
                @Index(name = "idx_flip_current_flip_type", columnList = "flip_type"),
                @Index(name = "idx_flip_current_expected_profit", columnList = "expected_profit"),
                @Index(name = "idx_flip_current_roi", columnList = "roi"),
                @Index(name = "idx_flip_current_roi_per_hour", columnList = "roi_per_hour"),
                @Index(name = "idx_flip_current_liquidity_score", columnList = "liquidity_score"),
                @Index(name = "idx_flip_current_risk_score", columnList = "risk_score")
        }
)
public class FlipCurrentEntity {

    @Id
    @Column(name = "flip_key", nullable = false, length = 128)
    private String flipKey;

    @Column(name = "stable_flip_id", nullable = false, unique = true)
    private UUID stableFlipId;

    @Enumerated(EnumType.STRING)
    @Column(name = "flip_type", nullable = false)
    private FlipType flipType;

    @Column(name = "snapshot_timestamp_epoch_millis", nullable = false)
    private long snapshotTimestampEpochMillis;

    @Column(name = "required_capital")
    private Long requiredCapital;

    @Column(name = "expected_profit")
    private Long expectedProfit;

    @Column(name = "roi")
    private Double roi;

    @Column(name = "roi_per_hour")
    private Double roiPerHour;

    @Column(name = "duration_seconds")
    private Long durationSeconds;

    @Column(name = "fees")
    private Long fees;

    @Column(name = "liquidity_score")
    private Double liquidityScore;

    @Column(name = "risk_score")
    private Double riskScore;

    @Column(name = "partial", nullable = false)
    private boolean partial;

    @Column(name = "partial_reasons_json", nullable = false, columnDefinition = "text")
    private String partialReasonsJson;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
