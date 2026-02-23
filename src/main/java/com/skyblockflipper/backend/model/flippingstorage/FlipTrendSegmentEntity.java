package com.skyblockflipper.backend.model.flippingstorage;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(
        name = "flip_trend_segment",
        indexes = {
                @Index(name = "idx_flip_trend_segment_flip_key_from_snapshot", columnList = "flip_key,valid_from_snapshot_epoch_millis")
        }
)
public class FlipTrendSegmentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "flip_key", nullable = false, length = 128)
    private String flipKey;

    @Column(name = "valid_from_snapshot_epoch_millis", nullable = false)
    private long validFromSnapshotEpochMillis;

    @Column(name = "valid_to_snapshot_epoch_millis", nullable = false)
    private long validToSnapshotEpochMillis;

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

    @Column(name = "sample_count", nullable = false)
    private int sampleCount;

    @Column(name = "created_at_epoch_millis", nullable = false)
    private long createdAtEpochMillis;

    @Column(name = "updated_at_epoch_millis", nullable = false)
    private long updatedAtEpochMillis;
}
