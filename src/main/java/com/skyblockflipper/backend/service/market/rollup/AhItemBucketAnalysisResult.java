package com.skyblockflipper.backend.service.market.rollup;

import com.skyblockflipper.backend.model.market.AhItemAnomalySegmentEntity;
import com.skyblockflipper.backend.model.market.AhItemBucketRollupEntity;

import java.util.List;
import java.util.Optional;

public record AhItemBucketAnalysisResult(
        Optional<AhItemBucketRollupEntity> rollup,
        List<AhItemAnomalySegmentEntity> anomalySegments
) {

    public AhItemBucketAnalysisResult {
        rollup = rollup == null ? Optional.empty() : rollup;
        anomalySegments = anomalySegments == null ? List.of() : List.copyOf(anomalySegments);
    }
}
