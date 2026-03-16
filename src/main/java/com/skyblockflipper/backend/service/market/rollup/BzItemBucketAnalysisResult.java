package com.skyblockflipper.backend.service.market.rollup;

import com.skyblockflipper.backend.model.market.BzItemAnomalySegmentEntity;
import com.skyblockflipper.backend.model.market.BzItemBucketRollupEntity;

import java.util.List;
import java.util.Optional;

public record BzItemBucketAnalysisResult(
        Optional<BzItemBucketRollupEntity> rollup,
        List<BzItemAnomalySegmentEntity> anomalySegments
) {

    public BzItemBucketAnalysisResult {
        rollup = rollup == null ? Optional.empty() : rollup;
        anomalySegments = anomalySegments == null ? List.of() : List.copyOf(anomalySegments);
    }
}
