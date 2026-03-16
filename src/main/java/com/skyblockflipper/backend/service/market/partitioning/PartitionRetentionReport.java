package com.skyblockflipper.backend.service.market.partitioning;

import java.util.List;
import java.util.Map;

public record PartitionRetentionReport(
        int scannedPartitions,
        int droppedPartitions,
        int wouldDropPartitions,
        boolean dryRun,
        boolean partitionedTargetsDetected,
        Map<String, Integer> droppedByParent,
        Map<String, Integer> wouldDropByParent,
        List<String> messages
) {
    public static PartitionRetentionReport empty(boolean dryRun) {
        return new PartitionRetentionReport(
                0,
                0,
                0,
                dryRun,
                false,
                Map.of(),
                Map.of(),
                List.of()
        );
    }
}
