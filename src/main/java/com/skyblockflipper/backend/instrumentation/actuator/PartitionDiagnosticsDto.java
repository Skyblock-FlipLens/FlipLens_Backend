package com.skyblockflipper.backend.instrumentation.actuator;

import java.time.LocalDate;
import java.util.List;

public record PartitionDiagnosticsDto(
        String status,
        boolean enabled,
        String mode,
        boolean dryRun,
        String schemaName,
        LocalDate todayUtc,
        int precreateDays,
        int totalPartitions,
        int totalRetainedPartitions,
        int totalFuturePartitions,
        int totalOutOfRetentionPartitions,
        int totalDefaultPartitions,
        int totalUnclassifiedPartitions,
        boolean partitionsIncluded,
        List<TargetDiagnosticsDto> targets
) {

    public PartitionDiagnosticsDto {
        targets = targets == null ? List.of() : List.copyOf(targets);
    }

    public record TargetDiagnosticsDto(
            String target,
            String parentTable,
            int retentionDays,
            boolean partitioned,
            int partitionCount,
            LocalDate oldestKeptDayUtc,
            LocalDate oldestPartitionDayUtc,
            LocalDate newestPartitionDayUtc,
            int retainedPartitionCount,
            int futurePartitionCount,
            int outOfRetentionPartitionCount,
            int defaultPartitionCount,
            int unclassifiedPartitionCount,
            List<String> partitions,
            String error
    ) {
        public TargetDiagnosticsDto {
            partitions = partitions == null ? List.of() : List.copyOf(partitions);
        }
    }
}
