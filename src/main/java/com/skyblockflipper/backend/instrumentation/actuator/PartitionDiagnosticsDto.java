package com.skyblockflipper.backend.instrumentation.actuator;

import java.util.List;

public record PartitionDiagnosticsDto(
        String status,
        boolean enabled,
        String mode,
        boolean dryRun,
        String schemaName,
        int totalPartitions,
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
            List<String> partitions,
            String error
    ) {
        public TargetDiagnosticsDto {
            partitions = partitions == null ? List.of() : List.copyOf(partitions);
        }
    }
}
