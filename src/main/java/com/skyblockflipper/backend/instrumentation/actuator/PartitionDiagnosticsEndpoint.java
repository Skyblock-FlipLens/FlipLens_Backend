package com.skyblockflipper.backend.instrumentation.actuator;

import com.skyblockflipper.backend.repository.PartitionAdminRepository;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Endpoint(id = "partitionDiagnostics")
@RequiredArgsConstructor
public class PartitionDiagnosticsEndpoint {

    private final PartitionAdminRepository partitionAdminRepository;
    private final PartitioningProperties partitioningProperties;

    @ReadOperation
    public PartitionDiagnosticsDto partitionDiagnostics(Boolean includePartitions) {
        boolean includePartitionNames = Boolean.TRUE.equals(includePartitions);
        List<PartitionDiagnosticsDto.TargetDiagnosticsDto> targets = List.of(
                inspectTarget(
                        "marketSnapshot",
                        partitioningProperties.getMarketSnapshotParentTable(),
                        Math.max(0, partitioningProperties.getMarketSnapshotRetentionDays()),
                        includePartitionNames
                ),
                inspectTarget(
                        "ahSnapshot",
                        partitioningProperties.getAhSnapshotParentTable(),
                        Math.max(0, partitioningProperties.getAhSnapshotRetentionDays()),
                        includePartitionNames
                ),
                inspectTarget(
                        "bzSnapshot",
                        partitioningProperties.getBzSnapshotParentTable(),
                        Math.max(0, partitioningProperties.getBzSnapshotRetentionDays()),
                        includePartitionNames
                )
        );

        int totalPartitions = targets.stream()
                .mapToInt(PartitionDiagnosticsDto.TargetDiagnosticsDto::partitionCount)
                .sum();
        boolean anyErrors = targets.stream().anyMatch(target -> target.error() != null);
        boolean anyPartitioned = targets.stream().anyMatch(PartitionDiagnosticsDto.TargetDiagnosticsDto::partitioned);

        return new PartitionDiagnosticsDto(
                resolveStatus(anyErrors, anyPartitioned),
                partitioningProperties.isEnabled(),
                partitioningProperties.getMode() == null ? null : partitioningProperties.getMode().name(),
                partitioningProperties.isDryRun(),
                partitioningProperties.getSchemaName(),
                totalPartitions,
                includePartitionNames,
                targets
        );
    }

    private PartitionDiagnosticsDto.TargetDiagnosticsDto inspectTarget(String target,
                                                                       String parentTable,
                                                                       int retentionDays,
                                                                       boolean includePartitionNames) {
        try {
            String schemaName = partitioningProperties.getSchemaName();
            boolean partitioned = partitionAdminRepository.isTablePartitioned(schemaName, parentTable);
            List<String> partitions = partitioned
                    ? partitionAdminRepository.listChildPartitions(schemaName, parentTable)
                    : List.of();
            return new PartitionDiagnosticsDto.TargetDiagnosticsDto(
                    target,
                    parentTable,
                    retentionDays,
                    partitioned,
                    partitions.size(),
                    includePartitionNames ? partitions : List.of(),
                    null
            );
        } catch (RuntimeException exception) {
            return new PartitionDiagnosticsDto.TargetDiagnosticsDto(
                    target,
                    parentTable,
                    retentionDays,
                    false,
                    0,
                    List.of(),
                    summarize(exception)
            );
        }
    }

    private String resolveStatus(boolean anyErrors, boolean anyPartitioned) {
        if (anyErrors) {
            return "ERROR";
        }
        if (!partitioningProperties.isEnabled()) {
            return "DISABLED";
        }
        return anyPartitioned ? "OK" : "NO_PARTITIONED_TARGETS";
    }

    private String summarize(RuntimeException exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return exception.getClass().getSimpleName() + ": " + message;
    }
}
