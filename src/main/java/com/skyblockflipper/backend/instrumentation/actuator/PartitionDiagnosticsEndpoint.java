package com.skyblockflipper.backend.instrumentation.actuator;

import com.skyblockflipper.backend.repository.PartitionAdminRepository;
import com.skyblockflipper.backend.service.market.partitioning.UtcDayBucket;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Endpoint(id = "partitionDiagnostics")
public class PartitionDiagnosticsEndpoint {

    private final PartitionAdminRepository partitionAdminRepository;
    private final PartitioningProperties partitioningProperties;
    private final Clock clock;

    @Autowired
    public PartitionDiagnosticsEndpoint(PartitionAdminRepository partitionAdminRepository,
                                        PartitioningProperties partitioningProperties) {
        this(partitionAdminRepository, partitioningProperties, Clock.systemUTC());
    }

    PartitionDiagnosticsEndpoint(PartitionAdminRepository partitionAdminRepository,
                                 PartitioningProperties partitioningProperties,
                                 Clock clock) {
        this.partitionAdminRepository = partitionAdminRepository;
        this.partitioningProperties = partitioningProperties;
        this.clock = clock;
    }

    @ReadOperation
    public PartitionDiagnosticsDto partitionDiagnostics(@Nullable Boolean includePartitions) {
        boolean includePartitionNames = Boolean.TRUE.equals(includePartitions);
        LocalDate todayUtc = UtcDayBucket.utcDay(Instant.now(clock));
        List<PartitionDiagnosticsDto.TargetDiagnosticsDto> targets = List.of(
                inspectTarget(
                        "marketSnapshot",
                        partitioningProperties.getMarketSnapshotParentTable(),
                        Math.max(0, partitioningProperties.getMarketSnapshotRetentionDays()),
                        todayUtc,
                        includePartitionNames
                ),
                inspectTarget(
                        "ahSnapshot",
                        partitioningProperties.getAhSnapshotParentTable(),
                        Math.max(0, partitioningProperties.getAhSnapshotRetentionDays()),
                        todayUtc,
                        includePartitionNames
                ),
                inspectTarget(
                        "bzSnapshot",
                        partitioningProperties.getBzSnapshotParentTable(),
                        Math.max(0, partitioningProperties.getBzSnapshotRetentionDays()),
                        todayUtc,
                        includePartitionNames
                )
        );

        int totalPartitions = targets.stream()
                .mapToInt(PartitionDiagnosticsDto.TargetDiagnosticsDto::partitionCount)
                .sum();
        int totalRetainedPartitions = targets.stream()
                .mapToInt(PartitionDiagnosticsDto.TargetDiagnosticsDto::retainedPartitionCount)
                .sum();
        int totalFuturePartitions = targets.stream()
                .mapToInt(PartitionDiagnosticsDto.TargetDiagnosticsDto::futurePartitionCount)
                .sum();
        int totalOutOfRetentionPartitions = targets.stream()
                .mapToInt(PartitionDiagnosticsDto.TargetDiagnosticsDto::outOfRetentionPartitionCount)
                .sum();
        int totalDefaultPartitions = targets.stream()
                .mapToInt(PartitionDiagnosticsDto.TargetDiagnosticsDto::defaultPartitionCount)
                .sum();
        int totalUnclassifiedPartitions = targets.stream()
                .mapToInt(PartitionDiagnosticsDto.TargetDiagnosticsDto::unclassifiedPartitionCount)
                .sum();
        boolean anyErrors = targets.stream().anyMatch(target -> target.error() != null);
        boolean anyPartitioned = targets.stream().anyMatch(PartitionDiagnosticsDto.TargetDiagnosticsDto::partitioned);

        return new PartitionDiagnosticsDto(
                resolveStatus(anyErrors, anyPartitioned),
                partitioningProperties.isEnabled(),
                partitioningProperties.getMode() == null ? null : partitioningProperties.getMode().name(),
                partitioningProperties.isDryRun(),
                partitioningProperties.getSchemaName(),
                todayUtc,
                Math.max(0, partitioningProperties.getPrecreateDays()),
                totalPartitions,
                totalRetainedPartitions,
                totalFuturePartitions,
                totalOutOfRetentionPartitions,
                totalDefaultPartitions,
                totalUnclassifiedPartitions,
                includePartitionNames,
                targets
        );
    }

    private PartitionDiagnosticsDto.TargetDiagnosticsDto inspectTarget(String target,
                                                                       String parentTable,
                                                                       int retentionDays,
                                                                       LocalDate todayUtc,
                                                                       boolean includePartitionNames) {
        try {
            String schemaName = partitioningProperties.getSchemaName();
            boolean partitioned = partitionAdminRepository.isTablePartitioned(schemaName, parentTable);
            List<String> partitions = partitioned
                    ? partitionAdminRepository.listChildPartitions(schemaName, parentTable)
                    : List.of();
            PartitionBreakdown breakdown = classifyPartitions(parentTable, partitions, retentionDays, todayUtc);
            return new PartitionDiagnosticsDto.TargetDiagnosticsDto(
                    target,
                    parentTable,
                    retentionDays,
                    partitioned,
                    partitions.size(),
                    breakdown.oldestKeptDayUtc(),
                    breakdown.oldestPartitionDayUtc(),
                    breakdown.newestPartitionDayUtc(),
                    breakdown.retainedPartitionCount(),
                    breakdown.futurePartitionCount(),
                    breakdown.outOfRetentionPartitionCount(),
                    breakdown.defaultPartitionCount(),
                    breakdown.unclassifiedPartitionCount(),
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
                    retentionDays <= 0 ? null : todayUtc.minusDays(retentionDays - 1L),
                    null,
                    null,
                    0,
                    0,
                    0,
                    0,
                    0,
                    List.of(),
                    summarize(exception)
            );
        }
    }

    private PartitionBreakdown classifyPartitions(String parentTable,
                                                  List<String> partitions,
                                                  int retentionDays,
                                                  LocalDate todayUtc) {
        LocalDate oldestKeptDayUtc = retentionDays <= 0 ? null : todayUtc.minusDays(retentionDays - 1L);
        int retained = 0;
        int future = 0;
        int outOfRetention = 0;
        int defaults = 0;
        int unclassified = 0;
        LocalDate oldestPartitionDayUtc = null;
        LocalDate newestPartitionDayUtc = null;

        for (String partition : partitions) {
            if (partition == null || partition.isBlank()) {
                unclassified++;
                continue;
            }
            if (partition.equalsIgnoreCase(parentTable + "_default")) {
                defaults++;
                continue;
            }
            LocalDate partitionDay = parseDaySuffix(parentTable, partition);
            if (partitionDay == null) {
                unclassified++;
                continue;
            }
            if (oldestPartitionDayUtc == null || partitionDay.isBefore(oldestPartitionDayUtc)) {
                oldestPartitionDayUtc = partitionDay;
            }
            if (newestPartitionDayUtc == null || partitionDay.isAfter(newestPartitionDayUtc)) {
                newestPartitionDayUtc = partitionDay;
            }
            if (partitionDay.isAfter(todayUtc)) {
                future++;
                continue;
            }
            if (oldestKeptDayUtc != null && partitionDay.isBefore(oldestKeptDayUtc)) {
                outOfRetention++;
                continue;
            }
            retained++;
        }

        return new PartitionBreakdown(
                oldestKeptDayUtc,
                oldestPartitionDayUtc,
                newestPartitionDayUtc,
                retained,
                future,
                outOfRetention,
                defaults,
                unclassified
        );
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

    private LocalDate parseDaySuffix(String parentTable, String partitionTable) {
        String pattern = "^" + Pattern.quote(parentTable) + "_(\\d{4})_(\\d{2})_(\\d{2})$";
        Matcher matcher = Pattern.compile(pattern).matcher(partitionTable);
        if (!matcher.matches()) {
            return null;
        }
        try {
            return LocalDate.of(
                    Integer.parseInt(matcher.group(1)),
                    Integer.parseInt(matcher.group(2)),
                    Integer.parseInt(matcher.group(3))
            );
        } catch (RuntimeException exception) {
            return null;
        }
    }

    private record PartitionBreakdown(
            LocalDate oldestKeptDayUtc,
            LocalDate oldestPartitionDayUtc,
            LocalDate newestPartitionDayUtc,
            int retainedPartitionCount,
            int futurePartitionCount,
            int outOfRetentionPartitionCount,
            int defaultPartitionCount,
            int unclassifiedPartitionCount
    ) {
    }
}
