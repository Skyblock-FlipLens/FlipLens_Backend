package com.skyblockflipper.backend.service.market.partitioning;

import com.skyblockflipper.backend.repository.PartitionAdminRepository;
import com.skyblockflipper.backend.service.market.rollup.MarketBucketMaterializationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Profile("compactor")
@Slf4j
public class PartitionLifecycleService {

    private final PartitionAdminRepository partitionAdminRepository;
    private final PartitioningProperties partitioningProperties;
    private volatile AggregateRollupService aggregateRollupService;
    private volatile MarketBucketMaterializationService marketBucketMaterializationService;

    public PartitionLifecycleService(PartitionAdminRepository partitionAdminRepository,
                                     PartitioningProperties partitioningProperties) {
        this.partitionAdminRepository = partitionAdminRepository;
        this.partitioningProperties = partitioningProperties;
    }

    @Autowired(required = false)
    public void setAggregateRollupService(AggregateRollupService aggregateRollupService) {
        this.aggregateRollupService = aggregateRollupService;
    }

    @Autowired(required = false)
    public void setMarketBucketMaterializationService(MarketBucketMaterializationService marketBucketMaterializationService) {
        this.marketBucketMaterializationService = marketBucketMaterializationService;
    }

    public boolean isPartitionCompactionEnabled() {
        return partitioningProperties.isEnabled()
                && partitioningProperties.getMode() == PartitioningMode.PARTITION_DROP;
    }

    public boolean isDryRun() {
        return partitioningProperties.isDryRun();
    }

    public void ensureForwardPartitions(Instant now) {
        if (!partitioningProperties.isEnabled()) {
            return;
        }
        Instant safeNow = now == null ? Instant.now() : now;
        LocalDate today = UtcDayBucket.utcDay(safeNow);
        int precreateDays = Math.max(0, partitioningProperties.getPrecreateDays());
        ensureForTarget(partitioningProperties.getMarketSnapshotParentTable(), today, precreateDays);
        ensureForTarget(partitioningProperties.getAhSnapshotParentTable(), today, precreateDays);
        ensureForTarget(partitioningProperties.getBzSnapshotParentTable(), today, precreateDays);
    }

    public PartitionRetentionReport executeRawSnapshotRetention(Instant now) {
        RetentionTarget target = new RetentionTarget(
                partitioningProperties.getMarketSnapshotParentTable(),
                Math.max(0, partitioningProperties.getMarketSnapshotRetentionDays())
        );
        return executeRetention(now, List.of(target));
    }

    public PartitionRetentionReport executeAggregateRetention(Instant now) {
        List<RetentionTarget> targets = List.of(
                new RetentionTarget(
                        partitioningProperties.getAhSnapshotParentTable(),
                        Math.max(0, partitioningProperties.getAhSnapshotRetentionDays())
                ),
                new RetentionTarget(
                        partitioningProperties.getBzSnapshotParentTable(),
                        Math.max(0, partitioningProperties.getBzSnapshotRetentionDays())
                )
        );
        return executeRetention(now, targets);
    }

    private PartitionRetentionReport executeRetention(Instant now, List<RetentionTarget> targets) {
        if (!isPartitionCompactionEnabled()) {
            return PartitionRetentionReport.empty(partitioningProperties.isDryRun());
        }
        Instant safeNow = now == null ? Instant.now() : now;
        LocalDate today = UtcDayBucket.utcDay(safeNow);
        boolean dryRun = partitioningProperties.isDryRun();
        String schemaName = partitioningProperties.getSchemaName();

        int scannedPartitions = 0;
        int droppedPartitions = 0;
        int wouldDropPartitions = 0;
        boolean partitionedTargetsDetected = false;
        List<String> messages = new ArrayList<>();
        Map<String, Integer> droppedByParent = new LinkedHashMap<>();
        Map<String, Integer> wouldDropByParent = new LinkedHashMap<>();

        for (RetentionTarget target : targets) {
            if (target.retentionDays() <= 0) {
                messages.add("skip " + target.parentTableName() + ": retentionDays<=0");
                continue;
            }

            String parentTable = target.parentTableName();
            if (!partitionAdminRepository.isTablePartitioned(schemaName, parentTable)) {
                messages.add("skip " + parentTable + ": table not partitioned");
                continue;
            }
            partitionedTargetsDetected = true;

            LocalDate oldestKeptDay = today.minusDays(target.retentionDays() - 1L);
            List<String> partitions = partitionAdminRepository.listChildPartitions(schemaName, parentTable);
            scannedPartitions += partitions.size();

            for (String partitionTable : partitions) {
                LocalDate partitionDay = parseDaySuffix(parentTable, partitionTable);
                if (partitionDay == null || !partitionDay.isBefore(oldestKeptDay)) {
                    continue;
                }
                if (!isEligibleForDrop(parentTable, partitionDay, messages)) {
                    continue;
                }
                if (dryRun) {
                    wouldDropPartitions++;
                    wouldDropByParent.merge(parentTable, 1, Integer::sum);
                    continue;
                }
                AggregateRollupService rollupService = this.aggregateRollupService;
                if (rollupService != null && partitionDay != null) {
                    rollupService.rollupDailyForTable(parentTable, partitionDay);
                }
                partitionAdminRepository.dropTableIfExists(schemaName, partitionTable);
                droppedPartitions++;
                droppedByParent.merge(parentTable, 1, Integer::sum);
            }
        }

        return new PartitionRetentionReport(
                scannedPartitions,
                droppedPartitions,
                wouldDropPartitions,
                dryRun,
                partitionedTargetsDetected,
                Map.copyOf(droppedByParent),
                Map.copyOf(wouldDropByParent),
                List.copyOf(messages)
        );
    }

    private boolean isEligibleForDrop(String parentTable, LocalDate partitionDay, List<String> messages) {
        if (!isAggregateParent(parentTable)) {
            return true;
        }

        MarketBucketMaterializationService materializationService = this.marketBucketMaterializationService;
        if (materializationService == null || !materializationService.isEnabled()) {
            messages.add("skip " + partitionName(parentTable, partitionDay) + ": rollup materialization unavailable");
            return false;
        }
        if (!materializationService.isAggregatePartitionMaterialized(parentTable, partitionDay)) {
            messages.add("skip " + partitionName(parentTable, partitionDay) + ": aggregate buckets not fully materialized");
            return false;
        }
        return true;
    }

    private boolean isAggregateParent(String parentTable) {
        if (parentTable == null || parentTable.isBlank()) {
            return false;
        }
        return parentTable.equalsIgnoreCase(partitioningProperties.getAhSnapshotParentTable())
                || parentTable.equalsIgnoreCase(partitioningProperties.getBzSnapshotParentTable());
    }

    private void ensureForTarget(String parentTable, LocalDate today, int precreateDays) {
        String schemaName = partitioningProperties.getSchemaName();
        if (!partitionAdminRepository.isTablePartitioned(schemaName, parentTable)) {
            return;
        }
        for (int dayOffset = 0; dayOffset <= precreateDays; dayOffset++) {
            LocalDate day = today.plusDays(dayOffset);
            long from = UtcDayBucket.startEpochMillis(day);
            long to = UtcDayBucket.endEpochMillis(day);
            String partitionName = partitionName(parentTable, day);
            try {
                partitionAdminRepository.ensureDailyRangePartition(
                        schemaName,
                        parentTable,
                        partitionName,
                        from,
                        to
                );
            } catch (Exception e) {
                log.warn("Failed to ensure partition {} for parent {}: {}",
                        partitionName,
                        parentTable,
                        e.toString());
            }
        }
    }

    private String partitionName(String parentTable, LocalDate day) {
        return parentTable
                + "_"
                + String.format("%04d_%02d_%02d", day.getYear(), day.getMonthValue(), day.getDayOfMonth());
    }

    private LocalDate parseDaySuffix(String parentTable, String partitionTable) {
        String pattern = "^" + Pattern.quote(parentTable) + "_(\\d{4})_(\\d{2})_(\\d{2})$";
        Matcher matcher = Pattern.compile(pattern).matcher(partitionTable);
        if (!matcher.matches()) {
            return null;
        }
        try {
            int year = Integer.parseInt(matcher.group(1));
            int month = Integer.parseInt(matcher.group(2));
            int day = Integer.parseInt(matcher.group(3));
            return LocalDate.of(year, month, day);
        } catch (Exception ignored) {
            return null;
        }
    }

    private record RetentionTarget(String parentTableName, int retentionDays) {
    }
}
