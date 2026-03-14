package com.skyblockflipper.backend.service.market.partitioning;

import com.skyblockflipper.backend.repository.PartitionAdminRepository;
import com.skyblockflipper.backend.service.market.rollup.MarketBucketMaterializationService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionLifecycleServiceTest {

    @Test
    void executeRawSnapshotRetentionDryRunCountsWouldDropPartitions() {
        PartitionAdminRepository adminRepository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setMode(PartitioningMode.PARTITION_DROP);
        properties.setDryRun(true);
        properties.setMarketSnapshotRetentionDays(7);
        properties.setMarketSnapshotParentTable("market_snapshot");

        when(adminRepository.isTablePartitioned("public", "market_snapshot")).thenReturn(true);
        when(adminRepository.listChildPartitions("public", "market_snapshot")).thenReturn(List.of(
                "market_snapshot_2026_03_01",
                "market_snapshot_2026_03_09",
                "market_snapshot_default"
        ));

        PartitionLifecycleService service = new PartitionLifecycleService(adminRepository, properties);
        PartitionRetentionReport report = service.executeRawSnapshotRetention(Instant.parse("2026-03-10T12:00:00Z"));

        assertTrue(report.dryRun());
        assertTrue(report.partitionedTargetsDetected());
        assertEquals(3, report.scannedPartitions());
        assertEquals(0, report.droppedPartitions());
        assertEquals(1, report.wouldDropPartitions());
        verify(adminRepository, never()).dropTableIfExists(eq("public"), eq("market_snapshot_2026_03_01"));
    }

    @Test
    void ensureForwardPartitionsPrecreatesConfiguredDayWindow() {
        PartitionAdminRepository adminRepository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setPrecreateDays(1);
        properties.setMarketSnapshotParentTable("market_snapshot");
        properties.setAhSnapshotParentTable("ah_item_snapshot");
        properties.setBzSnapshotParentTable("bz_item_snapshot");

        when(adminRepository.isTablePartitioned("public", "market_snapshot")).thenReturn(true);
        when(adminRepository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(false);
        when(adminRepository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(false);

        PartitionLifecycleService service = new PartitionLifecycleService(adminRepository, properties);
        service.ensureForwardPartitions(Instant.parse("2026-03-10T12:00:00Z"));

        verify(adminRepository, times(2)).ensureDailyRangePartition(
                eq("public"),
                eq("market_snapshot"),
                org.mockito.ArgumentMatchers.startsWith("market_snapshot_"),
                anyLong(),
                anyLong()
        );
        verify(adminRepository, never()).ensureDailyRangePartition(
                eq("public"),
                eq("ah_item_snapshot"),
                org.mockito.ArgumentMatchers.anyString(),
                anyLong(),
                anyLong()
        );
    }

    @Test
    void executeRawSnapshotRetentionReturnsEmptyWhenDisabled() {
        PartitionAdminRepository adminRepository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(false);

        PartitionLifecycleService service = new PartitionLifecycleService(adminRepository, properties);
        PartitionRetentionReport report = service.executeRawSnapshotRetention(Instant.parse("2026-03-10T12:00:00Z"));

        assertFalse(report.partitionedTargetsDetected());
        assertEquals(0, report.scannedPartitions());
        assertEquals(0, report.droppedPartitions());
    }

    @Test
    void executeAggregateRetentionSkipsDropWhenMaterializationIsIncomplete() {
        PartitionAdminRepository adminRepository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setMode(PartitioningMode.PARTITION_DROP);
        properties.setDryRun(false);
        properties.setAhSnapshotRetentionDays(1);
        properties.setAhSnapshotParentTable("ah_item_snapshot");

        when(adminRepository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(true);
        when(adminRepository.listChildPartitions("public", "ah_item_snapshot")).thenReturn(List.of("ah_item_snapshot_2026_03_08"));

        MarketBucketMaterializationService materializationService = mock(MarketBucketMaterializationService.class);
        when(materializationService.isEnabled()).thenReturn(true);
        when(materializationService.isAggregatePartitionMaterialized("ah_item_snapshot", java.time.LocalDate.parse("2026-03-08")))
                .thenReturn(false);

        PartitionLifecycleService service = new PartitionLifecycleService(adminRepository, properties);
        service.setMarketBucketMaterializationService(materializationService);

        PartitionRetentionReport report = service.executeAggregateRetention(Instant.parse("2026-03-10T12:00:00Z"));

        assertEquals(0, report.droppedPartitions());
        assertTrue(report.messages().stream().anyMatch(message -> message.contains("not fully materialized")));
        verify(adminRepository, never()).dropTableIfExists("public", "ah_item_snapshot_2026_03_08");
    }

    @Test
    void executeAggregateRetentionDropsPartitionAfterMaterializationAndDailyRollup() {
        PartitionAdminRepository adminRepository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setMode(PartitioningMode.PARTITION_DROP);
        properties.setDryRun(false);
        properties.setBzSnapshotRetentionDays(1);
        properties.setBzSnapshotParentTable("bz_item_snapshot");

        when(adminRepository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(true);
        when(adminRepository.listChildPartitions("public", "bz_item_snapshot")).thenReturn(List.of("bz_item_snapshot_2026_03_08"));

        MarketBucketMaterializationService materializationService = mock(MarketBucketMaterializationService.class);
        when(materializationService.isEnabled()).thenReturn(true);
        when(materializationService.isAggregatePartitionMaterialized("bz_item_snapshot", java.time.LocalDate.parse("2026-03-08")))
                .thenReturn(true);

        AggregateRollupService aggregateRollupService = mock(AggregateRollupService.class);

        PartitionLifecycleService service = new PartitionLifecycleService(adminRepository, properties);
        service.setMarketBucketMaterializationService(materializationService);
        service.setAggregateRollupService(aggregateRollupService);

        PartitionRetentionReport report = service.executeAggregateRetention(Instant.parse("2026-03-10T12:00:00Z"));

        assertEquals(1, report.droppedPartitions());
        verify(aggregateRollupService).rollupDailyForTable("bz_item_snapshot", java.time.LocalDate.parse("2026-03-08"));
        verify(adminRepository).dropTableIfExists("public", "bz_item_snapshot_2026_03_08");
    }
}
