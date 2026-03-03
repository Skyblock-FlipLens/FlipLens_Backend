package com.skyblockflipper.backend.service.market.partitioning;

import com.skyblockflipper.backend.repository.PartitionAdminRepository;
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
}
