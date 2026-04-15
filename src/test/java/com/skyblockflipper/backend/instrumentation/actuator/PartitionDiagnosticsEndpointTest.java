package com.skyblockflipper.backend.instrumentation.actuator;

import com.skyblockflipper.backend.repository.PartitionAdminRepository;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningMode;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PartitionDiagnosticsEndpointTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-03-21T10:15:30Z"), ZoneOffset.UTC);

    @Test
    void partitionDiagnosticsClassifiesRetainedFutureStaleAndDefaultPartitions() {
        PartitionAdminRepository repository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setMode(PartitioningMode.PARTITION_DROP);
        properties.setPrecreateDays(14);
        properties.setMarketSnapshotRetentionDays(7);
        properties.setAhSnapshotRetentionDays(7);
        properties.setBzSnapshotRetentionDays(7);

        when(repository.isTablePartitioned("public", "market_snapshot")).thenReturn(true);
        when(repository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(true);
        when(repository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(true);
        when(repository.listChildPartitions("public", "market_snapshot"))
                .thenReturn(List.of(
                        "market_snapshot_2026_03_21",
                        "market_snapshot_2026_03_22",
                        "market_snapshot_default"
                ));
        when(repository.listChildPartitions("public", "ah_item_snapshot"))
                .thenReturn(List.of(
                        "ah_item_snapshot_2026_03_14",
                        "ah_item_snapshot_2026_03_15",
                        "ah_item_snapshot_2026_03_21",
                        "ah_item_snapshot_2026_03_22",
                        "ah_item_snapshot_default"
                ));
        when(repository.listChildPartitions("public", "bz_item_snapshot"))
                .thenReturn(List.of(
                        "bz_item_snapshot_2026_03_15",
                        "bz_item_snapshot_2026_03_23",
                        "bz_item_snapshot_weird",
                        "bz_item_snapshot_default"
                ));

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties, FIXED_CLOCK);

        PartitionDiagnosticsDto response = endpoint.partitionDiagnostics(null);

        assertEquals("OK", response.status());
        assertEquals(LocalDate.parse("2026-03-21"), response.todayUtc());
        assertEquals(14, response.precreateDays());
        assertEquals(12, response.totalPartitions());
        assertEquals(4, response.totalRetainedPartitions());
        assertEquals(3, response.totalFuturePartitions());
        assertEquals(1, response.totalOutOfRetentionPartitions());
        assertEquals(3, response.totalDefaultPartitions());
        assertEquals(1, response.totalUnclassifiedPartitions());
        assertEquals(false, response.partitionsIncluded());
        assertEquals(3, response.targets().size());
        assertEquals(3, response.targets().get(0).partitionCount());
        assertEquals(1, response.targets().get(0).retainedPartitionCount());
        assertEquals(1, response.targets().get(0).futurePartitionCount());
        assertEquals(1, response.targets().get(0).defaultPartitionCount());
        assertTrue(response.targets().get(0).partitions().isEmpty());
        assertEquals(LocalDate.parse("2026-03-15"), response.targets().get(0).oldestKeptDayUtc());
        assertEquals(1, response.targets().get(1).outOfRetentionPartitionCount());
        assertEquals(LocalDate.parse("2026-03-14"), response.targets().get(1).oldestPartitionDayUtc());
        assertEquals(LocalDate.parse("2026-03-22"), response.targets().get(1).newestPartitionDayUtc());
        assertEquals(1, response.targets().get(2).unclassifiedPartitionCount());
    }

    @Test
    void partitionDiagnosticsIncludesPartitionNamesWhenRequested() {
        PartitionAdminRepository repository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);

        when(repository.isTablePartitioned("public", "market_snapshot")).thenReturn(true);
        when(repository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(false);
        when(repository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(false);
        when(repository.listChildPartitions("public", "market_snapshot"))
                .thenReturn(List.of("market_snapshot_2026_03_20", "market_snapshot_2026_03_21"));

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties, FIXED_CLOCK);

        PartitionDiagnosticsDto response = endpoint.partitionDiagnostics(true);

        assertEquals(true, response.partitionsIncluded());
        assertEquals(List.of("market_snapshot_2026_03_20", "market_snapshot_2026_03_21"),
                response.targets().getFirst().partitions());
    }

    @Test
    void partitionDiagnosticsReturnsDisabledStatusWhenPartitioningIsOff() {
        PartitionAdminRepository repository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(false);

        when(repository.isTablePartitioned("public", "market_snapshot")).thenReturn(false);
        when(repository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(false);
        when(repository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(false);

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties, FIXED_CLOCK);

        PartitionDiagnosticsDto response = endpoint.partitionDiagnostics(null);

        assertEquals("DISABLED", response.status());
        assertEquals(0, response.totalPartitions());
    }

    @Test
    void partitionDiagnosticsAutowiredConstructorReportsNoPartitionedTargets() {
        PartitionAdminRepository repository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setMode(null);

        when(repository.isTablePartitioned("public", "market_snapshot")).thenReturn(false);
        when(repository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(false);
        when(repository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(false);

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties);

        PartitionDiagnosticsDto response = endpoint.partitionDiagnostics(null);

        assertEquals("NO_PARTITIONED_TARGETS", response.status());
        assertNull(response.mode());
        assertEquals(0, response.totalPartitions());
        assertNotNull(response.todayUtc());
    }

    @Test
    void partitionDiagnosticsReturnsErrorStatusWhenRepositoryInspectionFails() {
        PartitionAdminRepository repository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setMode(null);
        properties.setMarketSnapshotRetentionDays(0);
        properties.setAhSnapshotRetentionDays(0);
        properties.setBzSnapshotRetentionDays(0);

        when(repository.isTablePartitioned("public", "market_snapshot"))
                .thenThrow(new IllegalStateException());
        when(repository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(false);
        when(repository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(false);

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties, FIXED_CLOCK);

        PartitionDiagnosticsDto response = endpoint.partitionDiagnostics(null);

        assertEquals("ERROR", response.status());
        assertNull(response.mode());
        assertNull(response.targets().getFirst().oldestKeptDayUtc());
        assertEquals("IllegalStateException", response.targets().getFirst().error());
    }

    @Test
    void partitionDiagnosticsTreatsBlankAndInvalidPartitionsAsUnclassified() {
        PartitionAdminRepository repository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setMarketSnapshotRetentionDays(0);

        when(repository.isTablePartitioned("public", "market_snapshot")).thenReturn(true);
        when(repository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(false);
        when(repository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(false);
        when(repository.listChildPartitions("public", "market_snapshot"))
                .thenReturn(Arrays.asList(
                        null,
                        "   ",
                        "market_snapshot_2026_02_30",
                        "market_snapshot_default",
                        "market_snapshot_2026_03_21"
                ));

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties, FIXED_CLOCK);

        PartitionDiagnosticsDto response = endpoint.partitionDiagnostics(null);
        PartitionDiagnosticsDto.TargetDiagnosticsDto marketTarget = response.targets().getFirst();

        assertEquals("OK", response.status());
        assertNull(marketTarget.oldestKeptDayUtc());
        assertEquals(5, marketTarget.partitionCount());
        assertEquals(1, marketTarget.retainedPartitionCount());
        assertEquals(1, marketTarget.defaultPartitionCount());
        assertEquals(3, marketTarget.unclassifiedPartitionCount());
        assertEquals(LocalDate.parse("2026-03-21"), marketTarget.oldestPartitionDayUtc());
        assertEquals(LocalDate.parse("2026-03-21"), marketTarget.newestPartitionDayUtc());
    }
}
