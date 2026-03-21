package com.skyblockflipper.backend.instrumentation.actuator;

import com.skyblockflipper.backend.repository.PartitionAdminRepository;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningMode;
import com.skyblockflipper.backend.service.market.partitioning.PartitioningProperties;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PartitionDiagnosticsEndpointTest {

    @Test
    void partitionDiagnosticsReturnsPerTargetCountsAndTotal() {
        PartitionAdminRepository repository = mock(PartitionAdminRepository.class);
        PartitioningProperties properties = new PartitioningProperties();
        properties.setEnabled(true);
        properties.setMode(PartitioningMode.PARTITION_DROP);

        when(repository.isTablePartitioned("public", "market_snapshot")).thenReturn(true);
        when(repository.isTablePartitioned("public", "ah_item_snapshot")).thenReturn(true);
        when(repository.isTablePartitioned("public", "bz_item_snapshot")).thenReturn(false);
        when(repository.listChildPartitions("public", "market_snapshot"))
                .thenReturn(List.of("market_snapshot_2026_03_20", "market_snapshot_2026_03_21"));
        when(repository.listChildPartitions("public", "ah_item_snapshot"))
                .thenReturn(List.of("ah_item_snapshot_2026_03_21"));

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties);

        PartitionDiagnosticsDto response = endpoint.partitionDiagnostics(null);

        assertEquals("OK", response.status());
        assertEquals(3, response.totalPartitions());
        assertEquals(false, response.partitionsIncluded());
        assertEquals(3, response.targets().size());
        assertEquals(2, response.targets().get(0).partitionCount());
        assertTrue(response.targets().get(0).partitions().isEmpty());
        assertEquals(1, response.targets().get(1).partitionCount());
        assertEquals(0, response.targets().get(2).partitionCount());
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

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties);

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

        PartitionDiagnosticsEndpoint endpoint = new PartitionDiagnosticsEndpoint(repository, properties);

        PartitionDiagnosticsDto response = endpoint.partitionDiagnostics(null);

        assertEquals("DISABLED", response.status());
        assertEquals(0, response.totalPartitions());
    }
}
