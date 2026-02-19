package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "config.snapshot.retention.raw-window-seconds=30",
        "config.snapshot.retention.minute-tier-upper-seconds=120",
        "config.snapshot.retention.two-hour-tier-upper-seconds=3600",
        "config.snapshot.retention.minute-interval-seconds=30",
        "config.snapshot.retention.two-hour-interval-seconds=600"
})
class MarketSnapshotRetentionConfigurationTest {

    @Autowired
    private MarketSnapshotPersistenceService marketSnapshotPersistenceService;

    @Autowired
    private MarketSnapshotRepository marketSnapshotRepository;

    @BeforeEach
    void clean() {
        marketSnapshotRepository.deleteAll();
    }

    @Test
    void compactSnapshotsUsesConfiguredMinuteIntervalSlots() {
        Instant now = Instant.parse("2026-02-17T12:00:00Z");

        saveAt("2026-02-17T11:59:10Z");
        saveAt("2026-02-17T11:59:20Z");
        saveAt("2026-02-17T11:58:50Z");
        saveAt("2026-02-17T11:58:55Z");

        MarketSnapshotPersistenceService.SnapshotCompactionResult result = marketSnapshotPersistenceService.compactSnapshots(now);

        assertEquals(4, result.scannedCount());
        assertEquals(2, result.deletedCount());
        assertEquals(2, result.keptCount());

        List<Instant> survivors = marketSnapshotRepository.findAll().stream()
                .map(entity -> Instant.ofEpochMilli(entity.getSnapshotTimestampEpochMillis()))
                .sorted()
                .toList();

        assertEquals(2, survivors.size());
        assertTrue(survivors.contains(Instant.parse("2026-02-17T11:58:50Z")));
        assertTrue(survivors.contains(Instant.parse("2026-02-17T11:59:10Z")));
    }

    private void saveAt(String timestamp) {
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse(timestamp),
                List.of(),
                Map.of()
        ));
    }
}
