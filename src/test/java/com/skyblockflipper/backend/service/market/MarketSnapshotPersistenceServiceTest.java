package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
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

@SpringBootTest
class MarketSnapshotPersistenceServiceTest {

    @Autowired
    private MarketSnapshotPersistenceService marketSnapshotPersistenceService;

    @Autowired
    private MarketSnapshotRepository marketSnapshotRepository;

    @BeforeEach
    void clean() {
        marketSnapshotRepository.deleteAll();
    }

    @Test
    void saveAndReadLatestRoundTripsSnapshot() {
        MarketSnapshot snapshot = new MarketSnapshot(
                Instant.parse("2026-02-15T12:30:00Z"),
                List.of(),
                Map.of("ENCHANTED_DIAMOND", new BazaarMarketRecord("ENCHANTED_DIAMOND", 10.0, 9.5, 100, 90, 1000, 900, 4, 3))
        );

        marketSnapshotPersistenceService.save(snapshot);

        MarketSnapshot latest = marketSnapshotPersistenceService.latest().orElseThrow();
        assertEquals(snapshot.snapshotTimestamp(), latest.snapshotTimestamp());
        assertEquals(1, latest.bazaarProducts().size());
        assertTrue(latest.bazaarProducts().containsKey("ENCHANTED_DIAMOND"));
    }

    @Test
    void asOfReturnsFloorSnapshot() {
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse("2026-02-15T12:00:00Z"), List.of(), Map.of()
        ));
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse("2026-02-15T12:01:00Z"), List.of(), Map.of()
        ));

        MarketSnapshot asOf = marketSnapshotPersistenceService
                .asOf(Instant.parse("2026-02-15T12:00:30Z"))
                .orElseThrow();

        assertEquals(Instant.parse("2026-02-15T12:00:00Z"), asOf.snapshotTimestamp());
    }
}
