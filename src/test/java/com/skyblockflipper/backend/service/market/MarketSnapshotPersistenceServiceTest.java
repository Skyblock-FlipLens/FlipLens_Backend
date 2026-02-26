package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import org.junit.jupiter.api.AfterEach;
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

    @Autowired
    private FlipRepository flipRepository;

    @BeforeEach
    void clean() {
        flipRepository.deleteAll();
        marketSnapshotRepository.deleteAll();
    }

    @AfterEach
    void cleanAfterEach() {
        clean();
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

    @Test
    void betweenReturnsSnapshotsInsideInclusiveRangeOrderedByTimestamp() {
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse("2026-02-15T11:59:59Z"), List.of(), Map.of()
        ));
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse("2026-02-15T12:00:00Z"), List.of(), Map.of()
        ));
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse("2026-02-15T12:00:30Z"), List.of(), Map.of()
        ));
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse("2026-02-15T12:01:01Z"), List.of(), Map.of()
        ));

        List<MarketSnapshot> snapshots = marketSnapshotPersistenceService.between(
                Instant.parse("2026-02-15T12:00:00Z"),
                Instant.parse("2026-02-15T12:01:00Z")
        );

        assertEquals(2, snapshots.size());
        assertEquals(Instant.parse("2026-02-15T12:00:00Z"), snapshots.get(0).snapshotTimestamp());
        assertEquals(Instant.parse("2026-02-15T12:00:30Z"), snapshots.get(1).snapshotTimestamp());
    }

    @Test
    void compactSnapshotsAppliesRetentionTiersAndKeepsDailyFirstSnapshot() {
        Instant now = Instant.parse("2026-02-17T12:00:00Z");

        // Younger than 90s: keep all.
        saveAt("2026-02-17T11:59:10Z");
        saveAt("2026-02-17T11:59:20Z");

        // Minute tier (>90s to <=30m): keep one per minute slot.
        saveAt("2026-02-17T11:57:05Z");
        saveAt("2026-02-17T11:57:10Z"); // same minute slot -> delete
        saveAt("2026-02-17T11:56:20Z");

        // Two-hour tier (>30m to <=12h): keep one per 2h slot.
        saveAt("2026-02-17T10:58:20Z");
        saveAt("2026-02-17T11:00:00Z"); // same 10:00-11:59 slot -> delete
        saveAt("2026-02-17T08:15:00Z");

        // Daily tier (>12h): keep first snapshot per epochDay.
        saveAt("2026-02-16T00:00:05Z"); // keep for 2026-02-16
        saveAt("2026-02-16T16:00:00Z"); // delete
        saveAt("2026-02-15T00:00:10Z"); // keep for 2026-02-15
        saveAt("2026-02-15T15:00:00Z"); // delete

        MarketSnapshotPersistenceService.SnapshotCompactionResult result = marketSnapshotPersistenceService.compactSnapshots(now);

        assertEquals(10, result.scannedCount());
        assertEquals(4, result.deletedCount());
        assertEquals(6, result.keptCount());

        List<MarketSnapshot> survivors = marketSnapshotRepository.findAll().stream()
                .map(entity -> Instant.ofEpochMilli(entity.getSnapshotTimestampEpochMillis()))
                .sorted()
                .map(ts -> new MarketSnapshot(ts, List.of(), Map.of()))
                .toList();

        assertEquals(8, survivors.size());
        assertTrue(survivors.stream().anyMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-17T11:59:10Z"))));
        assertTrue(survivors.stream().anyMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-17T11:59:20Z"))));
        assertTrue(survivors.stream().anyMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-17T11:57:05Z"))));
        assertTrue(survivors.stream().noneMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-17T11:57:10Z"))));
        assertTrue(survivors.stream().anyMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-17T10:58:20Z"))));
        assertTrue(survivors.stream().noneMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-17T11:00:00Z"))));
        assertTrue(survivors.stream().anyMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-16T00:00:05Z"))));
        assertTrue(survivors.stream().noneMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-16T16:00:00Z"))));
        assertTrue(survivors.stream().anyMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-15T00:00:10Z"))));
        assertTrue(survivors.stream().noneMatch(s -> s.snapshotTimestamp().equals(Instant.parse("2026-02-15T15:00:00Z"))));
    }

    @Test
    void compactSnapshotsDeletesFlipsOnlyWhenTheirSnapshotTimestampNoLongerExists() {
        Instant now = Instant.parse("2026-02-17T12:00:00Z");

        saveAt("2026-02-17T11:57:00Z");
        saveAt("2026-02-17T11:57:00Z");
        saveAt("2026-02-17T11:56:00Z");
        saveAt("2026-02-17T11:56:30Z");

        saveFlipAt("2026-02-17T11:57:00Z", "DUPLICATE_TS_KEEP");
        saveFlipAt("2026-02-17T11:56:00Z", "KEPT_SLOT");
        saveFlipAt("2026-02-17T11:56:30Z", "DELETED_SLOT");

        MarketSnapshotPersistenceService.SnapshotCompactionResult result = marketSnapshotPersistenceService.compactSnapshots(now);

        assertEquals(4, result.scannedCount());
        assertEquals(2, result.deletedCount());
        assertEquals(2, result.keptCount());

        List<Flip> remainingFlips = flipRepository.findAll();
        assertEquals(2, remainingFlips.size());
        assertTrue(remainingFlips.stream().anyMatch(f -> "DUPLICATE_TS_KEEP".equals(f.getResultItemId())));
        assertTrue(remainingFlips.stream().anyMatch(f -> "KEPT_SLOT".equals(f.getResultItemId())));
        assertTrue(remainingFlips.stream().noneMatch(f -> "DELETED_SLOT".equals(f.getResultItemId())));
    }

    private void saveAt(String timestamp) {
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse(timestamp),
                List.of(),
                Map.of()
        ));
    }

    private void saveFlipAt(String snapshotTimestamp, String resultItemId) {
        Flip flip = new Flip(null, FlipType.BAZAAR, List.of(), resultItemId, List.of());
        flip.setSnapshotTimestampEpochMillis(Instant.parse(snapshotTimestamp).toEpochMilli());
        flipRepository.save(flip);
    }
}
