package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
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

@SpringBootTest(properties = {
        "config.snapshot.retention.flip-delete-batch-size=2",
        "config.snapshot.retention.flip-delete-batch-pause-millis=0"
})
class MarketSnapshotOrphanChunkDeletionTest {

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
    void compactSnapshotsDeletesOrphanFlipsAcrossMultipleIdChunks() {
        Instant now = Instant.parse("2026-02-17T12:00:00Z");

        // Same minute slot (>90s old): first is kept, second is deleted.
        saveAt("2026-02-17T11:56:00Z");
        saveAt("2026-02-17T11:56:30Z");

        // Keep slot control snapshot.
        saveAt("2026-02-17T11:57:00Z");

        saveFlipAt("2026-02-17T11:56:00Z", "KEPT_TS");
        for (int i = 0; i < 5; i++) {
            saveFlipAt("2026-02-17T11:56:30Z", "ORPHAN_" + i);
        }

        MarketSnapshotPersistenceService.SnapshotCompactionResult result = marketSnapshotPersistenceService.compactSnapshots(now);

        assertEquals(3, result.scannedCount());
        assertEquals(1, result.deletedCount());
        assertEquals(2, result.keptCount());

        List<Flip> remainingFlips = flipRepository.findAll();
        assertEquals(1, remainingFlips.size());
        assertTrue(remainingFlips.stream().anyMatch(f -> "KEPT_TS".equals(f.getResultItemId())));
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
