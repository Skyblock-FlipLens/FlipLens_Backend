package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class FlipStorageBackfillService {

    private final FlipRepository flipRepository;
    private final UnifiedFlipStorageService unifiedFlipStorageService;

    public FlipStorageBackfillService(FlipRepository flipRepository,
                                      UnifiedFlipStorageService unifiedFlipStorageService) {
        this.flipRepository = flipRepository;
        this.unifiedFlipStorageService = unifiedFlipStorageService;
    }

    @Transactional
    public BackfillResult backfillLatestLegacySnapshot() {
        Optional<Long> latestLegacy = flipRepository.findMaxSnapshotTimestampEpochMillis();
        if (latestLegacy.isEmpty()) {
            return new BackfillResult(null, 0, false, "No legacy snapshot found.");
        }
        return backfillSnapshot(latestLegacy.get());
    }

    @Transactional
    public BackfillResult backfillSnapshot(long snapshotEpochMillis) {
        List<Flip> flips = flipRepository.findAllBySnapshotTimestampEpochMillis(snapshotEpochMillis);
        if (flips.isEmpty()) {
            return new BackfillResult(snapshotEpochMillis, 0, false, "No flips found for snapshot.");
        }

        unifiedFlipStorageService.persistSnapshotFlipsForced(flips, Instant.ofEpochMilli(snapshotEpochMillis));
        return new BackfillResult(snapshotEpochMillis, flips.size(), true, "Backfill completed.");
    }

    public record BackfillResult(
            Long snapshotEpochMillis,
            int processedFlipCount,
            boolean success,
            String message
    ) {
    }
}
