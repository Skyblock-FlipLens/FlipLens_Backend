package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.service.flipping.FlipScoreFeatureSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MarketTimescaleFeatureServiceTest {

    @Test
    void dailyFeaturesUseFirstSnapshotInEachEpochDayBucket() {
        BzItemSnapshotRepository repository = mock(BzItemSnapshotRepository.class);
        MarketTimescaleFeatureService featureService = new MarketTimescaleFeatureService(repository);

        MarketSnapshot latest = snapshot("2026-02-18T12:00:00Z", 300D);
        List<BzItemSnapshotEntity> microWindow = List.of(
                row("2026-02-18T11:59:00Z", 295D),
                row("2026-02-18T12:00:00Z", 300D)
        );
        List<Long> dailyAnchors = List.of(
                Instant.parse("2026-02-17T00:00:05Z").toEpochMilli(),
                Instant.parse("2026-02-18T00:00:10Z").toEpochMilli()
        );
        List<BzItemSnapshotEntity> dailyHistory = List.of(
                row("2026-02-17T00:00:05Z", 100D),
                row("2026-02-18T00:00:10Z", 120D)
        );
        when(repository.findBySnapshotTsBetweenAndProductIdInOrderBySnapshotTsAsc(any(Long.class), any(Long.class), anyCollection()))
                .thenReturn(microWindow);
        when(repository.findFirstSnapshotTsPerDayBetween(any(Long.class), any(Long.class)))
                .thenReturn(dailyAnchors);
        when(repository.findBySnapshotTsInAndProductIdInOrderBySnapshotTsAsc(anyCollection(), anyCollection()))
                .thenReturn(dailyHistory);

        FlipScoreFeatureSet featureSet = featureService.computeFor(latest);
        FlipScoreFeatureSet.ItemTimescaleFeatures itemFeatures = featureSet.get("ENCHANTED_DIAMOND");

        assertNotNull(itemFeatures);
        assertEquals(Math.log(120D / 100D), itemFeatures.macroReturn1d(), 1e-9);
    }

    private BzItemSnapshotEntity row(String timestamp, double midPrice) {
        double buyPrice = midPrice + 2D;
        double sellPrice = midPrice - 2D;
        return new BzItemSnapshotEntity(
                Instant.parse(timestamp).toEpochMilli(),
                "ENCHANTED_DIAMOND",
                buyPrice,
                sellPrice,
                10_000L,
                10_000L
        );
    }

    private MarketSnapshot snapshot(String timestamp, double midPrice) {
        double buyPrice = midPrice + 2D;
        double sellPrice = midPrice - 2D;
        BazaarMarketRecord record = new BazaarMarketRecord(
                "ENCHANTED_DIAMOND",
                buyPrice,
                sellPrice,
                10_000L,
                10_000L,
                840_000L,
                840_000L,
                80,
                80
        );
        return new MarketSnapshot(
                Instant.parse(timestamp),
                List.of(),
                Map.of("ENCHANTED_DIAMOND", record)
        );
    }
}
