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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MarketTimescaleFeatureServiceTest {

    @Test
    void dailyFeaturesUseFirstSnapshotInEachEpochDayBucket() {
        BzItemSnapshotRepository repository = mock(BzItemSnapshotRepository.class);
        MarketTimescaleFeatureService featureService = new MarketTimescaleFeatureService(repository, new MarketItemKeyService());

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
        Instant evaluationTs = Instant.parse("2026-02-18T12:00:00Z");
        long evaluationMillis = evaluationTs.toEpochMilli();
        long expectedMicroStart = evaluationMillis - 60_000L;
        long expectedMacroStart = (Math.floorDiv(evaluationTs.getEpochSecond(), 86_400L) - 32L) * 86_400_000L;
        Set<String> expectedProductIds = Set.of("ENCHANTED_DIAMOND");

        when(repository.findBySnapshotTsBetweenAndProductIdInOrderBySnapshotTsAsc(
                eq(expectedMicroStart),
                eq(evaluationMillis),
                eq(expectedProductIds)
        ))
                .thenReturn(microWindow);
        when(repository.findFirstSnapshotTsPerDayBetween(eq(expectedMacroStart), eq(evaluationMillis)))
                .thenReturn(dailyAnchors);
        when(repository.findBySnapshotTsInAndProductIdInOrderBySnapshotTsAsc(
                eq(dailyAnchors),
                eq(expectedProductIds)
        ))
                .thenReturn(dailyHistory);

        FlipScoreFeatureSet featureSet = featureService.computeFor(latest);
        FlipScoreFeatureSet.ItemTimescaleFeatures itemFeatures = featureSet.get("ENCHANTED_DIAMOND");

        assertNotNull(itemFeatures);
        assertEquals(Math.log(120D / 100D), itemFeatures.macroReturn1d(), 1e-9);
    }

    @Test
    void constructorRejectsNullDependencies() {
        BzItemSnapshotRepository repository = mock(BzItemSnapshotRepository.class);
        assertThrows(NullPointerException.class, () -> new MarketTimescaleFeatureService(null, new MarketItemKeyService()));
        assertThrows(NullPointerException.class, () -> new MarketTimescaleFeatureService(repository, null));
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
