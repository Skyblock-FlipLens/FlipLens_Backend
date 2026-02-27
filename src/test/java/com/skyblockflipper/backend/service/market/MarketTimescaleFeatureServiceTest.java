package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.BzItemSnapshotRepository;
import com.skyblockflipper.backend.service.flipping.FlipScoreFeatureSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
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
    void computeForReturnsEmptyWhenSnapshotMissingOrKeysCannotBeNormalized() {
        BzItemSnapshotRepository repository = mock(BzItemSnapshotRepository.class);
        MarketTimescaleFeatureService featureService = new MarketTimescaleFeatureService(repository, new MarketItemKeyService());

        FlipScoreFeatureSet fromNull = featureService.computeFor(null);
        FlipScoreFeatureSet fromEmpty = featureService.computeFor(new MarketSnapshot(Instant.now(), List.of(), Map.of()));
        MarketSnapshot invalidIds = new MarketSnapshot(
                Instant.parse("2026-02-18T12:00:00Z"),
                List.of(),
                Map.of("bad", new BazaarMarketRecord(" ", 100D, 99D, 1_000L, 1_000L, 10_000L, 10_000L, 1, 1))
        );
        FlipScoreFeatureSet fromInvalidIds = featureService.computeFor(invalidIds);

        assertTrue(fromNull.byItemId().isEmpty());
        assertTrue(fromEmpty.byItemId().isEmpty());
        assertTrue(fromInvalidIds.byItemId().isEmpty());
        verifyNoInteractions(repository);
    }

    @Test
    void computeForProducesHighConfidenceSignalsWhenHistoryIsDense() {
        BzItemSnapshotRepository repository = mock(BzItemSnapshotRepository.class);
        MarketTimescaleFeatureService featureService = new MarketTimescaleFeatureService(repository, new MarketItemKeyService());
        Instant evaluation = Instant.parse("2026-02-18T12:00:00Z");
        MarketSnapshot latest = snapshot(evaluation.toString(), 300D);

        List<BzItemSnapshotEntity> microRows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            microRows.add(rowAtMillis(evaluation.toEpochMilli() - (60_000L - (i * 6_000L)), "ENCHANTED_DIAMOND", 240D + i));
        }

        List<Long> anchors = List.of(
                Instant.parse("2026-02-11T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-12T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-13T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-14T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-15T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-16T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-17T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-18T00:00:00Z").toEpochMilli()
        );
        List<BzItemSnapshotEntity> dailyRows = List.of(
                rowAtMillis(anchors.get(0), "ENCHANTED_DIAMOND", 100D),
                rowAtMillis(anchors.get(1), "ENCHANTED_DIAMOND", 105D),
                rowAtMillis(anchors.get(2), "ENCHANTED_DIAMOND", 110D),
                rowAtMillis(anchors.get(3), "ENCHANTED_DIAMOND", 120D),
                rowAtMillis(anchors.get(4), "ENCHANTED_DIAMOND", 130D),
                rowAtMillis(anchors.get(5), "ENCHANTED_DIAMOND", 125D),
                rowAtMillis(anchors.get(6), "ENCHANTED_DIAMOND", 132D),
                rowAtMillis(anchors.get(7), "ENCHANTED_DIAMOND", 140D)
        );

        Set<String> ids = Set.of("ENCHANTED_DIAMOND");
        long evalMillis = evaluation.toEpochMilli();
        when(repository.findBySnapshotTsBetweenAndProductIdInOrderBySnapshotTsAsc(eq(evalMillis - 60_000L), eq(evalMillis), eq(ids)))
                .thenReturn(microRows);
        when(repository.findFirstSnapshotTsPerDayBetween(eq((Math.floorDiv(evaluation.getEpochSecond(), 86_400L) - 32L) * 86_400_000L), eq(evalMillis)))
                .thenReturn(anchors);
        when(repository.findBySnapshotTsInAndProductIdInOrderBySnapshotTsAsc(eq(anchors), eq(ids)))
                .thenReturn(dailyRows);

        FlipScoreFeatureSet.ItemTimescaleFeatures features = featureService.computeFor(latest).get("ENCHANTED_DIAMOND");

        assertNotNull(features);
        assertEquals(FlipScoreFeatureSet.ConfidenceLevel.HIGH, features.microConfidence());
        assertEquals(FlipScoreFeatureSet.ConfidenceLevel.HIGH, features.macroConfidence());
        assertNotNull(features.microReturn1m());
        assertNotNull(features.microVolatility1m());
        assertNotNull(features.macroVolatility1d());
    }

    @Test
    void computeForMarksStructuralIlliquidityFromLatestSpreadAndTurnover() {
        BzItemSnapshotRepository repository = mock(BzItemSnapshotRepository.class);
        MarketTimescaleFeatureService featureService = new MarketTimescaleFeatureService(repository, new MarketItemKeyService());
        Instant evaluation = Instant.parse("2026-02-18T12:00:00Z");
        BazaarMarketRecord illiquidLatest = new BazaarMarketRecord(
                "ENCHANTED_DIAMOND",
                120D,
                80D,
                100L,
                100L,
                0L,
                0L,
                1,
                1
        );
        MarketSnapshot latest = new MarketSnapshot(evaluation, List.of(), Map.of("ENCHANTED_DIAMOND", illiquidLatest));
        Set<String> ids = Set.of("ENCHANTED_DIAMOND");
        long evalMillis = evaluation.toEpochMilli();
        when(repository.findBySnapshotTsBetweenAndProductIdInOrderBySnapshotTsAsc(eq(evalMillis - 60_000L), eq(evalMillis), eq(ids)))
                .thenReturn(List.of());
        when(repository.findFirstSnapshotTsPerDayBetween(eq((Math.floorDiv(evaluation.getEpochSecond(), 86_400L) - 32L) * 86_400_000L), eq(evalMillis)))
                .thenReturn(List.of());

        FlipScoreFeatureSet.ItemTimescaleFeatures features = featureService.computeFor(latest).get("ENCHANTED_DIAMOND");

        assertNotNull(features);
        assertTrue(features.structurallyIlliquid());
    }

    @Test
    void computeForUsesRecentDailyMedianToMarkStructuralIlliquidity() {
        BzItemSnapshotRepository repository = mock(BzItemSnapshotRepository.class);
        MarketTimescaleFeatureService featureService = new MarketTimescaleFeatureService(repository, new MarketItemKeyService());
        Instant evaluation = Instant.parse("2026-02-18T12:00:00Z");
        BazaarMarketRecord latest = new BazaarMarketRecord(
                "ENCHANTED_DIAMOND",
                101D,
                100D,
                20_000L,
                20_000L,
                1_680_000L,
                1_680_000L,
                10,
                10
        );
        MarketSnapshot snapshot = new MarketSnapshot(evaluation, List.of(), Map.of("ENCHANTED_DIAMOND", latest));
        Set<String> ids = Set.of("ENCHANTED_DIAMOND");
        long evalMillis = evaluation.toEpochMilli();

        List<Long> anchors = List.of(
                Instant.parse("2026-02-15T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-16T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-17T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-02-18T00:00:00Z").toEpochMilli()
        );
        List<BzItemSnapshotEntity> dailyRows = List.of(
                rowCustom(anchors.get(0), "ENCHANTED_DIAMOND", 120D, 80D, 120L, 120L),
                rowCustom(anchors.get(1), "ENCHANTED_DIAMOND", 121D, 79D, 80L, 80L),
                rowCustom(anchors.get(2), "ENCHANTED_DIAMOND", 118D, 82D, 90L, 90L),
                rowCustom(anchors.get(3), "ENCHANTED_DIAMOND", 119D, 81D, 110L, 110L)
        );

        when(repository.findBySnapshotTsBetweenAndProductIdInOrderBySnapshotTsAsc(eq(evalMillis - 60_000L), eq(evalMillis), eq(ids)))
                .thenReturn(List.of(rowAtMillis(evalMillis - 30_000L, "ENCHANTED_DIAMOND", 100D), rowAtMillis(evalMillis, "ENCHANTED_DIAMOND", 101D)));
        when(repository.findFirstSnapshotTsPerDayBetween(eq((Math.floorDiv(evaluation.getEpochSecond(), 86_400L) - 32L) * 86_400_000L), eq(evalMillis)))
                .thenReturn(anchors);
        when(repository.findBySnapshotTsInAndProductIdInOrderBySnapshotTsAsc(eq(anchors), eq(ids)))
                .thenReturn(dailyRows);

        FlipScoreFeatureSet.ItemTimescaleFeatures features = featureService.computeFor(snapshot).get("ENCHANTED_DIAMOND");

        assertNotNull(features);
        assertTrue(features.structurallyIlliquid());
    }

    @Test
    void computeForHandlesMissingAnchorsAndInvalidRowsGracefully() {
        BzItemSnapshotRepository repository = mock(BzItemSnapshotRepository.class);
        MarketTimescaleFeatureService featureService = new MarketTimescaleFeatureService(repository, new MarketItemKeyService());
        Instant evaluation = Instant.parse("2026-02-18T12:00:00Z");
        MarketSnapshot latest = snapshot(evaluation.toString(), 300D);
        Set<String> ids = Set.of("ENCHANTED_DIAMOND");
        long evalMillis = evaluation.toEpochMilli();

        List<BzItemSnapshotEntity> microRows = new ArrayList<>();
        microRows.add(null);
        microRows.add(rowCustom(evalMillis - 30_000L, "ENCHANTED_DIAMOND", null, 100D, 10_000L, 10_000L));
        microRows.add(rowCustom(evalMillis, "ENCHANTED_DIAMOND", 300D, 299D, 10_000L, 10_000L));
        when(repository.findBySnapshotTsBetweenAndProductIdInOrderBySnapshotTsAsc(eq(evalMillis - 60_000L), eq(evalMillis), eq(ids)))
                .thenReturn(microRows);
        when(repository.findFirstSnapshotTsPerDayBetween(eq((Math.floorDiv(evaluation.getEpochSecond(), 86_400L) - 32L) * 86_400_000L), eq(evalMillis)))
                .thenReturn(List.of());

        FlipScoreFeatureSet.ItemTimescaleFeatures features = featureService.computeFor(latest).get("ENCHANTED_DIAMOND");

        assertNotNull(features);
        assertFalse(features.structurallyIlliquid());
        assertEquals(FlipScoreFeatureSet.ConfidenceLevel.LOW, features.macroConfidence());
        assertEquals(null, features.macroReturn1d());
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

    private BzItemSnapshotEntity rowAtMillis(long timestampMillis, String productId, double midPrice) {
        double buyPrice = midPrice + 2D;
        double sellPrice = midPrice - 2D;
        return rowCustom(timestampMillis, productId, buyPrice, sellPrice, 10_000L, 10_000L);
    }

    private BzItemSnapshotEntity rowCustom(long timestampMillis,
                                           String productId,
                                           Double buyPrice,
                                           Double sellPrice,
                                           Long buyVolume,
                                           Long sellVolume) {
        return new BzItemSnapshotEntity(
                timestampMillis,
                productId,
                buyPrice,
                sellPrice,
                buyVolume,
                sellVolume
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
