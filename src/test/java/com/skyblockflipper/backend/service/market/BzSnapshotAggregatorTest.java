package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BzSnapshotAggregatorTest {

    @Test
    void constructorRejectsNullMarketItemKeyService() {
        assertThrows(NullPointerException.class, () -> new BzSnapshotAggregator(null));
    }

    @Test
    void aggregateReturnsEmptyForNullSnapshotOrInput() {
        BzSnapshotAggregator aggregator = new BzSnapshotAggregator(new MarketItemKeyService());

        assertTrue(aggregator.aggregate(null, Map.of()).isEmpty());
        assertTrue(aggregator.aggregate(Instant.now(), null).isEmpty());
        assertTrue(aggregator.aggregate(Instant.now(), Map.of()).isEmpty());
    }

    @Test
    void aggregateSanitizesAndSortsUsingRecordProductIdOrEntryKeyFallback() {
        BzSnapshotAggregator aggregator = new BzSnapshotAggregator(new MarketItemKeyService());
        Instant snapshot = Instant.ofEpochMilli(1_000L);
        Map<String, BazaarMarketRecord> input = new LinkedHashMap<>();
        input.put("enchanted_carrot", new BazaarMarketRecord("ENCHANTED_CARROT", 12.0, 11.5, 100, 150, 1, 1, 1, 1));
        input.put("magma_cream", new BazaarMarketRecord(" ", 0.0, Double.NaN, -10, -5, 0, 0, 0, 0));
        input.put("skyblock_item", new BazaarMarketRecord(null, Double.POSITIVE_INFINITY, 99.0, 200, 300, 0, 0, 0, 0));
        input.put("null_value", null);

        List<BzItemSnapshotEntity> entities = aggregator.aggregate(snapshot, input);

        assertEquals(3, entities.size());
        assertEquals("ENCHANTED_CARROT", entities.get(0).getProductId());
        assertEquals("MAGMA_CREAM", entities.get(1).getProductId());
        assertEquals("SKYBLOCK_ITEM", entities.get(2).getProductId());

        BzItemSnapshotEntity carrot = entities.get(0);
        assertEquals(12.0, carrot.getBuyPrice());
        assertEquals(11.5, carrot.getSellPrice());
        assertEquals(100L, carrot.getBuyVolume());
        assertEquals(150L, carrot.getSellVolume());

        BzItemSnapshotEntity magma = entities.get(1);
        assertNull(magma.getBuyPrice());
        assertNull(magma.getSellPrice());
        assertNull(magma.getBuyVolume());
        assertNull(magma.getSellVolume());

        BzItemSnapshotEntity skyblock = entities.get(2);
        assertNull(skyblock.getBuyPrice());
        assertEquals(99.0, skyblock.getSellPrice());
        assertEquals(200L, skyblock.getBuyVolume());
        assertEquals(300L, skyblock.getSellVolume());
    }
}
