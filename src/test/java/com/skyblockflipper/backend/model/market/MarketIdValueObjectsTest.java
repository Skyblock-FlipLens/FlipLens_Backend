package com.skyblockflipper.backend.model.market;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MarketIdValueObjectsTest {

    @Test
    void snapshotIdValueObjectsExposeFieldsAndEquality() {
        AhItemSnapshotId ah = new AhItemSnapshotId(10L, "ASPECT_OF_THE_END");
        AhItemSnapshotId ahSame = new AhItemSnapshotId(10L, "ASPECT_OF_THE_END");
        AhItemSnapshotId ahDifferent = new AhItemSnapshotId(11L, "ASPECT_OF_THE_VOID");
        assertEquals(10L, ah.getSnapshotTs());
        assertEquals("ASPECT_OF_THE_END", ah.getItemKey());
        assertEquals(ah, ahSame);
        assertEquals(ah.hashCode(), ahSame.hashCode());
        assertNotEquals(ah, ahDifferent);
        assertNotEquals(ah, new AhItemSnapshotId(11L, "ASPECT_OF_THE_END"));
        assertNotEquals(ah, new AhItemSnapshotId(10L, "ASPECT_OF_THE_VOID"));

        BzItemSnapshotId bz = new BzItemSnapshotId(12L, "ENCHANTED_GOLD");
        BzItemSnapshotId bzSame = new BzItemSnapshotId(12L, "ENCHANTED_GOLD");
        BzItemSnapshotId bzDifferent = new BzItemSnapshotId(13L, "ENCHANTED_DIAMOND");
        assertEquals(12L, bz.getSnapshotTs());
        assertEquals("ENCHANTED_GOLD", bz.getProductId());
        assertEquals(bz, bzSame);
        assertEquals(bz.hashCode(), bzSame.hashCode());
        assertNotEquals(bz, bzDifferent);
        assertNotEquals(bz, new BzItemSnapshotId(13L, "ENCHANTED_GOLD"));
        assertNotEquals(bz, new BzItemSnapshotId(12L, "ENCHANTED_DIAMOND"));
    }

    @Test
    void bucketIdValueObjectsExposeFieldsAndEquality() {
        AhItemBucketRollupId ah = new AhItemBucketRollupId(100L, "1h", "HYPERION");
        AhItemBucketRollupId ahSame = new AhItemBucketRollupId(100L, "1h", "HYPERION");
        AhItemBucketRollupId ahDifferent = new AhItemBucketRollupId(200L, "1d", "TERMINATOR");
        assertEquals(100L, ah.getBucketStartEpochMillis());
        assertEquals("1h", ah.getBucketGranularity());
        assertEquals("HYPERION", ah.getItemKey());
        assertEquals(ah, ahSame);
        assertEquals(ah.hashCode(), ahSame.hashCode());
        assertNotEquals(ah, ahDifferent);
        assertNotEquals(ah, new AhItemBucketRollupId(200L, "1h", "HYPERION"));
        assertNotEquals(ah, new AhItemBucketRollupId(100L, "1d", "HYPERION"));
        assertNotEquals(ah, new AhItemBucketRollupId(100L, "1h", "TERMINATOR"));

        BzItemBucketRollupId bz = new BzItemBucketRollupId(120L, "1h", "ENCHANTED_ENDER_PEARL");
        BzItemBucketRollupId bzSame = new BzItemBucketRollupId(120L, "1h", "ENCHANTED_ENDER_PEARL");
        BzItemBucketRollupId bzDifferent = new BzItemBucketRollupId(121L, "1d", "ENCHANTED_EYE_OF_ENDER");
        assertEquals(120L, bz.getBucketStartEpochMillis());
        assertEquals("1h", bz.getBucketGranularity());
        assertEquals("ENCHANTED_ENDER_PEARL", bz.getProductId());
        assertEquals(bz, bzSame);
        assertEquals(bz.hashCode(), bzSame.hashCode());
        assertNotEquals(bz, bzDifferent);
        assertNotEquals(bz, new BzItemBucketRollupId(121L, "1h", "ENCHANTED_ENDER_PEARL"));
        assertNotEquals(bz, new BzItemBucketRollupId(120L, "1d", "ENCHANTED_ENDER_PEARL"));
        assertNotEquals(bz, new BzItemBucketRollupId(120L, "1h", "ENCHANTED_EYE_OF_ENDER"));
    }

    @Test
    void materializationStateIdExposesFieldsAndEquality() {
        ItemBucketMaterializationStateId id = new ItemBucketMaterializationStateId(300L, "1d", "BZ");
        ItemBucketMaterializationStateId same = new ItemBucketMaterializationStateId(300L, "1d", "BZ");
        ItemBucketMaterializationStateId different = new ItemBucketMaterializationStateId(301L, "1h", "AH");

        assertEquals(300L, id.getBucketStartEpochMillis());
        assertEquals("1d", id.getBucketGranularity());
        assertEquals("BZ", id.getMarketType());
        assertEquals(id, same);
        assertEquals(id.hashCode(), same.hashCode());
        assertNotEquals(id, different);
        assertNotEquals(id, new ItemBucketMaterializationStateId(301L, "1d", "BZ"));
        assertNotEquals(id, new ItemBucketMaterializationStateId(300L, "1h", "BZ"));
        assertNotEquals(id, new ItemBucketMaterializationStateId(300L, "1d", "AH"));
    }
}
