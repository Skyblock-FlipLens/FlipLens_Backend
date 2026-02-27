package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarketItemKeyServiceTest {

    private final MarketItemKeyService service = new MarketItemKeyService();

    @Test
    void toAuctionItemKeyReturnsNullForNullRecord() {
        assertNull(service.toAuctionItemKey(null));
    }

    @Test
    void normalizeBazaarProductIdHandlesNullBlankAndTrimUppercase() {
        assertNull(service.normalizeBazaarProductId(null));
        assertNull(service.normalizeBazaarProductId("   "));
        assertEquals("ENCHANTED_CARROT", service.normalizeBazaarProductId(" enchanted_carrot "));
    }

    @Test
    void toBazaarItemKeyNormalizesProductIdFromRecord() {
        BazaarMarketRecord record = new BazaarMarketRecord("enchanted_diamond", 10.0, 9.0, 1, 2, 3, 4, 5, 6);

        assertEquals("ENCHANTED_DIAMOND", service.toBazaarItemKey(record));
    }

    @Test
    void toAuctionItemKeyUsesInternalNameFromExtraAndDetectsRecombAndStars() {
        AuctionMarketRecord record = new AuctionMarketRecord(
                "a1",
                "Aspect of the End",
                "weapon",
                "epic",
                100_000L,
                0L,
                1L,
                2L,
                false,
                true,
                "RARITY UPGRADED \u272A",
                "{\"internalname\":\"ASPECT_OF_THE_END\"}"
        );

        String key = service.toAuctionItemKey(record);

        assertEquals("ASPECT_OF_THE_END|T:EPIC|C:WEAPON|P:-|S:1|R:1", key);
    }

    @Test
    void toAuctionItemKeyParsesPetLevelAndCountsStarsFromNameAndLore() {
        AuctionMarketRecord record = new AuctionMarketRecord(
                "a2",
                "[Lvl 85] Griffin Pet \u272A\u272A",
                "pet",
                "legendary",
                2_500_000L,
                0L,
                1L,
                2L,
                false,
                true,
                "my lore \u272A",
                null
        );

        String key = service.toAuctionItemKey(record);

        assertTrue(key.startsWith("GRIFFIN_PET_\u272A\u272A|T:LEGENDARY|C:PET|P:85|S:3|R:0"));
    }

    @Test
    void toAuctionItemKeyFallsBackToNormalizedItemNameWhenNoExtraId() {
        AuctionMarketRecord record = new AuctionMarketRecord(
                "a3",
                "Juju Shortbow",
                "weapon",
                "epic",
                1_200_000L,
                0L,
                1L,
                2L,
                false,
                true,
                null,
                null
        );

        String key = service.toAuctionItemKey(record);

        assertEquals("JUJU_SHORTBOW|T:EPIC|C:WEAPON|P:-|S:0|R:0", key);
    }

    @Test
    void toAuctionItemKeyUsesUnknownDefaultsForBlankInputs() {
        AuctionMarketRecord record = new AuctionMarketRecord(
                "a4",
                "   ",
                " ",
                " ",
                100L,
                0L,
                1L,
                2L,
                false,
                true,
                null,
                null
        );

        String key = service.toAuctionItemKey(record);

        assertEquals("UNKNOWN|T:UNKNOWN|C:UNKNOWN|P:-|S:0|R:0", key);
    }

    @Test
    void toAuctionItemKeySupportsEscapedExtraAndAlternateIdFields() {
        AuctionMarketRecord escaped = new AuctionMarketRecord(
                "a5",
                "Any Item",
                "misc",
                "rare",
                100L,
                0L,
                1L,
                2L,
                false,
                true,
                null,
                "{\\\"itemId\\\":\\\"my_item\\\"}"
        );
        AuctionMarketRecord alt = new AuctionMarketRecord(
                "a6",
                "Any Item",
                "misc",
                "rare",
                100L,
                0L,
                1L,
                2L,
                false,
                true,
                null,
                "{\"ITEM_ID\":\"alternate_item\"}"
        );

        assertEquals("MY_ITEM|T:RARE|C:MISC|P:-|S:0|R:0", service.toAuctionItemKey(escaped));
        assertEquals("ALTERNATE_ITEM|T:RARE|C:MISC|P:-|S:0|R:0", service.toAuctionItemKey(alt));
    }
}
