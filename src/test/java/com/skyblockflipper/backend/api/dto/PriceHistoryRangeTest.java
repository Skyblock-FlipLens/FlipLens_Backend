package com.skyblockflipper.backend.api.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PriceHistoryRangeTest {

    @Test
    void fromQueryValueParsesKnownValuesAndDefaultsToThirtyDays() {
        assertEquals(PriceHistoryRange.H24, PriceHistoryRange.fromQueryValue("24h"));
        assertEquals(PriceHistoryRange.D7, PriceHistoryRange.fromQueryValue("7d"));
        assertEquals(PriceHistoryRange.D30, PriceHistoryRange.fromQueryValue("30d"));
        assertEquals(PriceHistoryRange.D90, PriceHistoryRange.fromQueryValue("90d"));
        assertEquals(PriceHistoryRange.D30, PriceHistoryRange.fromQueryValue(null));
        assertEquals(PriceHistoryRange.D30, PriceHistoryRange.fromQueryValue(" "));
        assertEquals(PriceHistoryRange.D30, PriceHistoryRange.fromQueryValue("invalid"));
    }
}

