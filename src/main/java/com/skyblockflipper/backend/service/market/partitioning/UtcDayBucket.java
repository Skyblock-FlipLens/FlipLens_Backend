package com.skyblockflipper.backend.service.market.partitioning;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

public final class UtcDayBucket {

    private UtcDayBucket() {
    }

    public static LocalDate utcDay(Instant timestamp) {
        Instant safe = timestamp == null ? Instant.now() : timestamp;
        return safe.atZone(ZoneOffset.UTC).toLocalDate();
    }

    public static long startEpochMillis(LocalDate day) {
        if (day == null) {
            return 0L;
        }
        return day.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    public static long endEpochMillis(LocalDate day) {
        if (day == null) {
            return 0L;
        }
        return day.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }
}
