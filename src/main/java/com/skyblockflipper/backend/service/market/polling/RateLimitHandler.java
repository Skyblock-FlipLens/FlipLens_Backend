package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import org.springframework.http.HttpHeaders;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimitHandler {

    private final AtomicLong blockedUntilEpochMillis = new AtomicLong(0L);
    private final long fallbackBlockMillis;

    public RateLimitHandler(long fallbackBlockMillis) {
        this.fallbackBlockMillis = Math.max(500L, fallbackBlockMillis);
    }

    public void on429(HypixelHttpResult<?> result, long nowMillis) {
        long candidate = Math.max(nowMillis + fallbackBlockMillis, parseBlockedUntil(result.headers(), nowMillis));
        blockedUntilEpochMillis.accumulateAndGet(candidate, Math::max);
    }

    public long blockedForMillis(long nowMillis) {
        long blockedUntil = blockedUntilEpochMillis.get();
        return Math.max(0L, blockedUntil - nowMillis);
    }

    static long parseBlockedUntil(HttpHeaders headers, long nowMillis) {
        long fromRetryAfter = parseRetryAfter(headers.getFirst(HttpHeaders.RETRY_AFTER), nowMillis);
        long fromRateLimitReset = parseReset(headers.getFirst("RateLimit-Reset"), nowMillis);
        if (fromRateLimitReset <= 0L) {
            fromRateLimitReset = parseReset(headers.getFirst("X-RateLimit-Reset"), nowMillis);
        }
        return Math.max(fromRetryAfter, fromRateLimitReset);
    }

    private static long parseRetryAfter(String value, long nowMillis) {
        if (!StringUtils.hasText(value)) {
            return 0L;
        }
        String trimmed = value.trim();
        if (trimmed.chars().allMatch(Character::isDigit)) {
            long seconds = Long.parseLong(trimmed);
            return nowMillis + (seconds * 1_000L);
        }
        try {
            Instant parsed = ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            return parsed.toEpochMilli();
        } catch (DateTimeParseException ignored) {
            return 0L;
        }
    }

    private static long parseReset(String value, long nowMillis) {
        if (!StringUtils.hasText(value)) {
            return 0L;
        }
        String trimmed = value.trim();
        if (!trimmed.chars().allMatch(Character::isDigit)) {
            return 0L;
        }
        long numeric = Long.parseLong(trimmed);
        if (numeric > 10_000_000_000L) {
            return numeric;
        }
        if (numeric > 1_000_000_000L) {
            return numeric * 1_000L;
        }
        return nowMillis + (numeric * 1_000L);
    }
}
