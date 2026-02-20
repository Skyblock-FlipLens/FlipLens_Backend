package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimitHandlerTest {

    @Test
    void parsesRetryAfterSeconds() {
        long now = 1_000_000L;
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.RETRY_AFTER, "3");

        long blockedUntil = RateLimitHandler.parseBlockedUntil(headers, now);
        assertTrue(blockedUntil >= now + 3_000L);
    }

    @Test
    void parsesRateLimitResetAsDeltaSeconds() {
        long now = 2_000_000L;
        HttpHeaders headers = new HttpHeaders();
        headers.add("RateLimit-Reset", "5");

        long blockedUntil = RateLimitHandler.parseBlockedUntil(headers, now);
        assertTrue(blockedUntil >= now + 5_000L);
    }

    @Test
    void storesAndReportsBlockedWindowAfter429() {
        long now = System.currentTimeMillis();
        RateLimitHandler handler = new RateLimitHandler(1_000L);
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.RETRY_AFTER, "2");

        handler.on429(HypixelHttpResult.error(429, headers, "rate limited"), now);
        assertTrue(handler.blockedForMillis(now) >= 1_500L);
    }
}
