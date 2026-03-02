package com.skyblockflipper.backend.service.market.polling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalRequestLimiterTest {

    @Test
    void reserveDelayDoesNotPushWindowForwardWhenAlreadyRateLimited() {
        GlobalRequestLimiter limiter = new GlobalRequestLimiter(3.0d);

        assertEquals(0L, limiter.reserveDelayMillis());

        long firstDelay = limiter.reserveDelayMillis();
        long secondDelay = limiter.reserveDelayMillis();

        assertTrue(firstDelay > 0L, "Expected positive delay once rate limited");
        assertTrue(secondDelay <= firstDelay + 10L, "Delay should not grow on repeated checks");
    }

    @Test
    void reserveDelayAllowsProgressAfterWaiting() throws InterruptedException {
        GlobalRequestLimiter limiter = new GlobalRequestLimiter(3.0d);

        assertEquals(0L, limiter.reserveDelayMillis());
        long waitMillis = limiter.reserveDelayMillis();
        assertTrue(waitMillis > 0L, "Expected positive delay before next permit");

        Thread.sleep(waitMillis + 30L);

        assertEquals(0L, limiter.reserveDelayMillis());
    }
}
