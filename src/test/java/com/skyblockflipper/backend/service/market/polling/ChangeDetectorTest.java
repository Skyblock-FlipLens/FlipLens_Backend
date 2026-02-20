package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChangeDetectorTest {

    @Test
    void detectsChangeUsingEtagThenNoChangeOnSameEtag() {
        ChangeDetector detector = new ChangeDetector();
        HttpHeaders headers = new HttpHeaders();
        headers.setETag("\"v1\"");

        ChangeDetector.ChangeDecision first = detector.evaluate(HypixelHttpResult.success(200, headers, "body"), "hash1");
        ChangeDetector.ChangeDecision second = detector.evaluate(HypixelHttpResult.success(200, headers, "body"), "hash1");

        assertEquals(ChangeDetector.Decision.CHANGED, first.decision());
        assertEquals(ChangeDetector.Decision.NO_CHANGE, second.decision());
    }

    @Test
    void treats304AsNoChange() {
        ChangeDetector detector = new ChangeDetector();
        ChangeDetector.ChangeDecision decision = detector.evaluate(HypixelHttpResult.error(304, HttpHeaders.EMPTY, "not-modified"), null);
        assertEquals(ChangeDetector.Decision.NO_CHANGE, decision.decision());
    }

    @Test
    void fallsBackToHashWhenValidatorHeadersMissing() {
        ChangeDetector detector = new ChangeDetector();
        ChangeDetector.ChangeDecision first = detector.evaluate(HypixelHttpResult.success(200, HttpHeaders.EMPTY, "b1"), "h1");
        ChangeDetector.ChangeDecision second = detector.evaluate(HypixelHttpResult.success(200, HttpHeaders.EMPTY, "b2"), "h2");

        assertEquals(ChangeDetector.Decision.CHANGED, first.decision());
        assertEquals(ChangeDetector.Decision.CHANGED, second.decision());
    }

    @Test
    void flags429AsRateLimited() {
        ChangeDetector detector = new ChangeDetector();
        ChangeDetector.ChangeDecision decision = detector.evaluate(HypixelHttpResult.error(429, HttpHeaders.EMPTY, "rate"), null);
        assertEquals(ChangeDetector.Decision.RATE_LIMITED, decision.decision());
    }
}
