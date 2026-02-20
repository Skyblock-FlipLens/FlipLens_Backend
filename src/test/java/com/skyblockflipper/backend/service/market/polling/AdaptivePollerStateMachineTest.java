package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.config.properties.AdaptivePollingProperties;
import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptivePollerStateMachineTest {

    @Test
    void transitionsWarmupSteadyBurstBackoffAndBackToSteadyOnChange() {
        AdaptivePollingProperties.Endpoint cfg = AdaptivePollingProperties.Endpoint.defaults("test", "/x", Duration.ofSeconds(20));
        cfg.setWarmupMaxSeconds(5);
        cfg.setWarmupInterval(Duration.ofSeconds(1));
        cfg.setBurstWindowMs(1_000L);
        cfg.setBurstIntervalMs(200L);
        cfg.setBackoffInterval(Duration.ofMillis(500L));

        AdaptivePoller<String> poller = getStringAdaptivePoller(cfg);

        AdaptivePollerState state = poller.snapshotState();
        state.setMode(PollerMode.WARMUP);
        state.setWarmupStartedAtMillis(0L);
        poller.onNoChange(6_000L);
        assertEquals(PollerMode.STEADY, state.getMode());

        state.setMode(PollerMode.STEADY);
        state.setLastChangeAtMillis(10_000L);
        state.setEstimatedPeriodMillis(20_000L);
        poller.onNoChange(15_000L);
        assertEquals(PollerMode.BURST, state.getMode());

        state.setMode(PollerMode.BURST);
        state.setBurstStartedAtMillis(0L);
        poller.onNoChange(2_000L);
        assertEquals(PollerMode.BACKOFF, state.getMode());

        poller.onChanged(
                new AdaptivePoller.PollExecution<>(
                        ChangeDetector.ChangeDecision.changed(),
                        "payload",
                        30_000L,
                        HypixelHttpResult.success(200, HttpHeaders.EMPTY, null)
                ),
                30_000L
        );
        assertEquals(PollerMode.STEADY, state.getMode());
        assertEquals(1L, state.getUpdateCount());
        assertTrue(state.getEstimatedPeriodMillis() > 0L);

        poller.stop();
    }

    private static AdaptivePoller<String> getStringAdaptivePoller(AdaptivePollingProperties.Endpoint cfg) {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ProcessingPipeline<String> pipeline = new ProcessingPipeline<>(
                "test",
                meterRegistry,
                1,
                true,
                payload -> { }
        );
        return new AdaptivePoller<>(
                "test",
                cfg,
                new ThreadPoolTaskScheduler(),
                meterRegistry,
                detector -> new AdaptivePoller.PollExecution<>(
                        ChangeDetector.ChangeDecision.noChange(),
                        null,
                        0L,
                        HypixelHttpResult.success(200, HttpHeaders.EMPTY, null)
                ),
                pipeline,
                new GlobalRequestLimiter(1000d)
        );
    }
}
