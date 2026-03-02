package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.config.properties.AdaptivePollingProperties;
import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.TaskScheduler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ScheduledFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AdaptivePollerBehaviorTest {

    @Test
    void runOnceReturnsImmediatelyWhenNotRunning() throws Exception {
        Fixture fixture = fixture();

        invokePrivate(fixture.poller, "runOnce");

        verify(fixture.pollExecutor, times(0)).execute(any());
        verify(fixture.taskScheduler, times(0)).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void runOnceSkipsOverlappingExecutionAndSchedulesBurstRetry() throws Exception {
        Fixture fixture = fixture();
        setAtomicFlag(fixture.poller, "running", true);
        setAtomicFlag(fixture.poller, "inFlight", true);

        invokePrivate(fixture.poller, "runOnce");

        verify(fixture.pollExecutor, times(0)).execute(any());
        verify(fixture.taskScheduler, times(1)).schedule(any(Runnable.class), any(Instant.class));
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.overlap_guard_skip")
                        .tag("endpoint", "test")
                        .counter()
                        .count()
        );
    }

    @Test
    void runOnceExecutesPollAndSchedulesFollowup() throws Exception {
        Fixture fixture = fixture();
        setAtomicFlag(fixture.poller, "running", true);
        AdaptivePoller.PollExecution<String> noChange = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.noChange(),
                null,
                0L,
                HypixelHttpResult.success(200, HttpHeaders.EMPTY, "ok")
        );
        when(fixture.pollExecutor.execute(any())).thenReturn(noChange);

        invokePrivate(fixture.poller, "runOnce");

        verify(fixture.pollExecutor, times(1)).execute(any());
        verify(fixture.taskScheduler, times(1)).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void runOnceHandlesPollerFailuresWithBackoff() throws Exception {
        Fixture fixture = fixture();
        setAtomicFlag(fixture.poller, "running", true);
        when(fixture.pollExecutor.execute(any())).thenThrow(new RuntimeException("boom"));

        invokePrivate(fixture.poller, "runOnce");

        verify(fixture.pollExecutor, times(1)).execute(any());
        assertEquals(PollerMode.BACKOFF, fixture.poller.snapshotState().getMode());
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.poller_error")
                        .tag("endpoint", "test")
                        .counter()
                        .count()
        );
    }

    @Test
    void startSchedulesOnceAndInitializesWarmup() {
        Fixture fixture = fixture();

        fixture.poller.start();
        fixture.poller.start();

        verify(fixture.taskScheduler, times(1)).schedule(any(Runnable.class), any(Instant.class));
        assertEquals(PollerMode.WARMUP, fixture.poller.snapshotState().getMode());
        assertTrue(fixture.poller.snapshotState().getWarmupStartedAtMillis() > 0L);
    }

    @Test
    void stopCancelsNextScheduleAndClosesPipeline() {
        Fixture fixture = fixture();
        fixture.poller.start();

        fixture.poller.stop();

        verify(fixture.scheduledFuture).cancel(true);
        verify(fixture.pipeline).close();
    }

    @Test
    void onNoChangeCoversWarmupSteadyBurstBackoffTransitions() throws Exception {
        Fixture fixture = fixture();
        AdaptivePollerState state = stateOf(fixture.poller);
        long now = System.currentTimeMillis();
        state.setWarmupStartedAtMillis(now - 500L);

        long warmupDelay = fixture.poller.onNoChange(now);
        assertEquals(200L, warmupDelay);
        assertEquals(PollerMode.WARMUP, state.getMode());

        state.setWarmupStartedAtMillis(now - 5_000L);
        long steadyDelay = fixture.poller.onNoChange(now);
        assertEquals(PollerMode.STEADY, state.getMode());
        assertEquals(200L, steadyDelay);

        long burstDelay = fixture.poller.onNoChange(now);
        assertEquals(100L, burstDelay);
        assertEquals(PollerMode.BURST, state.getMode());

        state.setBurstStartedAtMillis(now - 50L);
        long burstDelay2 = fixture.poller.onNoChange(now);
        assertEquals(100L, burstDelay2);
        assertEquals(PollerMode.BURST, state.getMode());

        state.setBurstStartedAtMillis(now - 1_000L);
        long backoffDelay = fixture.poller.onNoChange(now);
        assertEquals(500L, backoffDelay);
        assertEquals(PollerMode.BACKOFF, state.getMode());

        state.setLastChangeAtMillis(now - 5_000L);
        state.setEstimatedPeriodMillis(1_000L);
        long backToSteady = fixture.poller.onNoChange(now);
        assertEquals(PollerMode.STEADY, state.getMode());
        assertTrue(backToSteady >= 0L);
    }

    @Test
    void onErrorIncrementsMetricAndReturnsBackoff() {
        Fixture fixture = fixture();

        long delay = fixture.poller.onError();

        assertEquals(500L, delay);
        assertEquals(PollerMode.BACKOFF, fixture.poller.snapshotState().getMode());
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.http_error")
                        .tag("endpoint", "test")
                        .counter()
                        .count()
        );
    }

    @Test
    void onChangedUpdatesEstimatorStateAndSubmitsPayload() throws Exception {
        Fixture fixture = fixture();
        AdaptivePollerState state = stateOf(fixture.poller);
        long now = System.currentTimeMillis();
        state.setLastChangeAtMillis(now - 2_000L);
        state.setExpectedChangeAtMillis(now - 100L);
        when(fixture.pipeline.submit("payload")).thenReturn(true);
        AdaptivePoller.PollExecution<String> execution = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.changed(),
                "payload",
                now,
                HypixelHttpResult.success(200, HttpHeaders.EMPTY, "probe")
        );

        long delay = fixture.poller.onChanged(execution, now);

        verify(fixture.pipeline).submit("payload");
        assertEquals(0L, state.getMissCount());
        assertEquals(1L, state.getUpdateCount());
        assertEquals(PollerMode.STEADY, state.getMode());
        assertTrue(delay >= 0L);
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.update_detected")
                        .tag("endpoint", "test")
                        .counter()
                        .count()
        );
    }

    @Test
    void onChangedIncrementsRejectedCounterWhenPipelineDropsPayload() {
        Fixture fixture = fixture();
        when(fixture.pipeline.submit("payload")).thenReturn(false);
        long now = System.currentTimeMillis();
        AdaptivePoller.PollExecution<String> execution = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.changed(),
                "payload",
                now,
                HypixelHttpResult.success(200, HttpHeaders.EMPTY, "probe")
        );

        fixture.poller.onChanged(execution, now);

        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.processing_rejected")
                        .tag("endpoint", "test")
                        .counter()
                        .count()
        );
    }

    @Test
    void computeSteadyDelayUsesExpectedChangeAndGuardWindow() throws Exception {
        Fixture fixture = fixture();
        AdaptivePollerState state = stateOf(fixture.poller);
        state.setLastChangeAtMillis(10_000L);
        state.setEstimatedPeriodMillis(2_000L);

        long delay = fixture.poller.computeSteadyDelay(11_000L);

        assertEquals(800L, delay);
        assertEquals(12_000L, state.getExpectedChangeAtMillis());
    }

    @Test
    void executeWithRetryRetriesTransientFailures() throws Exception {
        Fixture fixture = fixture();
        AdaptivePoller.PollExecution<String> transientFailure = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.error(),
                null,
                0L,
                HypixelHttpResult.transportError("net")
        );
        AdaptivePoller.PollExecution<String> success = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.noChange(),
                null,
                0L,
                HypixelHttpResult.success(200, HttpHeaders.EMPTY, "ok")
        );
        when(fixture.pollExecutor.execute(any())).thenReturn(transientFailure, success);

        Object result = invokePrivate(fixture.poller, "executeWithRetry");

        verify(fixture.pollExecutor, times(2)).execute(any());
        assertEquals(success, result);
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.transient_retry")
                        .tag("endpoint", "test")
                        .counter()
                        .count()
        );
    }

    @Test
    void decideNextDelayHandlesRateLimitAndErrors() throws Exception {
        Fixture fixture = fixture();
        long now = System.currentTimeMillis();
        AdaptivePoller.PollExecution<String> rateLimited = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.rateLimited(),
                null,
                0L,
                HypixelHttpResult.error(429, HttpHeaders.EMPTY, "rate")
        );
        AdaptivePoller.PollExecution<String> error = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.error(),
                null,
                0L,
                HypixelHttpResult.error(500, HttpHeaders.EMPTY, "err")
        );

        long delay429 = (long) invokePrivate(
                fixture.poller,
                "decideNextDelay",
                new Class<?>[]{AdaptivePoller.PollExecution.class, long.class},
                rateLimited,
                now
        );
        long delayError = (long) invokePrivate(
                fixture.poller,
                "decideNextDelay",
                new Class<?>[]{AdaptivePoller.PollExecution.class, long.class},
                error,
                now
        );

        assertTrue(delay429 >= 500L);
        assertEquals(500L, delayError);
        assertEquals(
                1.0d,
                fixture.meterRegistry.get("skyblock.adaptive.http_429")
                        .tag("endpoint", "test")
                        .counter()
                        .count()
        );
    }

    @Test
    void decideNextDelayNoChangeHonorsCacheHeaders() throws Exception {
        Fixture fixture = fixture();
        long now = System.currentTimeMillis();
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CACHE_CONTROL, "public, max-age=10");
        headers.add(HttpHeaders.AGE, "2");
        AdaptivePoller.PollExecution<String> noChange = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.noChange(),
                null,
                0L,
                HypixelHttpResult.success(200, headers, "ok")
        );

        long delay = (long) invokePrivate(
                fixture.poller,
                "decideNextDelay",
                new Class<?>[]{AdaptivePoller.PollExecution.class, long.class},
                noChange,
                now
        );

        assertEquals(8_000L, delay);
    }

    @Test
    void decideNextDelayChangedWithOverrideStillProcessesPayload() throws Exception {
        Fixture fixture = fixture();
        long now = System.currentTimeMillis();
        AdaptivePoller.PollExecution<String> changedWithOverride = new AdaptivePoller.PollExecution<>(
                ChangeDetector.ChangeDecision.changed(),
                "payload",
                now,
                HypixelHttpResult.success(200, HttpHeaders.EMPTY, "ok"),
                1_234L
        );

        long delay = (long) invokePrivate(
                fixture.poller,
                "decideNextDelay",
                new Class<?>[]{AdaptivePoller.PollExecution.class, long.class},
                changedWithOverride,
                now
        );

        verify(fixture.pipeline).submit("payload");
        assertEquals(1_234L, delay);
    }

    @Test
    void cacheFreshForMillisAndDirectiveParserHandleInvalidInput() throws Exception {
        Fixture fixture = fixture();
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CACHE_CONTROL, "max-age=nope");
        headers.add(HttpHeaders.AGE, "not-a-number");

        long invalid = (long) invokePrivate(
                fixture.poller,
                "cacheFreshForMillis",
                new Class<?>[]{HttpHeaders.class},
                headers
        );
        Object parsed = invokePrivate(
                fixture.poller,
                "parseDirectiveSeconds",
                new Class<?>[]{String.class, String.class},
                "public, max-age=nope",
                "max-age"
        );

        assertEquals(0L, invalid);
        assertNull(parsed);
    }

    private static Fixture fixture() {
        AdaptivePollingProperties.Endpoint cfg = AdaptivePollingProperties.Endpoint.defaults("test", "/x", Duration.ofSeconds(20));
        cfg.setWarmupInterval(Duration.ofMillis(200));
        cfg.setWarmupMaxSeconds(2);
        cfg.setBurstIntervalMs(100L);
        cfg.setBurstWindowMs(300L);
        cfg.setBackoffInterval(Duration.ofMillis(500L));
        cfg.setGuardWindowMs(200L);
        cfg.setMinGuardWindowMs(100L);
        cfg.setMaxGuardWindowMs(400L);
        cfg.setTransientRetries(1);

        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        when(taskScheduler.schedule(any(Runnable.class), any(Instant.class))).thenReturn((ScheduledFuture) scheduledFuture);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        @SuppressWarnings("unchecked")
        ProcessingPipeline<String> pipeline = mock(ProcessingPipeline.class);
        when(pipeline.submit(any())).thenReturn(true);
        @SuppressWarnings("unchecked")
        AdaptivePoller.PollExecutor<String> pollExecutor = mock(AdaptivePoller.PollExecutor.class);

        AdaptivePoller<String> poller = new AdaptivePoller<>(
                "test",
                cfg,
                taskScheduler,
                meterRegistry,
                pollExecutor,
                pipeline,
                new GlobalRequestLimiter(1_000d)
        );
        return new Fixture(poller, taskScheduler, scheduledFuture, meterRegistry, pipeline, pollExecutor);
    }

    private static AdaptivePollerState stateOf(AdaptivePoller<?> poller) throws Exception {
        Field stateField = AdaptivePoller.class.getDeclaredField("state");
        stateField.setAccessible(true);
        return (AdaptivePollerState) stateField.get(poller);
    }

    private static Object invokePrivate(Object target, String methodName) throws Exception {
        return invokePrivate(target, methodName, new Class<?>[0]);
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private static void setAtomicFlag(AdaptivePoller<?> poller, String fieldName, boolean value) throws Exception {
        Field field = AdaptivePoller.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        AtomicBoolean atomic = (AtomicBoolean) field.get(poller);
        atomic.set(value);
    }

    private record Fixture(AdaptivePoller<String> poller,
                           TaskScheduler taskScheduler,
                           ScheduledFuture<?> scheduledFuture,
                           SimpleMeterRegistry meterRegistry,
                           ProcessingPipeline<String> pipeline,
                           AdaptivePoller.PollExecutor<String> pollExecutor) {
    }
}
