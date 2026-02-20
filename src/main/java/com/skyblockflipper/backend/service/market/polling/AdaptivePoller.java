package com.skyblockflipper.backend.service.market.polling;

import com.skyblockflipper.backend.config.properties.AdaptivePollingProperties;
import com.skyblockflipper.backend.hypixel.HypixelHttpResult;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class AdaptivePoller<T> {

    private final String endpoint;
    private final AdaptivePollingProperties.Endpoint cfg;
    private final TaskScheduler taskScheduler;
    private final MeterRegistry meterRegistry;
    private final PollExecutor<T> pollExecutor;
    private final ProcessingPipeline<T> processingPipeline;
    private final GlobalRequestLimiter globalRequestLimiter;
    private final GlobalRequestLimiter burstRequestLimiter;
    private final ChangeDetector changeDetector;
    private final RateLimitHandler rateLimitHandler;
    private final PeriodEstimator periodEstimator;
    private final AdaptivePollerState state = new AdaptivePollerState();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean inFlight = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> nextScheduled;

    public AdaptivePoller(String endpoint,
                          AdaptivePollingProperties.Endpoint cfg,
                          TaskScheduler taskScheduler,
                          MeterRegistry meterRegistry,
                          PollExecutor<T> pollExecutor,
                          ProcessingPipeline<T> processingPipeline,
                          GlobalRequestLimiter globalRequestLimiter) {
        this.endpoint = endpoint;
        this.cfg = cfg;
        this.taskScheduler = taskScheduler;
        this.meterRegistry = meterRegistry;
        this.pollExecutor = pollExecutor;
        this.processingPipeline = processingPipeline;
        this.globalRequestLimiter = globalRequestLimiter;
        this.burstRequestLimiter = new GlobalRequestLimiter(cfg.getMaxBurstRate());
        long hintMillis = Math.max(1_000L, cfg.getPeriodHint().toMillis());
        this.periodEstimator = new PeriodEstimator(
                cfg.getEstimatorWindowSize(),
                cfg.getEmaAlpha(),
                hintMillis,
                Math.round(hintMillis * cfg.getMinPeriodMultiplier()),
                Math.round(hintMillis * cfg.getMaxPeriodMultiplier())
        );
        this.changeDetector = new ChangeDetector();
        this.rateLimitHandler = new RateLimitHandler(cfg.getBackoffInterval().toMillis());
        state.setEstimatedPeriodMillis(hintMillis);
        Gauge.builder("skyblock.adaptive.state", state, s -> s.getMode().ordinal())
                .tag("endpoint", endpoint)
                .register(meterRegistry);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long now = System.currentTimeMillis();
        state.setWarmupStartedAtMillis(now);
        scheduleAfter(0L);
        log.info("Adaptive poller started for endpoint {}", endpoint);
    }

    public void stop() {
        running.set(false);
        ScheduledFuture<?> scheduled = nextScheduled;
        if (scheduled != null) {
            scheduled.cancel(true);
        }
        processingPipeline.close();
        log.info("Adaptive poller stopped for endpoint {}", endpoint);
    }

    public AdaptivePollerState snapshotState() {
        return state;
    }

    private void scheduleAfter(long delayMillis) {
        if (!running.get()) {
            return;
        }
        long safeDelay = Math.max(0L, delayMillis);
        Instant runAt = Instant.ofEpochMilli(System.currentTimeMillis() + safeDelay);
        nextScheduled = taskScheduler.schedule(this::runOnce, runAt);
    }

    private void runOnce() {
        if (!running.get()) {
            return;
        }
        if (!inFlight.compareAndSet(false, true)) {
            meterRegistry.counter("skyblock.adaptive.overlap_guard_skip", "endpoint", endpoint).increment();
            scheduleAfter(Math.max(100L, cfg.getBurstIntervalMs()));
            return;
        }
        try {
            long now = System.currentTimeMillis();
            if (state.getLastPollAtMillis() > 0L) {
                meterRegistry.summary("skyblock.adaptive.poll_interval_ms", "endpoint", endpoint)
                        .record(Math.max(0L, now - state.getLastPollAtMillis()));
            }
            state.setLastPollAtMillis(now);

            long blockedMillis = rateLimitHandler.blockedForMillis(now);
            if (blockedMillis > 0L) {
                transitionTo(PollerMode.BACKOFF);
                scheduleAfter(blockedMillis);
                return;
            }

            long globalDelay = globalRequestLimiter.reserveDelayMillis();
            long endpointDelay = burstRequestLimiter.reserveDelayMillis();
            if (globalDelay > 0L || endpointDelay > 0L) {
                scheduleAfter(Math.max(globalDelay, endpointDelay));
                return;
            }

            PollExecution<T> execution = executeWithRetry();
            long nextDelay = decideNextDelay(execution, now);
            scheduleAfter(nextDelay);
        } catch (RuntimeException e) {
            meterRegistry.counter("skyblock.adaptive.poller_error", "endpoint", endpoint).increment();
            log.warn("Adaptive poller failure for endpoint {}: {}", endpoint, e.getMessage());
            transitionTo(PollerMode.BACKOFF);
            scheduleAfter(cfg.getBackoffInterval().toMillis());
        } finally {
            inFlight.set(false);
        }
    }

    private PollExecution<T> executeWithRetry() {
        PollExecution<T> latest = null;
        int attempts = Math.max(0, cfg.getTransientRetries()) + 1;
        for (int i = 0; i < attempts; i++) {
            latest = pollExecutor.execute(changeDetector);
            if (!isTransientFailure(latest)) {
                return latest;
            }
            meterRegistry.counter("skyblock.adaptive.transient_retry", "endpoint", endpoint).increment();
            sleepQuietly(Math.min(1_500L, 250L * (1L << i)));
        }
        return latest;
    }

    private boolean isTransientFailure(PollExecution<T> execution) {
        if (execution == null || execution.httpResult() == null) {
            return true;
        }
        HypixelHttpResult<?> httpResult = execution.httpResult();
        if (httpResult.transportError()) {
            return true;
        }
        int status = httpResult.statusCode();
        return status >= 500 && status < 600;
    }

    private long decideNextDelay(PollExecution<T> execution, long nowMillis) {
        if (execution == null || execution.httpResult() == null) {
            return onError();
        }
        HypixelHttpResult<?> httpResult = execution.httpResult();
        if (execution.decision().decision() == ChangeDetector.Decision.RATE_LIMITED || httpResult.statusCode() == 429) {
            meterRegistry.counter("skyblock.adaptive.http_429", "endpoint", endpoint).increment();
            rateLimitHandler.on429(httpResult, nowMillis);
            transitionTo(PollerMode.BACKOFF);
            return Math.max(cfg.getBackoffInterval().toMillis(), rateLimitHandler.blockedForMillis(nowMillis));
        }
        if (execution.decision().decision() == ChangeDetector.Decision.ERROR) {
            return onError();
        }
        if (execution.decision().isChanged()) {
            return onChanged(execution, nowMillis);
        }
        long baseDelay = onNoChange(nowMillis);
        long cacheDelay = cacheFreshForMillis(execution.httpResult().headers());
        if (cacheDelay > 0L) {
            meterRegistry.summary("skyblock.adaptive.cache_fresh_ms", "endpoint", endpoint).record(cacheDelay);
            return Math.max(baseDelay, cacheDelay);
        }
        return baseDelay;
    }

    long onChanged(PollExecution<T> execution, long nowMillis) {
        long changeAt = execution.changeTimestampMillis() > 0L ? execution.changeTimestampMillis() : nowMillis;
        if (state.getLastChangeAtMillis() > 0L && changeAt > state.getLastChangeAtMillis()) {
            periodEstimator.observeDelta(changeAt - state.getLastChangeAtMillis());
        }
        state.setLastChangeAtMillis(changeAt);
        state.setEstimatedPeriodMillis(periodEstimator.estimateMillis());
        state.setMissCount(0L);
        state.setUpdateCount(state.getUpdateCount() + 1L);
        state.setConsecutiveErrors(0L);

        if (state.getExpectedChangeAtMillis() > 0L) {
            long phaseErrorMillis = changeAt - state.getExpectedChangeAtMillis();
            meterRegistry.summary("skyblock.adaptive.phase_error_ms", "endpoint", endpoint)
                    .record(Math.abs(phaseErrorMillis));
        }

        if (execution.payload() != null) {
            boolean accepted = processingPipeline.submit(execution.payload());
            if (!accepted) {
                meterRegistry.counter("skyblock.adaptive.processing_rejected", "endpoint", endpoint).increment();
            }
        }
        meterRegistry.counter("skyblock.adaptive.update_detected", "endpoint", endpoint).increment();
        transitionTo(PollerMode.STEADY);
        return computeSteadyDelay(nowMillis);
    }

    long onNoChange(long nowMillis) {
        state.setMissCount(state.getMissCount() + 1L);
        meterRegistry.counter("skyblock.adaptive.misses", "endpoint", endpoint).increment();
        if (state.getMode() == PollerMode.WARMUP) {
            long warmupElapsed = nowMillis - state.getWarmupStartedAtMillis();
            if (warmupElapsed >= (cfg.getWarmupMaxSeconds() * 1_000L) && periodEstimator.sampleCount() >= 2) {
                transitionTo(PollerMode.STEADY);
                return computeSteadyDelay(nowMillis);
            }
            if (warmupElapsed >= (cfg.getWarmupMaxSeconds() * 1_000L)) {
                state.setEstimatedPeriodMillis(Math.max(1_000L, cfg.getPeriodHint().toMillis()));
                transitionTo(PollerMode.STEADY);
                return computeSteadyDelay(nowMillis);
            }
            return cfg.getWarmupInterval().toMillis();
        }
        if (state.getMode() == PollerMode.STEADY) {
            transitionTo(PollerMode.BURST);
            state.setBurstStartedAtMillis(nowMillis);
            return cfg.getBurstIntervalMs();
        }
        if (state.getMode() == PollerMode.BURST) {
            long burstElapsed = nowMillis - state.getBurstStartedAtMillis();
            if (burstElapsed <= cfg.getBurstWindowMs()) {
                return cfg.getBurstIntervalMs();
            }
            transitionTo(PollerMode.BACKOFF);
            return cfg.getBackoffInterval().toMillis();
        }
        transitionTo(PollerMode.STEADY);
        return computeSteadyDelay(nowMillis);
    }

    long onError() {
        state.setConsecutiveErrors(state.getConsecutiveErrors() + 1L);
        meterRegistry.counter("skyblock.adaptive.http_error", "endpoint", endpoint).increment();
        transitionTo(PollerMode.BACKOFF);
        return cfg.getBackoffInterval().toMillis();
    }

    long computeSteadyDelay(long nowMillis) {
        long estimatedPeriod = Math.max(1_000L, state.getEstimatedPeriodMillis());
        long lastChangeAt = state.getLastChangeAtMillis();
        if (lastChangeAt <= 0L) {
            return cfg.getWarmupInterval().toMillis();
        }
        long expectedChange = lastChangeAt + estimatedPeriod;
        long guardWindow = adaptiveGuardWindowMillis();
        state.setExpectedChangeAtMillis(expectedChange);
        long probeAt = expectedChange - guardWindow;
        long delay = probeAt - nowMillis;
        return Math.max(0L, delay);
    }

    long adaptiveGuardWindowMillis() {
        long jitter = periodEstimator.jitterMadMillis();
        long base = cfg.getGuardWindowMs();
        long adaptive = base + (jitter * 2L);
        return Math.max(cfg.getMinGuardWindowMs(), Math.min(cfg.getMaxGuardWindowMs(), adaptive));
    }

    private void transitionTo(PollerMode target) {
        PollerMode current = state.getMode();
        if (current == target) {
            return;
        }
        state.setMode(target);
        meterRegistry.counter(
                "skyblock.adaptive.mode_transitions",
                "endpoint", endpoint,
                "from", current.name(),
                "to", target.name()
        ).increment();
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(Math.max(0L, millis));
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private long cacheFreshForMillis(HttpHeaders headers) {
        if (headers == null) {
            return 0L;
        }
        String cacheControl = headers.getFirst(HttpHeaders.CACHE_CONTROL);
        if (!StringUtils.hasText(cacheControl)) {
            return 0L;
        }
        Long maxAgeSeconds = parseDirectiveSeconds(cacheControl, "max-age");
        if (maxAgeSeconds == null || maxAgeSeconds <= 0L) {
            return 0L;
        }
        long ageSeconds = 0L;
        String ageHeader = headers.getFirst(HttpHeaders.AGE);
        if (StringUtils.hasText(ageHeader) && ageHeader.trim().chars().allMatch(Character::isDigit)) {
            ageSeconds = Long.parseLong(ageHeader.trim());
        }
        long remaining = maxAgeSeconds - ageSeconds;
        return Math.max(0L, remaining * 1_000L);
    }

    private Long parseDirectiveSeconds(String cacheControl, String directive) {
        String[] parts = cacheControl.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.startsWith(directive + "=")) {
                continue;
            }
            String value = trimmed.substring((directive + "=").length());
            if (value.chars().allMatch(Character::isDigit)) {
                return Long.parseLong(value);
            }
        }
        return null;
    }

    public interface PollExecutor<T> {
        PollExecution<T> execute(ChangeDetector detector);
    }

    public record PollExecution<T>(
            ChangeDetector.ChangeDecision decision,
            T payload,
            long changeTimestampMillis,
            HypixelHttpResult<?> httpResult
    ) {
    }
}
