package com.skyblockflipper.backend.service.market.polling;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Slf4j
public class ProcessingPipeline<T> implements AutoCloseable {

    private final String endpoint;
    private final MeterRegistry meterRegistry;
    private final Consumer<T> processor;
    private final ExecutorService executor;
    private final boolean coalesceEnabled;
    private final BlockingQueue<Envelope<T>> queue;
    private final AtomicReference<Envelope<T>> latestPending = new AtomicReference<>();
    private final AtomicBoolean draining = new AtomicBoolean(false);

    public ProcessingPipeline(String endpoint,
                              MeterRegistry meterRegistry,
                              int queueCapacity,
                              boolean coalesceEnabled,
                              Consumer<T> processor) {
        this.endpoint = endpoint;
        this.meterRegistry = meterRegistry;
        this.processor = processor;
        this.coalesceEnabled = coalesceEnabled;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
        this.executor = Executors.newSingleThreadExecutor(new PipelineThreadFactory(endpoint));
    }

    public boolean submit(T payload) {
        Envelope<T> envelope = new Envelope<>(payload, System.currentTimeMillis());
        boolean accepted;
        if (coalesceEnabled) {
            Envelope<T> replaced = latestPending.getAndSet(envelope);
            accepted = true;
            if (replaced != null) {
                meterRegistry.counter("skyblock.adaptive.processing_dropped", "endpoint", endpoint).increment();
            }
        } else {
            accepted = queue.offer(envelope);
            if (!accepted) {
                meterRegistry.counter("skyblock.adaptive.processing_dropped", "endpoint", endpoint).increment();
            }
        }
        triggerDrain();
        return accepted;
    }

    private void triggerDrain() {
        if (!draining.compareAndSet(false, true)) {
            return;
        }
        executor.execute(() -> {
            try {
                drainLoop();
            } finally {
                draining.set(false);
                if ((coalesceEnabled && latestPending.get() != null) || (!coalesceEnabled && !queue.isEmpty())) {
                    triggerDrain();
                }
            }
        });
    }

    private void drainLoop() {
        for (;;) {
            Envelope<T> envelope = coalesceEnabled ? latestPending.getAndSet(null) : queue.poll();
            if (envelope == null) {
                return;
            }
            long lagMillis = Math.max(0L, System.currentTimeMillis() - envelope.enqueuedAtMillis);
            meterRegistry.summary("skyblock.adaptive.processing_lag_ms", "endpoint", endpoint).record(lagMillis);
            long startedAt = System.nanoTime();
            try {
                processor.accept(envelope.payload);
                meterRegistry.counter("skyblock.adaptive.processing_success", "endpoint", endpoint).increment();
            } catch (RuntimeException e) {
                meterRegistry.counter("skyblock.adaptive.processing_error", "endpoint", endpoint).increment();
                log.warn("Processing failed for endpoint {}: {}", endpoint, e.getMessage());
            } finally {
                long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                meterRegistry.summary("skyblock.adaptive.processing_duration_ms", "endpoint", endpoint).record(tookMillis);
            }
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private record Envelope<T>(T payload, long enqueuedAtMillis) {
    }

    private static final class PipelineThreadFactory implements ThreadFactory {
        private final String endpoint;

        private PipelineThreadFactory(String endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("AdaptivePipeline-" + endpoint);
            thread.setDaemon(true);
            return thread;
        }
    }
}
