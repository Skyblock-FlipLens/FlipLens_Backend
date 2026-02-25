package com.skyblockflipper.backend.service.market.polling;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessingPipelineTest {

    @Test
    void submitWithoutCoalescingDropsWhenQueueIsFull() throws Exception {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();

        try (ProcessingPipeline<String> pipeline = new ProcessingPipeline<>(
                "bazaar",
                meterRegistry,
                1,
                false,
                payload -> {
                    started.countDown();
                    await(unblock, Duration.ofSeconds(2));
                    processed.incrementAndGet();
                }
        )) {
            assertTrue(pipeline.submit("first"));
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertTrue(pipeline.submit("second"));
            assertFalse(pipeline.submit("third"));

            unblock.countDown();
            waitFor(() -> processed.get() == 2
                    && meterRegistry.counter("skyblock.adaptive.processing_success", "endpoint", "bazaar").count() >= 2.0
                    && meterRegistry.counter("skyblock.adaptive.processing_dropped", "endpoint", "bazaar").count() >= 1.0,
                    Duration.ofSeconds(2));

            assertEquals(1.0, meterRegistry.counter("skyblock.adaptive.processing_dropped", "endpoint", "bazaar").count());
            assertEquals(2.0, meterRegistry.counter("skyblock.adaptive.processing_success", "endpoint", "bazaar").count());
        }
    }

    @Test
    void submitWithCoalescingKeepsLatestPendingPayload() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        List<String> processed = new CopyOnWriteArrayList<>();

        try (ProcessingPipeline<String> pipeline = new ProcessingPipeline<>(
                "auctions",
                meterRegistry,
                1,
                true,
                payload -> {
                    processed.add(payload);
                    if ("first".equals(payload)) {
                        started.countDown();
                        await(unblock, Duration.ofSeconds(2));
                    }
                }
        )) {
            assertTrue(pipeline.submit("first"));
            await(started, Duration.ofSeconds(1));
            assertTrue(pipeline.submit("second"));
            assertTrue(pipeline.submit("third"));

            unblock.countDown();
            waitFor(() -> processed.size() >= 2
                    && meterRegistry.counter("skyblock.adaptive.processing_success", "endpoint", "auctions").count() >= 2.0
                    && meterRegistry.counter("skyblock.adaptive.processing_dropped", "endpoint", "auctions").count() >= 1.0,
                    Duration.ofSeconds(2));

            assertEquals("first", processed.getFirst());
            assertEquals("third", processed.get(1));
            assertEquals(1.0, meterRegistry.counter("skyblock.adaptive.processing_dropped", "endpoint", "auctions").count());
            assertEquals(2.0, meterRegistry.counter("skyblock.adaptive.processing_success", "endpoint", "auctions").count());
        }
    }

    @Test
    void processorExceptionsIncrementErrorCounter() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        try (ProcessingPipeline<String> pipeline = new ProcessingPipeline<>(
                "errors",
                meterRegistry,
                2,
                false,
                payload -> {
                    throw new IllegalStateException("boom");
                }
        )) {
            assertTrue(pipeline.submit("x"));
            waitFor(() -> meterRegistry.counter("skyblock.adaptive.processing_error", "endpoint", "errors").count() >= 1.0
                    && meterRegistry.summary("skyblock.adaptive.processing_duration_ms", "endpoint", "errors").count() >= 1L,
                    Duration.ofSeconds(2));

            assertEquals(1.0, meterRegistry.counter("skyblock.adaptive.processing_error", "endpoint", "errors").count());
            assertEquals(0.0, meterRegistry.counter("skyblock.adaptive.processing_success", "endpoint", "errors").count());
            assertTrue(meterRegistry.summary("skyblock.adaptive.processing_duration_ms", "endpoint", "errors").count() >= 1L);
        }
    }

    private static void await(CountDownLatch latch, Duration timeout) {
        try {
            assertTrue(latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void waitFor(Check condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.ok()) {
                return;
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        throw new AssertionError("Condition not met in time");
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }
}
