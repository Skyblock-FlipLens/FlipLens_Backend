package com.skyblockflipper.backend.instrumentation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingTimeTrackerTest {

    @AfterEach
    void cleanup() {
        CycleContextHolder.clear();
    }

    @Test
    void recordReturnsValueAndCapturesPointInCycleContext() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getBlocking().setSlowThresholdMillis(10_000L);
        BlockingTimeTracker tracker = new BlockingTimeTracker(properties, Clock.systemUTC());
        CycleContext context = new CycleContext("cycle-1", Instant.now());
        CycleContextHolder.set(context);

        String result = tracker.record("load", "io", () -> "ok");

        assertEquals("ok", result);
        assertEquals(1, context.getBlockingPoints().size());
        BlockingTimeTracker.BlockingPoint point = context.getBlockingPoints().getFirst();
        assertEquals("load", point.label());
        assertEquals("io", point.category());
        assertTrue(point.blockedMillis() >= 0L);
    }

    @Test
    void recordWrapsCheckedExceptionAndStillCapturesPoint() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getBlocking().setSlowThresholdMillis(10_000L);
        BlockingTimeTracker tracker = new BlockingTimeTracker(properties, Clock.systemUTC());
        CycleContext context = new CycleContext("cycle-2", Instant.now());
        CycleContextHolder.set(context);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                tracker.record("phase", "db", () -> {
                    throw new Exception("checked");
                })
        );

        assertNotNull(exception.getCause());
        assertEquals("checked", exception.getCause().getMessage());
        assertEquals(1, context.getBlockingPoints().size());
    }

    @Test
    void recordRunnableWrapsCheckedException() {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getBlocking().setSlowThresholdMillis(10_000L);
        BlockingTimeTracker tracker = new BlockingTimeTracker(properties, Clock.systemUTC());

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                tracker.recordRunnable("phase", "network", () -> {
                    throw new Exception("checked-runnable");
                })
        );

        assertNotNull(exception.getCause());
        assertEquals("checked-runnable", exception.getCause().getMessage());
    }

    @Test
    void topBlockingPointsSortsDescendingAndAppliesLimit() {
        InstrumentationProperties properties = new InstrumentationProperties();
        BlockingTimeTracker tracker = new BlockingTimeTracker(properties, Clock.systemUTC());
        CycleContext context = new CycleContext("cycle-3", Instant.now());
        context.getBlockingPoints().add(new BlockingTimeTracker.BlockingPoint("a", "x", 10L));
        context.getBlockingPoints().add(new BlockingTimeTracker.BlockingPoint("b", "x", 50L));
        context.getBlockingPoints().add(new BlockingTimeTracker.BlockingPoint("c", "x", 30L));

        List<BlockingTimeTracker.BlockingPoint> top = tracker.topBlockingPoints(context, 2);

        assertEquals(2, top.size());
        assertEquals("b", top.get(0).label());
        assertEquals("c", top.get(1).label());
    }

    @Test
    void shouldCaptureStackRespectsRateLimitWindow() throws Exception {
        InstrumentationProperties properties = new InstrumentationProperties();
        properties.getBlocking().setStackSampleRate(1.0D);
        properties.getBlocking().setStackLogRateLimit(Duration.ofSeconds(60));
        Clock fixedClock = Clock.fixed(Instant.parse("2026-02-27T10:00:00Z"), ZoneOffset.UTC);
        BlockingTimeTracker tracker = new BlockingTimeTracker(properties, fixedClock);

        boolean first = (boolean) invokePrivate(tracker, "shouldCaptureStack");
        boolean second = (boolean) invokePrivate(tracker, "shouldCaptureStack");

        Field field = BlockingTimeTracker.class.getDeclaredField("nextStackLogEpochMillis");
        field.setAccessible(true);
        AtomicLong nextAllowed = (AtomicLong) field.get(tracker);
        nextAllowed.set(fixedClock.millis() - 1L);
        boolean third = (boolean) invokePrivate(tracker, "shouldCaptureStack");

        assertTrue(first);
        assertFalse(second);
        assertTrue(third);
    }

    private Object invokePrivate(Object target, String methodName) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }
}
