package com.skyblockflipper.backend.instrumentation;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CycleInstrumentationServiceTest {

    @AfterEach
    void clearContext() {
        CycleContextHolder.clear();
    }

    @Test
    void startCycleCreatesContextAndStoresItInHolder() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        BlockingTimeTracker blockingTimeTracker = mock(BlockingTimeTracker.class);
        CycleInstrumentationService service = new CycleInstrumentationService(meterRegistry, blockingTimeTracker);

        CycleContext context = service.startCycle();

        assertNotNull(context);
        assertNotNull(context.getCycleId());
        assertEquals(context, CycleContextHolder.get());
    }

    @Test
    void endPhaseWithoutContextIsNoOp() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        BlockingTimeTracker blockingTimeTracker = mock(BlockingTimeTracker.class);
        CycleInstrumentationService service = new CycleInstrumentationService(meterRegistry, blockingTimeTracker);

        service.endPhase("fetch", System.nanoTime(), true, 123L);

        assertNull(meterRegistry.find("skyblock.polling.phase").timer());
    }

    @Test
    void endPhaseWithContextRecordsTimerAndPayload() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        BlockingTimeTracker blockingTimeTracker = mock(BlockingTimeTracker.class);
        CycleInstrumentationService service = new CycleInstrumentationService(meterRegistry, blockingTimeTracker);
        CycleContext context = service.startCycle();
        long start = System.nanoTime() - 1_000_000L;

        service.endPhase("total_cycle", start, true, 456L);

        assertEquals(456L, context.getPayloadBytes());
        assertTrue(context.getPhaseMillis().containsKey("total_cycle"));
        assertEquals(
                1L,
                meterRegistry.get("skyblock.polling.phase")
                        .tag("phase", "total_cycle")
                        .timer()
                        .count()
        );
    }

    @Test
    void finishCycleUsesBlockingTrackerAndClearsContext() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        BlockingTimeTracker blockingTimeTracker = mock(BlockingTimeTracker.class);
        when(blockingTimeTracker.topBlockingPoints(any(CycleContext.class), eq(5))).thenReturn(List.of());
        CycleInstrumentationService service = new CycleInstrumentationService(meterRegistry, blockingTimeTracker);
        service.startCycle();

        service.finishCycle(true);

        verify(blockingTimeTracker).topBlockingPoints(any(CycleContext.class), eq(5));
        assertNull(CycleContextHolder.get());
    }

    @Test
    void bucketCycleAndBucketPayloadPrivateHelpersCoverBranches() throws Exception {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        BlockingTimeTracker blockingTimeTracker = mock(BlockingTimeTracker.class);
        CycleInstrumentationService service = new CycleInstrumentationService(meterRegistry, blockingTimeTracker);

        assertEquals("100-199", invokePrivate(service, "bucketCycle", new Class<?>[]{String.class}, "150-foo"));
        assertEquals("0-99", invokePrivate(service, "bucketCycle", new Class<?>[]{String.class}, "bad-id"));

        assertEquals("lt_100kb", invokePrivate(service, "bucketPayload", new Class<?>[]{long.class}, 99_999L));
        assertEquals("100kb_1mb", invokePrivate(service, "bucketPayload", new Class<?>[]{long.class}, 200_000L));
        assertEquals("1mb_10mb", invokePrivate(service, "bucketPayload", new Class<?>[]{long.class}, 2_000_000L));
        assertEquals("gte_10mb", invokePrivate(service, "bucketPayload", new Class<?>[]{long.class}, 20_000_000L));
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
