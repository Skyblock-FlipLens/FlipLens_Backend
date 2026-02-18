package com.skyblockflipper.backend.instrumentation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class BlockingTimeTracker {

    private final InstrumentationProperties properties;
    private final Clock clock;
    private final AtomicLong nextStackLogEpochMillis = new AtomicLong(0);

    @Autowired
    public BlockingTimeTracker(InstrumentationProperties properties) {
        this(properties, Clock.systemUTC());
    }

    BlockingTimeTracker(InstrumentationProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    public <T> T record(String label, String category, CheckedSupplier<T> supplier) {
        long start = System.nanoTime();
        try {
            return supplier.get();
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        } finally {
            long elapsed = (System.nanoTime() - start) / 1_000_000L;
            capture(label, category, elapsed);
        }
    }

    public void recordRunnable(String label, String category, CheckedRunnable runnable) {
        long start = System.nanoTime();
        try {
            runnable.run();
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        } finally {
            long elapsed = (System.nanoTime() - start) / 1_000_000L;
            capture(label, category, elapsed);
        }
    }

    private void capture(String label, String category, long blockedMillis) {
        CycleContext context = CycleContextHolder.get();
        if (context != null) {
            context.getBlockingPoints().add(new BlockingPoint(label, category, blockedMillis));
        }
        if (blockedMillis < properties.getBlocking().getSlowThresholdMillis()) {
            return;
        }
        if (!shouldCaptureStack()) {
            return;
        }
        log.warn("slow_blocking_point label={} category={} blockedMillis={} stack={}",
                label,
                category,
                blockedMillis,
                StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                        .walk(stream -> stream.limit(12).map(StackWalker.StackFrame::toString).toList()));
    }

    private boolean shouldCaptureStack() {
        if (ThreadLocalRandom.current().nextDouble() > properties.getBlocking().getStackSampleRate()) {
            return false;
        }
        long now = clock.millis();
        long nextAllowed = nextStackLogEpochMillis.get();
        if (now < nextAllowed) {
            return false;
        }
        return nextStackLogEpochMillis.compareAndSet(nextAllowed,
                now + properties.getBlocking().getStackLogRateLimit().toMillis());
    }

    public List<BlockingPoint> topBlockingPoints(CycleContext context, int limit) {
        return context.getBlockingPoints()
                .stream()
                .sorted(Comparator.comparingLong(BlockingPoint::blockedMillis).reversed())
                .limit(limit)
                .toList();
    }

    public record BlockingPoint(String label, String category, long blockedMillis) {}

    @FunctionalInterface
    public interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }
}
