package com.skyblockflipper.backend.instrumentation;

import org.slf4j.MDC;

public final class CycleContextHolder {
    public static final String MDC_KEY = "cycleId";
    private static final ThreadLocal<CycleContext> CONTEXT = new ThreadLocal<>();

    private CycleContextHolder() {}

    public static void set(CycleContext context) {
        CONTEXT.set(context);
        if (context != null) {
            MDC.put(MDC_KEY, context.getCycleId());
        } else {
            MDC.remove(MDC_KEY);
        }
    }

    public static CycleContext get() {
        return CONTEXT.get();
    }

    public static void clear() {
        CONTEXT.remove();
        MDC.remove(MDC_KEY);
    }
}
