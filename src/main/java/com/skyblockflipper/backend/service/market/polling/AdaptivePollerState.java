package com.skyblockflipper.backend.service.market.polling;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AdaptivePollerState {
    private PollerMode mode = PollerMode.WARMUP;
    private long warmupStartedAtMillis = -1L;
    private long burstStartedAtMillis = -1L;
    private long lastPollAtMillis = -1L;
    private long lastChangeAtMillis = -1L;
    private long estimatedPeriodMillis;
    private long expectedChangeAtMillis = -1L;
    private long missCount;
    private long updateCount;
    private long consecutiveErrors;
}
