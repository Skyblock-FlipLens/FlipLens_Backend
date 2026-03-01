package com.skyblockflipper.backend.service.market.polling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PollStateTest {

    @Test
    void auctionPollStateDefaultsAndMutationsWork() {
        AuctionPollState state = new AuctionPollState();

        assertEquals(-1L, state.getLastSeenLastUpdated());
        assertEquals(20_000L, state.getEwmaPeriodMs());
        assertEquals(-1L, state.getNextExpectedAtMs());
        assertEquals(AuctionPollState.Phase.SLEEP, state.getPhase());
        assertEquals(1_000L, state.getProbeIntervalMs());
        assertEquals(0, state.getProbeBackoffStep());

        state.setLastSeenLastUpdated(123L);
        state.setEwmaPeriodMs(21_000L);
        state.setNextExpectedAtMs(999L);
        state.setPhase(AuctionPollState.Phase.PROBE);
        state.setProbeIntervalMs(2_000L);
        state.setProbeBackoffStep(3);

        assertEquals(123L, state.getLastSeenLastUpdated());
        assertEquals(21_000L, state.getEwmaPeriodMs());
        assertEquals(999L, state.getNextExpectedAtMs());
        assertEquals(AuctionPollState.Phase.PROBE, state.getPhase());
        assertEquals(2_000L, state.getProbeIntervalMs());
        assertEquals(3, state.getProbeBackoffStep());
    }

    @Test
    void bazaarPollStateDefaultsAndMutationsWork() {
        BazaarPollState state = new BazaarPollState();

        assertEquals(-1L, state.getLastSeenLastUpdated());
        assertEquals(60_000L, state.getEwmaPeriodMs());
        assertEquals(-1L, state.getNextExpectedAtMs());
        assertEquals(BazaarPollState.Phase.SLEEP, state.getPhase());
        assertEquals(1_000L, state.getProbeIntervalMs());
        assertEquals(0, state.getProbeBackoffStep());

        state.setLastSeenLastUpdated(456L);
        state.setEwmaPeriodMs(61_000L);
        state.setNextExpectedAtMs(1_234L);
        state.setPhase(BazaarPollState.Phase.COMMIT_IN_FLIGHT);
        state.setProbeIntervalMs(1_500L);
        state.setProbeBackoffStep(2);

        assertEquals(456L, state.getLastSeenLastUpdated());
        assertEquals(61_000L, state.getEwmaPeriodMs());
        assertEquals(1_234L, state.getNextExpectedAtMs());
        assertEquals(BazaarPollState.Phase.COMMIT_IN_FLIGHT, state.getPhase());
        assertEquals(1_500L, state.getProbeIntervalMs());
        assertEquals(2, state.getProbeBackoffStep());
    }
}

