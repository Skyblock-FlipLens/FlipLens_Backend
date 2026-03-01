package com.skyblockflipper.backend.config.properties;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AdaptivePollingPropertiesTest {

    @Test
    void validatePassesWithDefaults() {
        AdaptivePollingProperties properties = new AdaptivePollingProperties();

        assertDoesNotThrow(properties::validate);
    }

    @Test
    void validateFailsWhenEndpointIsNull() {
        AdaptivePollingProperties properties = new AdaptivePollingProperties();
        properties.setAuctions(null);

        assertThrows(IllegalStateException.class, properties::validate);
    }

    @Test
    void validateFailsWhenMinGuardWindowExceedsMax() {
        AdaptivePollingProperties properties = new AdaptivePollingProperties();
        AdaptivePollingProperties.Endpoint endpoint = AdaptivePollingProperties.Endpoint.defaults(
                "auctions",
                "/skyblock/auctions",
                Duration.ofSeconds(20)
        );
        endpoint.setMinGuardWindowMs(1_500);
        endpoint.setMaxGuardWindowMs(1_000);
        properties.setAuctions(endpoint);

        assertThrows(IllegalStateException.class, properties::validate);
    }

    @Test
    void validateFailsWhenGuardWindowOutsideBounds() {
        AdaptivePollingProperties properties = new AdaptivePollingProperties();
        AdaptivePollingProperties.Endpoint endpoint = AdaptivePollingProperties.Endpoint.defaults(
                "auctions",
                "/skyblock/auctions",
                Duration.ofSeconds(20)
        );
        endpoint.setMinGuardWindowMs(400);
        endpoint.setMaxGuardWindowMs(1_200);
        endpoint.setGuardWindowMs(200);
        properties.setAuctions(endpoint);

        assertThrows(IllegalStateException.class, properties::validate);
    }

    @Test
    void validateFailsWhenMinPeriodMultiplierExceedsMax() {
        AdaptivePollingProperties properties = new AdaptivePollingProperties();
        AdaptivePollingProperties.Endpoint endpoint = AdaptivePollingProperties.Endpoint.defaults(
                "bazaar",
                "/skyblock/bazaar",
                Duration.ofSeconds(60)
        );
        endpoint.setMinPeriodMultiplier(2.0d);
        endpoint.setMaxPeriodMultiplier(1.5d);
        properties.setBazaar(endpoint);

        assertThrows(IllegalStateException.class, properties::validate);
    }

    @Test
    void validateFailsWhenBurstWindowIsSmallerThanBurstInterval() {
        AdaptivePollingProperties properties = new AdaptivePollingProperties();
        AdaptivePollingProperties.Endpoint endpoint = AdaptivePollingProperties.Endpoint.defaults(
                "bazaar",
                "/skyblock/bazaar",
                Duration.ofSeconds(60)
        );
        endpoint.setBurstIntervalMs(2_000);
        endpoint.setBurstWindowMs(1_000);
        properties.setBazaar(endpoint);

        assertThrows(IllegalStateException.class, properties::validate);
    }
}

