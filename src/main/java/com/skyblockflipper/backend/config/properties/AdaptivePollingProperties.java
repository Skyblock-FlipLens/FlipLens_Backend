package com.skyblockflipper.backend.config.properties;

import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "config.hypixel.adaptive")
public class AdaptivePollingProperties {

    private boolean enabled = true;

    @DecimalMin("0.1")
    private double globalMaxRequestsPerSecond = 3.0d;

    @Valid
    @NotNull
    private Endpoint auctions = Endpoint.defaults("auctions", "/skyblock/auctions", Duration.ofSeconds(20));

    @Valid
    @NotNull
    private Endpoint bazaar = Endpoint.defaults("bazaar", "/skyblock/bazaar", Duration.ofSeconds(60));

    @Valid
    @NotNull
    private Pipeline pipeline = new Pipeline();

    @Valid
    @NotNull
    private Debug debug = new Debug();

    @PostConstruct
    void validate() {
        validateEndpoint("auctions", auctions);
        validateEndpoint("bazaar", bazaar);
    }

    private void validateEndpoint(String endpointName, Endpoint endpoint) {
        if (endpoint == null) {
            throw new IllegalStateException("config.hypixel.adaptive." + endpointName + " must not be null");
        }
        if (endpoint.getMinGuardWindowMs() > endpoint.getMaxGuardWindowMs()) {
            throw new IllegalStateException("Invalid " + endpointName + " config: minGuardWindowMs must be <= maxGuardWindowMs");
        }
        if (endpoint.getGuardWindowMs() < endpoint.getMinGuardWindowMs()
                || endpoint.getGuardWindowMs() > endpoint.getMaxGuardWindowMs()) {
            throw new IllegalStateException("Invalid " + endpointName + " config: guardWindowMs must be between minGuardWindowMs and maxGuardWindowMs");
        }
        if (endpoint.getMinPeriodMultiplier() > endpoint.getMaxPeriodMultiplier()) {
            throw new IllegalStateException("Invalid " + endpointName + " config: minPeriodMultiplier must be <= maxPeriodMultiplier");
        }
        if (endpoint.getBurstWindowMs() < endpoint.getBurstIntervalMs()) {
            throw new IllegalStateException("Invalid " + endpointName + " config: burstWindowMs must be >= burstIntervalMs");
        }
    }

    @Getter
    @Setter
    public static class Pipeline {
        @Min(1)
        private int queueCapacity = 1;
        private boolean coalesceEnabled = true;
    }

    @Getter
    @Setter
    public static class Endpoint {
        @NotNull
        private String name;
        @NotNull
        private String path;
        @NotNull
        private Duration periodHint;
        @NotNull
        private Duration warmupInterval = Duration.ofSeconds(2);
        @Min(1)
        private int warmupMaxSeconds = 90;
        @Min(1)
        private long guardWindowMs = 400;
        @Min(1)
        private long minGuardWindowMs = 250;
        @Min(1)
        private long maxGuardWindowMs = 1200;
        @Min(1)
        private long burstIntervalMs = 500;
        @Min(1)
        private long burstWindowMs = 4000;
        @NotNull
        private Duration backoffInterval = Duration.ofSeconds(2);
        @DecimalMin("0.1")
        private double maxBurstRate = 2.0d;
        @NotNull
        private Duration requestTimeout = Duration.ofSeconds(8);
        @NotNull
        private Duration connectTimeout = Duration.ofSeconds(2);
        @Min(3)
        private int estimatorWindowSize = 7;
        @DecimalMin("0.01")
        private double emaAlpha = 0.25d;
        @DecimalMin("0.1")
        private double minPeriodMultiplier = 0.6d;
        @DecimalMin("0.5")
        private double maxPeriodMultiplier = 1.8d;
        @Min(0)
        private int transientRetries = 2;

        public static Endpoint defaults(String name, String path, Duration periodHint) {
            Endpoint endpoint = new Endpoint();
            endpoint.name = name;
            endpoint.path = path;
            endpoint.periodHint = periodHint;
            return endpoint;
        }
    }

    @Getter
    @Setter
    public static class Debug {
        private boolean logPhaseTransitions = false;
        private boolean logLastUpdated = false;
        private boolean logCommitStats = false;
    }
}
