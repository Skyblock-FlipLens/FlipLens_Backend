package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.model.ElectionSnapshot;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import com.skyblockflipper.backend.repository.ElectionSnapshotRepository;
import com.skyblockflipper.backend.service.market.MarketTimescaleFeatureService;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

@Service
@Slf4j
public class FlipCalculationContextService {

    private static final double STANDARD_BAZAAR_TAX = 0.0125D;
    private static final double STANDARD_AUCTION_TAX_MULTIPLIER = 1.0D;
    private static final double DERPY_AUCTION_TAX_MULTIPLIER = 4.0D;
    // SkyBlock mayor election cycle is 124 in-game hours.
    private static final Duration DEFAULT_ELECTION_CACHE_MAX_AGE = Duration.ofHours(124);

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final UnifiedFlipInputMapper unifiedFlipInputMapper;
    private final MarketTimescaleFeatureService marketTimescaleFeatureService;
    private final HypixelClient hypixelClient;
    private final ElectionSnapshotRepository electionSnapshotRepository;
    private final ObjectMapper objectMapper;
    private final Object electionCacheLock = new Object();
    @Value("${config.hypixel.polling.election-max-age:PT124H}")
    private Duration electionCacheMaxAge = DEFAULT_ELECTION_CACHE_MAX_AGE;
    private volatile CachedElection cachedElection;

    public FlipCalculationContextService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                         UnifiedFlipInputMapper unifiedFlipInputMapper,
                                         MarketTimescaleFeatureService marketTimescaleFeatureService,
                                         HypixelClient hypixelClient,
                                         ElectionSnapshotRepository electionSnapshotRepository,
                                         ObjectMapper objectMapper) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.unifiedFlipInputMapper = unifiedFlipInputMapper;
        this.marketTimescaleFeatureService = marketTimescaleFeatureService;
        this.hypixelClient = hypixelClient;
        this.electionSnapshotRepository = electionSnapshotRepository;
        this.objectMapper = objectMapper;
    }

    public FlipCalculationContext loadCurrentContext() {
        MarketSnapshot latestMarketSnapshot = marketSnapshotPersistenceService.latest().orElse(null);
        return buildContext(latestMarketSnapshot, Instant.now(), resolveLiveElection());
    }

    public FlipCalculationContext loadContextAsOf(Instant asOfTimestamp) {
        Instant requiredAsOfTimestamp = Objects.requireNonNull(asOfTimestamp, "asOfTimestamp must not be null");
        MarketSnapshot marketSnapshotAsOf = marketSnapshotPersistenceService.asOf(requiredAsOfTimestamp).orElse(null);
        return buildContext(marketSnapshotAsOf, requiredAsOfTimestamp, resolveHistoricalElection(requiredAsOfTimestamp));
    }

    private FlipCalculationContext buildContext(MarketSnapshot marketSnapshotDomain,
                                                Instant snapshotTimestamp,
                                                ElectionResolution electionResolution) {
        UnifiedFlipInputSnapshot marketSnapshot = marketSnapshotDomain == null
                ? new UnifiedFlipInputSnapshot(snapshotTimestamp, null, null)
                : unifiedFlipInputMapper.map(marketSnapshotDomain);
        FlipScoreFeatureSet scoreFeatures = marketSnapshotDomain == null
                ? FlipScoreFeatureSet.empty()
                : marketTimescaleFeatureService.computeFor(marketSnapshotDomain);

        return new FlipCalculationContext(
                marketSnapshot,
                STANDARD_BAZAAR_TAX,
                electionResolution.auctionTaxMultiplier(),
                electionResolution.partial(),
                scoreFeatures
        );
    }

    private ElectionResolution resolveLiveElection() {
        return resolveElection(loadLiveElection());
    }

    private ElectionResolution resolveHistoricalElection(Instant asOfTimestamp) {
        long maxAgeMillis = sanitizeDurationMillis(electionCacheMaxAge, DEFAULT_ELECTION_CACHE_MAX_AGE);
        return electionSnapshotRepository.findFirstByFetchedAtLessThanEqualOrderByFetchedAtDesc(asOfTimestamp)
                .filter(snapshot -> snapshot.getFetchedAt() != null)
                .filter(snapshot -> !asOfTimestamp.isAfter(snapshot.getFetchedAt().plusMillis(maxAgeMillis)))
                .map(this::parsePersistedElection)
                .map(this::resolveElection)
                .orElseGet(ElectionResolution::partialElection);
    }

    private JsonNode parsePersistedElection(ElectionSnapshot snapshot) {
        String payloadJson = snapshot.getPayloadJson();
        if (payloadJson == null || payloadJson.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readTree(payloadJson);
        } catch (Exception e) {
            log.warn("Failed to parse persisted election snapshot at {}: {}", snapshot.getFetchedAt(), e.getMessage());
            return null;
        }
    }

    private ElectionResolution resolveElection(JsonNode election) {
        if (election == null) {
            return ElectionResolution.partialElection();
        }

        double auctionTaxMultiplier = hasDerpyQuadTaxes(election)
                ? DERPY_AUCTION_TAX_MULTIPLIER
                : STANDARD_AUCTION_TAX_MULTIPLIER;
        return new ElectionResolution(auctionTaxMultiplier, false);
    }

    private JsonNode loadLiveElection() {
        Instant now = Instant.now();
        long maxAgeMillis = sanitizeDurationMillis(electionCacheMaxAge, DEFAULT_ELECTION_CACHE_MAX_AGE);
        CachedElection local = cachedElection;
        if (local != null && !now.isAfter(local.fetchedAt().plusMillis(maxAgeMillis))) {
            return local.payload();
        }

        synchronized (electionCacheLock) {
            now = Instant.now();
            local = cachedElection;
            if (local != null && !now.isAfter(local.fetchedAt().plusMillis(maxAgeMillis))) {
                return local.payload();
            }

            JsonNode fetched = hypixelClient.fetchElection();
            cachedElection = new CachedElection(now, fetched);
            return fetched;
        }
    }

    private long sanitizeDurationMillis(Duration configured, Duration fallback) {
        Duration safe = configured == null || configured.isNegative() || configured.isZero() ? fallback : configured;
        return safe.toMillis();
    }

    private boolean hasDerpyQuadTaxes(JsonNode election) {
        JsonNode mayor = resolveActiveMayor(election);
        if (mayor == null || mayor.isMissingNode()) {
            return false;
        }

        String mayorName = mayor.path("name").asString("");
        if (!"Derpy".equalsIgnoreCase(mayorName)) {
            return false;
        }

        JsonNode perks = mayor.path("perks");
        if (!perks.isArray()) {
            return false;
        }
        for (JsonNode perk : perks) {
            String perkName = perk.path("name").asString("");
            String description = perk.path("description").asString("");
            String lowerName = perkName.toLowerCase();
            String lowerDescription = description.toLowerCase();
            if (lowerName.contains("quad") && lowerName.contains("tax")) {
                return true;
            }
            if (lowerDescription.contains("quad") && lowerDescription.contains("tax")) {
                return true;
            }
        }
        return false;
    }

    private JsonNode resolveActiveMayor(JsonNode election) {
        JsonNode mayor = election.path("mayor");
        if (mayor.isObject()) {
            return mayor;
        }

        JsonNode currentMayor = election.path("current").path("mayor");
        if (currentMayor.isObject()) {
            return currentMayor;
        }

        JsonNode candidates = election.path("current").path("candidates");
        if (!candidates.isArray()) {
            return null;
        }
        for (JsonNode candidate : candidates) {
            if (candidate.path("elected").asBoolean(false)) {
                return candidate;
            }
        }
        return null;
    }

    private record CachedElection(
            Instant fetchedAt,
            JsonNode payload
    ) {
    }

    private record ElectionResolution(
            double auctionTaxMultiplier,
            boolean partial
    ) {
        private static ElectionResolution partialElection() {
            return new ElectionResolution(STANDARD_AUCTION_TAX_MULTIPLIER, true);
        }
    }
}
