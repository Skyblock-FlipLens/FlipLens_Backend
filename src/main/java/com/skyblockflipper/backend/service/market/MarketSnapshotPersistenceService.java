package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.instrumentation.BlockingTimeTracker;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class MarketSnapshotPersistenceService {

    private static final long SECONDS_PER_DAY = 86_400L;

    private static final TypeReference<List<AuctionMarketRecord>> AUCTIONS_TYPE = new TypeReference<>() {};
    private static final TypeReference<Map<String, BazaarMarketRecord>> BAZAAR_TYPE = new TypeReference<>() {};

    private final MarketSnapshotRepository marketSnapshotRepository;
    private final ObjectMapper objectMapper;
    private final BlockingTimeTracker blockingTimeTracker;
    private final long rawWindowSeconds;
    private final long minuteTierUpperSeconds;
    private final long twoHourTierUpperSeconds;
    private final long minuteIntervalMillis;
    private final long twoHourIntervalMillis;

    public MarketSnapshotPersistenceService(MarketSnapshotRepository marketSnapshotRepository,
                                            ObjectMapper objectMapper,
                                            BlockingTimeTracker blockingTimeTracker,
                                            SnapshotRetentionProperties retentionProperties) {
        this.marketSnapshotRepository = marketSnapshotRepository;
        this.objectMapper = objectMapper;
        this.blockingTimeTracker = blockingTimeTracker;
        SnapshotRetentionProperties configuredRetention = Objects.requireNonNull(
                retentionProperties,
                "SnapshotRetentionProperties must be injected"
        );
        this.rawWindowSeconds = sanitizeSeconds(configuredRetention.getRawWindowSeconds(), 90L);
        this.minuteTierUpperSeconds = sanitizeSeconds(configuredRetention.getMinuteTierUpperSeconds(), 30L * 60L);
        this.twoHourTierUpperSeconds = sanitizeSeconds(configuredRetention.getTwoHourTierUpperSeconds(), 12L * 60L * 60L);
        long minuteIntervalSeconds = sanitizeSeconds(configuredRetention.getMinuteIntervalSeconds(), 60L);
        long twoHourIntervalSeconds = sanitizeSeconds(configuredRetention.getTwoHourIntervalSeconds(), 2L * 60L * 60L);
        this.minuteIntervalMillis = minuteIntervalSeconds * 1_000L;
        this.twoHourIntervalMillis = twoHourIntervalSeconds * 1_000L;
    }

    private long sanitizeSeconds(long configured, long fallback) {
        if (configured <= 0L) {
            return fallback;
        }
        return configured;
    }

    public MarketSnapshot save(MarketSnapshot snapshot) {
        try {
            MarketSnapshotEntity entity = new MarketSnapshotEntity(
                    snapshot.snapshotTimestamp().toEpochMilli(),
                    snapshot.auctions().size(),
                    snapshot.bazaarProducts().size(),
                    objectMapper.writeValueAsString(snapshot.auctions()),
                    objectMapper.writeValueAsString(snapshot.bazaarProducts())
            );
            MarketSnapshotEntity saved = blockingTimeTracker.record("db.marketSnapshot.save", "db", () -> marketSnapshotRepository.save(entity));
            return toDomain(saved);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize market snapshot for persistence.", e);
        }
    }

    public Optional<MarketSnapshot> latest() {
        return blockingTimeTracker.record("db.marketSnapshot.latest", "db", () -> marketSnapshotRepository.findTopByOrderBySnapshotTimestampEpochMillisDesc().map(this::toDomain));
    }

    public Optional<MarketSnapshot> asOf(Instant asOfTimestamp) {
        if (asOfTimestamp == null) {
            return latest();
        }
        return blockingTimeTracker.record("db.marketSnapshot.asOf", "db", () -> marketSnapshotRepository
                .findTopBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisDesc(asOfTimestamp.toEpochMilli())
                .map(this::toDomain));
    }

    public List<MarketSnapshot> between(Instant fromInclusive, Instant toInclusive) {
        if (fromInclusive == null || toInclusive == null || fromInclusive.isAfter(toInclusive)) {
            return List.of();
        }
        return blockingTimeTracker.record("db.marketSnapshot.between", "db", () -> marketSnapshotRepository
                .findBySnapshotTimestampEpochMillisBetweenOrderBySnapshotTimestampEpochMillisAsc(
                        fromInclusive.toEpochMilli(),
                        toInclusive.toEpochMilli()
                )
                .stream()
                .map(this::toDomain)
                .toList());
    }

    public SnapshotCompactionResult compactSnapshots() {
        return compactSnapshots(Instant.now());
    }

    public SnapshotCompactionResult compactSnapshots(Instant now) {
        Instant safeNow = now == null ? Instant.now() : now;
        long nowMillis = safeNow.toEpochMilli();
        long compactionCandidateUpperBound = nowMillis - (rawWindowSeconds * 1_000L);

        List<MarketSnapshotEntity> candidates = blockingTimeTracker.record("db.marketSnapshot.compactionCandidates", "db", () -> marketSnapshotRepository
                .findBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisAsc(compactionCandidateUpperBound));
        if (candidates.isEmpty()) {
            return new SnapshotCompactionResult(0, 0, 0);
        }

        Map<Long, UUID> minuteKeepers = new LinkedHashMap<>();
        Map<Long, UUID> twoHourKeepers = new LinkedHashMap<>();
        Map<Long, UUID> dailyKeepers = new LinkedHashMap<>();
        List<UUID> toDelete = new ArrayList<>();

        for (MarketSnapshotEntity entity : candidates) {
            long snapshotMillis = entity.getSnapshotTimestampEpochMillis();
            long ageSeconds = Math.max(0L, (nowMillis - snapshotMillis) / 1_000L);

            if (ageSeconds <= minuteTierUpperSeconds) {
                long slot = Math.floorDiv(snapshotMillis, minuteIntervalMillis);
                if (minuteKeepers.putIfAbsent(slot, entity.getId()) != null) {
                    toDelete.add(entity.getId());
                }
                continue;
            }

            if (ageSeconds <= twoHourTierUpperSeconds) {
                long slot = Math.floorDiv(snapshotMillis, twoHourIntervalMillis);
                if (twoHourKeepers.putIfAbsent(slot, entity.getId()) != null) {
                    toDelete.add(entity.getId());
                }
                continue;
            }

            long epochDay = Math.floorDiv(snapshotMillis / 1_000L, SECONDS_PER_DAY);
            if (dailyKeepers.putIfAbsent(epochDay, entity.getId()) != null) {
                toDelete.add(entity.getId());
            }
        }

        if (!toDelete.isEmpty()) {
            blockingTimeTracker.recordRunnable("db.marketSnapshot.deleteBatch", "db", () -> marketSnapshotRepository.deleteAllByIdInBatch(toDelete));
        }

        int keptCount = candidates.size() - toDelete.size();
        return new SnapshotCompactionResult(candidates.size(), toDelete.size(), keptCount);
    }

    private MarketSnapshot toDomain(MarketSnapshotEntity entity) {
        try {
            List<AuctionMarketRecord> auctions = objectMapper.readValue(entity.getAuctionsJson(), AUCTIONS_TYPE);
            Map<String, BazaarMarketRecord> bazaar = objectMapper.readValue(entity.getBazaarProductsJson(), BAZAAR_TYPE);
            return new MarketSnapshot(Instant.ofEpochMilli(entity.getSnapshotTimestampEpochMillis()), auctions, bazaar);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to deserialize market snapshot from persistence.", e);
        }
    }

    public record SnapshotCompactionResult(
            int scannedCount,
            int deletedCount,
            int keptCount
    ) {
    }
}
