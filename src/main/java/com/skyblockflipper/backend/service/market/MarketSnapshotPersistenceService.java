package com.skyblockflipper.backend.service.market;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class MarketSnapshotPersistenceService {

    private static final TypeReference<List<AuctionMarketRecord>> AUCTIONS_TYPE = new TypeReference<>() {};
    private static final TypeReference<Map<String, BazaarMarketRecord>> BAZAAR_TYPE = new TypeReference<>() {};

    private final MarketSnapshotRepository marketSnapshotRepository;
    private final ObjectMapper objectMapper;

    public MarketSnapshotPersistenceService(MarketSnapshotRepository marketSnapshotRepository, ObjectMapper objectMapper) {
        this.marketSnapshotRepository = marketSnapshotRepository;
        this.objectMapper = objectMapper;
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
            MarketSnapshotEntity saved = marketSnapshotRepository.save(entity);
            return toDomain(saved);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize market snapshot for persistence.", e);
        }
    }

    public Optional<MarketSnapshot> latest() {
        return marketSnapshotRepository.findTopByOrderBySnapshotTimestampEpochMillisDesc().map(this::toDomain);
    }

    public Optional<MarketSnapshot> asOf(Instant asOfTimestamp) {
        if (asOfTimestamp == null) {
            return latest();
        }
        return marketSnapshotRepository
                .findTopBySnapshotTimestampEpochMillisLessThanEqualOrderBySnapshotTimestampEpochMillisDesc(asOfTimestamp.toEpochMilli())
                .map(this::toDomain);
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
}
