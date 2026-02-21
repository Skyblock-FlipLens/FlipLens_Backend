package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.api.MarketplaceType;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Service
public class ItemMarketplaceService {

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final FlipRepository flipRepository;

    public ItemMarketplaceService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                  FlipRepository flipRepository) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.flipRepository = flipRepository;
    }

    public Map<String, MarketplaceType> resolveMarketplaces(Collection<Item> items) {
        if (items == null || items.isEmpty()) {
            return Map.of();
        }

        Set<String> bazaarSignals = new HashSet<>();
        Set<String> auctionSignals = new HashSet<>();

        Optional<MarketSnapshot> latestSnapshot = marketSnapshotPersistenceService.latest();
        latestSnapshot.ifPresent(snapshot -> {
            snapshot.bazaarProducts().keySet().stream()
                    .map(this::normalize)
                    .filter(Objects::nonNull)
                    .forEach(bazaarSignals::add);
            snapshot.auctions().forEach(auction ->
                    addSignal(auctionSignals, auction.itemName()));
        });

        flipRepository.findMaxSnapshotTimestampEpochMillis().ifPresent(snapshotEpoch ->
                flipRepository.findByFlipTypeAndSnapshotTimestampEpochMillis(FlipType.AUCTION, snapshotEpoch)
                        .stream()
                        .map(Flip::getResultItemId)
                        .forEach(resultItemId -> addSignal(auctionSignals, resultItemId))
        );

        Map<String, MarketplaceType> result = new HashMap<>();
        for (Item item : items) {
            if (item == null || item.getId() == null) {
                continue;
            }
            boolean bazaar = hasSignal(item, bazaarSignals);
            boolean auction = hasSignal(item, auctionSignals);
            result.put(item.getId(), resolveMarketplace(bazaar, auction));
        }
        return result;
    }

    private boolean hasSignal(Item item, Set<String> signals) {
        String id = normalize(item.getId());
        String displayName = normalize(item.getDisplayName());
        String minecraftId = normalize(item.getMinecraftId());

        return (id != null && signals.contains(id))
                || (displayName != null && signals.contains(displayName))
                || (minecraftId != null && signals.contains(minecraftId));
    }

    private MarketplaceType resolveMarketplace(boolean bazaar, boolean auction) {
        if (bazaar && auction) {
            return MarketplaceType.BOTH;
        }
        if (bazaar) {
            return MarketplaceType.BAZAAR;
        }
        if (auction) {
            return MarketplaceType.AUCTION_HOUSE;
        }
        return MarketplaceType.NONE;
    }

    private void addSignal(Set<String> set, String value) {
        String normalized = normalize(value);
        if (normalized == null) {
            return;
        }
        set.add(normalized);
        set.add(normalized.replace("_", "").replace(" ", ""));
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }
}
