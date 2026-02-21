package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.BazaarOrderBookDto;
import com.skyblockflipper.backend.api.BazaarProductDto;
import com.skyblockflipper.backend.api.BazaarQuickFlipDto;
import com.skyblockflipper.backend.hypixel.HypixelClient;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarSummaryEntry;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Service
public class BazaarReadService {

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final HypixelClient hypixelClient;
    private final ItemRepository itemRepository;

    public BazaarReadService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                             HypixelClient hypixelClient,
                             ItemRepository itemRepository) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.hypixelClient = hypixelClient;
        this.itemRepository = itemRepository;
    }

    @Transactional(readOnly = true)
    public Optional<BazaarProductDto> getProduct(String itemId) {
        String normalized = normalize(itemId);
        if (normalized.isEmpty()) {
            return Optional.empty();
        }
        return marketSnapshotPersistenceService.latest()
                .map(MarketSnapshot::bazaarProducts)
                .map(products -> products.get(normalized))
                .map(this::toProductDto);
    }

    public BazaarOrderBookDto getOrderBook(String itemId, int depth) {
        String normalized = normalize(itemId);
        int safeDepth = Math.max(1, depth);
        BazaarResponse response = hypixelClient.fetchBazaar();
        if (response == null || response.getProducts() == null) {
            return new BazaarOrderBookDto(List.of(), List.of());
        }
        BazaarProduct product = response.getProducts().get(normalized);
        if (product == null) {
            return new BazaarOrderBookDto(List.of(), List.of());
        }
        return new BazaarOrderBookDto(
                toLevels(product.getSellSummary(), safeDepth),
                toLevels(product.getBuySummary(), safeDepth)
        );
    }

    @Transactional(readOnly = true)
    public List<BazaarQuickFlipDto> quickFlips(Double minSpreadPct, int limit) {
        double safeMinSpreadPct = minSpreadPct == null ? 0D : Math.max(0D, minSpreadPct);
        int safeLimit = Math.max(1, limit);
        Optional<MarketSnapshot> latest = marketSnapshotPersistenceService.latest();
        if (latest.isEmpty()) {
            return List.of();
        }

        Map<String, String> displayById = itemRepository.findAll().stream()
                .collect(java.util.stream.Collectors.toMap(
                        item -> item.getId().toUpperCase(Locale.ROOT),
                        item -> item.getDisplayName() == null ? item.getId() : item.getDisplayName(),
                        (left, right) -> left
                ));

        return latest.get().bazaarProducts().values().stream()
                .map(record -> toQuickFlip(record, displayById))
                .filter(dto -> dto.spreadPct() >= safeMinSpreadPct)
                .sorted(Comparator.comparingDouble(BazaarQuickFlipDto::spreadPct).reversed())
                .limit(safeLimit)
                .toList();
    }

    private BazaarProductDto toProductDto(BazaarMarketRecord record) {
        return new BazaarProductDto(
                record.productId(),
                round(record.buyPrice()),
                round(record.sellPrice()),
                record.buyVolume(),
                record.sellVolume(),
                record.buyOrders(),
                record.sellOrders(),
                record.buyMovingWeek(),
                record.sellMovingWeek()
        );
    }

    private BazaarQuickFlipDto toQuickFlip(BazaarMarketRecord record, Map<String, String> displayById) {
        long buy = round(record.buyPrice());
        long sell = round(record.sellPrice());
        long spread = Math.max(0L, buy - sell);
        double spreadPct = buy <= 0 ? 0D : round2((spread * 100D) / buy);
        String id = record.productId();
        String display = displayById.getOrDefault(id.toUpperCase(Locale.ROOT), id);
        return new BazaarQuickFlipDto(
                id,
                display,
                buy,
                sell,
                spread,
                spreadPct,
                Math.min(record.buyVolume(), record.sellVolume())
        );
    }

    private List<BazaarOrderBookDto.OrderLevelDto> toLevels(List<BazaarSummaryEntry> entries, int depth) {
        if (entries == null || entries.isEmpty()) {
            return List.of();
        }
        return entries.stream()
                .limit(depth)
                .map(entry -> new BazaarOrderBookDto.OrderLevelDto(
                        entry.getPricePerUnit(),
                        entry.getAmount(),
                        entry.getOrders()
                ))
                .toList();
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private long round(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0L;
        }
        return Math.round(value);
    }

    private double round2(double value) {
        return Math.round(value * 100D) / 100D;
    }
}
