package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.ItemQuickStatsDto;
import com.skyblockflipper.backend.api.PriceHistoryRange;
import com.skyblockflipper.backend.api.PricePointDto;
import com.skyblockflipper.backend.api.ScorePointDto;
import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@Service
public class ItemAnalyticsService {

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final FlipRepository flipRepository;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;
    private final FlipCalculationContextService flipCalculationContextService;
    private final ItemRepository itemRepository;

    public ItemAnalyticsService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                FlipRepository flipRepository,
                                UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                                FlipCalculationContextService flipCalculationContextService,
                                ItemRepository itemRepository) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.flipRepository = flipRepository;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
        this.itemRepository = itemRepository;
    }

    @Transactional(readOnly = true)
    public List<PricePointDto> listPriceHistory(String itemId, PriceHistoryRange range) {
        String normalizedItemId = normalize(itemId);
        if (normalizedItemId.isEmpty()) {
            return List.of();
        }
        Optional<MarketSnapshot> latestSnapshot = marketSnapshotPersistenceService.latest();
        if (latestSnapshot.isEmpty()) {
            return List.of();
        }

        PriceHistoryRange safeRange = range == null ? PriceHistoryRange.D30 : range;
        Instant end = latestSnapshot.get().snapshotTimestamp();
        Instant start = end.minus(safeRange.lookback());
        List<MarketSnapshot> snapshots = marketSnapshotPersistenceService.between(start, end);
        if (snapshots.isEmpty()) {
            return List.of();
        }

        Set<String> aliases = resolveAliases(normalizedItemId);
        long bucketSizeMillis = safeRange.bucketSize().toMillis();
        Map<Long, MarketSnapshot> perBucket = new HashMap<>();
        for (MarketSnapshot snapshot : snapshots) {
            long bucket = Math.floorDiv(snapshot.snapshotTimestamp().toEpochMilli(), bucketSizeMillis) * bucketSizeMillis;
            MarketSnapshot existing = perBucket.get(bucket);
            if (existing == null || snapshot.snapshotTimestamp().isAfter(existing.snapshotTimestamp())) {
                perBucket.put(bucket, snapshot);
            }
        }

        return perBucket.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> toPricePoint(entry.getValue(), normalizedItemId, aliases))
                .filter(point -> point.buyPrice() != null || point.sellPrice() != null)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<ScorePointDto> listScoreHistory(String itemId) {
        List<PricePointDto> history = listPriceHistory(itemId, PriceHistoryRange.D30);
        if (history.isEmpty()) {
            return List.of();
        }

        long maxVolume = history.stream()
                .map(PricePointDto::volume)
                .filter(value -> value != null && value > 0)
                .max(Long::compareTo)
                .orElse(0L);

        List<ScorePointDto> result = new ArrayList<>();
        PricePointDto prev = null;
        for (PricePointDto point : history) {
            double liquidity = maxVolume <= 0 || point.volume() == null
                    ? 0D
                    : clamp((point.volume() * 100D) / maxVolume, 0D, 100D);

            double spreadPct = 0D;
            if (point.buyPrice() != null && point.sellPrice() != null && point.buyPrice() > 0) {
                spreadPct = Math.abs(point.buyPrice() - point.sellPrice()) * 100D / point.buyPrice();
            }
            double volatility = spreadPct;
            if (prev != null && prev.sellPrice() != null && point.sellPrice() != null && prev.sellPrice() > 0) {
                volatility = Math.abs(point.sellPrice() - prev.sellPrice()) * 100D / prev.sellPrice();
            }

            double risk = clamp((spreadPct * 2D) + (volatility * 3D), 0D, 100D);
            result.add(new ScorePointDto(point.timestamp(), round2(liquidity), round2(risk)));
            prev = point;
        }

        return result;
    }

    @Transactional(readOnly = true)
    public Optional<ItemQuickStatsDto> quickStats(String itemId) {
        List<PricePointDto> history30d = listPriceHistory(itemId, PriceHistoryRange.D30);
        if (history30d.isEmpty()) {
            return Optional.empty();
        }
        List<PricePointDto> history24h = listPriceHistory(itemId, PriceHistoryRange.H24);
        List<PricePointDto> history7d = listPriceHistory(itemId, PriceHistoryRange.D7);

        PricePointDto latest = history30d.getLast();
        Long spread = latest.buyPrice() == null || latest.sellPrice() == null
                ? null
                : latest.buyPrice() - latest.sellPrice();
        Double spreadPct = spread == null || latest.buyPrice() == null || latest.buyPrice() <= 0
                ? null
                : round2((spread * 100D) / latest.buyPrice());

        Long avgVolume7d = averageLong(history7d.stream()
                .map(PricePointDto::volume)
                .filter(v -> v != null && v >= 0)
                .toList());

        Long high7d = history7d.stream()
                .map(PricePointDto::buyPrice)
                .filter(v -> v != null && v > 0)
                .max(Long::compareTo)
                .orElse(null);
        Long low7d = history7d.stream()
                .map(PricePointDto::sellPrice)
                .filter(v -> v != null && v > 0)
                .min(Long::compareTo)
                .orElse(null);

        return Optional.of(new ItemQuickStatsDto(
                latest.buyPrice(),
                latest.sellPrice(),
                percentageChange(history24h, PricePointDto::buyPrice),
                percentageChange(history24h, PricePointDto::sellPrice),
                spread,
                spreadPct,
                latest.volume(),
                avgVolume7d,
                high7d,
                low7d
        ));
    }

    @Transactional(readOnly = true)
    public Page<UnifiedFlipDto> listFlipsForItem(String itemId, Pageable pageable) {
        String normalized = normalize(itemId);
        if (normalized.isEmpty()) {
            return new PageImpl<>(List.of(), pageable, 0);
        }
        Long snapshotEpoch = flipRepository.findMaxSnapshotTimestampEpochMillis().orElse(null);
        if (snapshotEpoch == null) {
            return new PageImpl<>(List.of(), pageable, 0);
        }

        Set<String> aliases = resolveAliases(normalized);
        Instant snapshotTimestamp = Instant.ofEpochMilli(snapshotEpoch);
        FlipCalculationContext context = flipCalculationContextService.loadContextAsOf(snapshotTimestamp);

        List<UnifiedFlipDto> values = flipRepository.findAllBySnapshotTimestampEpochMillis(snapshotEpoch).stream()
                .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                .filter(dto -> dto != null && (matches(dto.inputItems(), aliases) || matches(dto.outputItems(), aliases)))
                .toList();

        return paginate(values, pageable);
    }

    private PricePointDto toPricePoint(MarketSnapshot snapshot, String itemId, Set<String> aliases) {
        BazaarMarketRecord bazaar = snapshot.bazaarProducts().get(itemId);
        if (bazaar != null) {
            return new PricePointDto(
                    snapshot.snapshotTimestamp(),
                    roundToLong(bazaar.buyPrice()),
                    roundToLong(bazaar.sellPrice()),
                    bazaar.buyVolume()
            );
        }

        List<AuctionMarketRecord> auctions = snapshot.auctions().stream()
                .filter(auction -> matchesAuction(auction, aliases))
                .toList();
        if (auctions.isEmpty()) {
            return new PricePointDto(snapshot.snapshotTimestamp(), null, null, 0L);
        }

        long buy = auctions.stream().map(AuctionMarketRecord::startingBid).min(Long::compareTo).orElse(0L);
        long sell = auctions.stream()
                .map(auction -> auction.highestBidAmount() > 0 ? auction.highestBidAmount() : auction.startingBid())
                .mapToLong(Long::longValue)
                .sum() / auctions.size();

        return new PricePointDto(snapshot.snapshotTimestamp(), buy, sell, (long) auctions.size());
    }

    private boolean matchesAuction(AuctionMarketRecord auction, Set<String> aliases) {
        if (auction == null || auction.itemName() == null) {
            return false;
        }
        String normalized = normalize(auction.itemName());
        String compact = normalized.replace("_", "").replace(" ", "");
        return aliases.stream().anyMatch(alias ->
                normalized.equals(alias)
                        || compact.equals(alias)
                        || normalized.contains(alias)
                        || compact.contains(alias)
        );
    }

    private Set<String> resolveAliases(String itemId) {
        Set<String> aliases = new HashSet<>();
        addAlias(aliases, itemId);
        itemRepository.findById(itemId).ifPresent(item -> {
            addAlias(aliases, item.getId());
            addAlias(aliases, item.getDisplayName());
            addAlias(aliases, item.getMinecraftId());
        });
        return aliases;
    }

    private void addAlias(Set<String> aliases, String value) {
        String normalized = normalize(value);
        if (normalized.isEmpty()) {
            return;
        }
        aliases.add(normalized);
        aliases.add(normalized.replace("_", "").replace(" ", ""));
    }

    private boolean matches(Collection<UnifiedFlipDto.ItemStackDto> stacks, Set<String> aliases) {
        if (stacks == null || stacks.isEmpty()) {
            return false;
        }
        for (UnifiedFlipDto.ItemStackDto stack : stacks) {
            if (stack == null) {
                continue;
            }
            String normalized = normalize(stack.itemId());
            String compact = normalized.replace("_", "").replace(" ", "");
            if (aliases.stream().anyMatch(alias ->
                    normalized.equals(alias)
                            || compact.equals(alias)
                            || normalized.contains(alias)
                            || compact.contains(alias))) {
                return true;
            }
        }
        return false;
    }

    private <T> Double percentageChange(List<PricePointDto> history, Function<PricePointDto, T> getter) {
        if (history == null || history.size() < 2) {
            return null;
        }
        T first = getter.apply(history.getFirst());
        T last = getter.apply(history.getLast());
        if (!(first instanceof Number firstN) || !(last instanceof Number lastN)) {
            return null;
        }
        double firstVal = firstN.doubleValue();
        double lastVal = lastN.doubleValue();
        if (firstVal <= 0D) {
            return null;
        }
        return round2(((lastVal - firstVal) / firstVal) * 100D);
    }

    private Long averageLong(List<Long> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return Math.round(values.stream().mapToLong(Long::longValue).average().orElse(0D));
    }

    private Page<UnifiedFlipDto> paginate(List<UnifiedFlipDto> values, Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            return new PageImpl<>(values);
        }
        int fromIndex = (int) Math.min((long) pageable.getPageNumber() * pageable.getPageSize(), values.size());
        int toIndex = Math.min(fromIndex + pageable.getPageSize(), values.size());
        List<UnifiedFlipDto> content = fromIndex >= toIndex ? List.of() : values.subList(fromIndex, toIndex);
        return new PageImpl<>(content, pageable, values.size());
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private double round2(double value) {
        return Math.round(value * 100D) / 100D;
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private long roundToLong(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0L;
        }
        return Math.round(value);
    }
}
