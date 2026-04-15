package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.config.properties.FlippingModelProperties;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.AuctionComparableKey;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class UnifiedFlipInputMapper {
    private static final Pattern PET_LEVEL_PATTERN = Pattern.compile("^\\[Lvl\\s*(\\d+)]\\s*(.+)$");
    private static final Pattern INTERNAL_NAME_PATTERN = Pattern.compile("\"(?:internalname|id|item_id|itemId)\"\\s*:\\s*\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
    private final FlippingModelProperties flippingModelProperties;

    public UnifiedFlipInputMapper() {
        this(new FlippingModelProperties());
    }

    @Autowired
    public UnifiedFlipInputMapper(FlippingModelProperties flippingModelProperties) {
        this.flippingModelProperties = flippingModelProperties == null
                ? new FlippingModelProperties()
                : flippingModelProperties;
    }

    public UnifiedFlipInputSnapshot map(MarketSnapshot marketSnapshot) {
        if (marketSnapshot == null) {
            return new UnifiedFlipInputSnapshot(null, Map.of(), Map.of());
        }
        return new UnifiedFlipInputSnapshot(
                marketSnapshot.snapshotTimestamp(),
                mapBazaarQuotes(marketSnapshot.bazaarProducts()),
                mapAuctionQuotes(marketSnapshot.auctions())
        );
    }

    private Map<String, UnifiedFlipInputSnapshot.BazaarQuote> mapBazaarQuotes(
            Map<String, BazaarMarketRecord> bazaarProducts
    ) {
        if (bazaarProducts == null || bazaarProducts.isEmpty()) {
            return Map.of();
        }
        Map<String, UnifiedFlipInputSnapshot.BazaarQuote> quotes = new LinkedHashMap<>();
        for (Map.Entry<String, BazaarMarketRecord> entry : bazaarProducts.entrySet()) {
            BazaarMarketRecord record = entry.getValue();
            if (record == null || record.productId() == null || record.productId().isBlank()) {
                continue;
            }
            quotes.put(entry.getKey(), new UnifiedFlipInputSnapshot.BazaarQuote(
                    record.buyPrice(),
                    record.sellPrice(),
                    record.buyVolume(),
                    record.sellVolume(),
                    record.buyMovingWeek(),
                    record.sellMovingWeek(),
                    record.buyOrders(),
                    record.sellOrders()
            ));
        }
        return quotes;
    }

    private Map<String, UnifiedFlipInputSnapshot.AuctionQuote> mapAuctionQuotes(List<AuctionMarketRecord> auctions) {
        if (auctions == null || auctions.isEmpty()) {
            return Map.of();
        }
        if (!flippingModelProperties.isAuctionModelV2Enabled()) {
            return mapAuctionQuotesLegacy(auctions);
        }

        Map<AuctionComparableKey, AuctionAccumulator> byComparableKey = new LinkedHashMap<>();
        Map<AuctionComparableKey, String> representativeItemNames = new LinkedHashMap<>();
        for (AuctionMarketRecord record : auctions) {
            if (record == null || record.itemName() == null || record.itemName().isBlank()) {
                continue;
            }
            if (!record.bin() || record.claimed()) {
                continue;
            }
            AuctionComparableKey comparableKey = toComparableKey(record);
            byComparableKey.computeIfAbsent(comparableKey, ignored -> new AuctionAccumulator())
                    .accept(record.startingBid());
            representativeItemNames.putIfAbsent(comparableKey, record.itemName().trim());
        }

        Map<String, UnifiedFlipInputSnapshot.AuctionQuote> result = new LinkedHashMap<>();
        for (Map.Entry<AuctionComparableKey, AuctionAccumulator> entry : byComparableKey.entrySet()) {
            AuctionAccumulator acc = entry.getValue();
            if (acc.sampleSize == 0) {
                continue;
            }
            String preferredKey = representativeItemNames.getOrDefault(entry.getKey(), entry.getKey().baseId());
            String outputKey = uniqueOutputKey(result, preferredKey, entry.getKey());
            result.put(outputKey, new UnifiedFlipInputSnapshot.AuctionQuote(
                    acc.lowestStartingBid,
                    acc.secondLowestStartingBid(),
                    acc.highestObservedBid,
                    acc.averageObservedPrice(),
                    acc.medianObservedPrice(),
                    acc.p25ObservedPrice(),
                    acc.sampleSize
            ));
        }
        return result;
    }

    private Map<String, UnifiedFlipInputSnapshot.AuctionQuote> mapAuctionQuotesLegacy(List<AuctionMarketRecord> auctions) {
        Map<String, AuctionAccumulator> byItem = new LinkedHashMap<>();
        for (AuctionMarketRecord record : auctions) {
            if (record == null
                    || record.itemName() == null
                    || record.itemName().isBlank()
                    || !record.bin()
                    || record.claimed()) {
                continue;
            }
            String key = normalizeIdentifier(record.itemName());
            if (key.isBlank()) {
                key = "UNKNOWN";
            }
            byItem.computeIfAbsent(key, ignored -> new AuctionAccumulator())
                    .accept(record.startingBid());
        }

        Map<String, UnifiedFlipInputSnapshot.AuctionQuote> result = new LinkedHashMap<>();
        for (Map.Entry<String, AuctionAccumulator> entry : byItem.entrySet()) {
            AuctionAccumulator acc = entry.getValue();
            if (acc.sampleSize == 0) {
                continue;
            }
            result.put(entry.getKey(), new UnifiedFlipInputSnapshot.AuctionQuote(
                    acc.lowestStartingBid,
                    acc.secondLowestStartingBid(),
                    acc.highestObservedBid,
                    acc.averageObservedPrice(),
                    acc.medianObservedPrice(),
                    acc.p25ObservedPrice(),
                    acc.sampleSize
            ));
        }
        return result;
    }

    private String uniqueOutputKey(Map<String, UnifiedFlipInputSnapshot.AuctionQuote> existing,
                                   String preferredKey,
                                   AuctionComparableKey comparableKey) {
        String keyFromComparable = comparableKey == null ? null : comparableKey.baseId();
        String base = isBlankOrUnknown(keyFromComparable) ? preferredKey : keyFromComparable;
        if (base == null || base.isBlank()) {
            base = "UNKNOWN";
        }
        if (!existing.containsKey(base)) {
            return base;
        }
        String suffix = comparableKey.tier() + "/" + comparableKey.category();
        String withSuffix = base + " [" + suffix + "]";
        if (!existing.containsKey(withSuffix)) {
            return withSuffix;
        }
        int counter = 2;
        while (existing.containsKey(withSuffix + " #" + counter)) {
            counter++;
        }
        return withSuffix + " #" + counter;
    }

    private AuctionComparableKey toComparableKey(AuctionMarketRecord record) {
        ParsedItemName parsedItemName = parseItemName(record.itemName());
        String baseIdFromExtra = parseBaseIdFromExtra(record.extra());
        String baseId = firstNonBlank(baseIdFromExtra, normalizeIdentifier(parsedItemName.baseName()), normalizeIdentifier(record.itemName()));

        Integer stars = null;
        int starCount = countStars(record.itemName()) + countStars(record.itemLore());
        if (starCount > 0) {
            stars = starCount;
        }

        boolean recombobulated = containsIgnoreCase(record.itemLore(), "RARITY UPGRADED")
                || containsIgnoreCase(record.extra(), "recombobulated");
        boolean lowConfidence = baseIdFromExtra == null || baseIdFromExtra.isBlank();

        return new AuctionComparableKey(
                baseId,
                record.category(),
                record.tier(),
                parsedItemName.petLevel(),
                stars,
                recombobulated,
                lowConfidence
        );
    }

    private ParsedItemName parseItemName(String itemName) {
        if (itemName == null || itemName.isBlank()) {
            return new ParsedItemName("UNKNOWN", null);
        }
        String normalized = itemName.trim();
        Matcher matcher = PET_LEVEL_PATTERN.matcher(normalized);
        if (matcher.matches()) {
            Integer petLevel = tryParseInteger(matcher.group(1));
            String baseName = matcher.group(2) == null || matcher.group(2).isBlank() ? normalized : matcher.group(2).trim();
            return new ParsedItemName(baseName, petLevel);
        }
        return new ParsedItemName(normalized, null);
    }

    private String parseBaseIdFromExtra(String extra) {
        if (extra == null || extra.isBlank()) {
            return null;
        }
        String[] candidates = {extra.replace("\\\"", "\""), extra};
        for (String candidate : candidates) {
            Matcher matcher = INTERNAL_NAME_PATTERN.matcher(candidate);
            if (matcher.find()) {
                String rawValue = matcher.group(1) == null ? "" : matcher.group(1).replace("\\", "");
                String value = normalizeIdentifier(rawValue);
                if (!value.isBlank()) {
                    return value;
                }
            }
        }
        return null;
    }

    private boolean isBlankOrUnknown(String value) {
        return value == null || value.isBlank() || "UNKNOWN".equalsIgnoreCase(value.trim());
    }

    private int countStars(String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        int stars = 0;
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '\u272A') {
                stars++;
            }
        }
        return stars;
    }

    private boolean containsIgnoreCase(String text, String needle) {
        if (text == null || needle == null || needle.isBlank()) {
            return false;
        }
        return text.toLowerCase(Locale.ROOT).contains(needle.toLowerCase(Locale.ROOT));
    }

    private Integer tryParseInteger(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private String normalizeIdentifier(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        normalized = normalized.replaceAll("^\\[LVL\\s*\\d+\\]\\s*", "");
        normalized = normalized.replace(' ', '_');
        return normalized;
    }

    private String firstNonBlank(String first, String second, String third) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        if (third != null && !third.isBlank()) {
            return third;
        }
        return "UNKNOWN";
    }

    private static final class AuctionAccumulator {
        private long lowestStartingBid = Long.MAX_VALUE;
        private long highestObservedBid = 0L;
        private final List<Long> activeBinPrices = new ArrayList<>();
        private List<Long> sortedActiveBinPrices = List.of();
        private boolean sortedPricesDirty = false;
        private long observedPriceSum = 0L;
        private int sampleSize = 0;

        private void accept(long startingBid) {
            if (startingBid <= 0L) {
                return;
            }
            activeBinPrices.add(startingBid);
            if (startingBid < lowestStartingBid) {
                lowestStartingBid = startingBid;
            }
            if (startingBid > highestObservedBid) {
                highestObservedBid = startingBid;
            }
            observedPriceSum += startingBid;
            sampleSize++;
            sortedPricesDirty = true;
        }

        private long secondLowestStartingBid() {
            List<Long> sorted = sortedPrices();
            if (sorted.isEmpty()) {
                return 0L;
            }
            if (sorted.size() < 2) {
                return sorted.get(0);
            }
            return sorted.get(1);
        }

        private double averageObservedPrice() {
            return sampleSize == 0 ? 0D : (double) observedPriceSum / sampleSize;
        }

        private double medianObservedPrice() {
            return percentile(0.50D);
        }

        private double p25ObservedPrice() {
            return percentile(0.25D);
        }

        private double percentile(double percentile) {
            List<Long> sorted = sortedPrices();
            if (sorted.isEmpty()) {
                return 0D;
            }
            double clamped = Math.max(0D, Math.min(1D, percentile));
            int index = (int) Math.floor(clamped * (sorted.size() - 1));
            return sorted.get(index);
        }

        private List<Long> sortedPrices() {
            if (activeBinPrices.isEmpty()) {
                return List.of();
            }
            if (!sortedPricesDirty) {
                return sortedActiveBinPrices;
            }
            List<Long> sorted = new ArrayList<>(activeBinPrices);
            Collections.sort(sorted);
            sortedActiveBinPrices = sorted;
            sortedPricesDirty = false;
            return sortedActiveBinPrices;
        }
    }

    private record ParsedItemName(String baseName, Integer petLevel) {
    }
}
