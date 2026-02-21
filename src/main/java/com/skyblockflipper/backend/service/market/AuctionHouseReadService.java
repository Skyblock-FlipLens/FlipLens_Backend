package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.AhListingBreakdownDto;
import com.skyblockflipper.backend.api.AhListingDto;
import com.skyblockflipper.backend.api.AhListingSortBy;
import com.skyblockflipper.backend.api.AhRecentSaleDto;
import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class AuctionHouseReadService {

    private static final String DEFAULT_REFORGE = "None";
    private static final Pattern STAR_PATTERN = Pattern.compile("(\\d+)\\s*-?\\s*STAR", Pattern.CASE_INSENSITIVE);
    private static final Pattern MINECRAFT_FORMATTING_PATTERN = Pattern.compile("§.");
    private static final Pattern MODIFIER_PATTERN = Pattern.compile("(?i)\\bmodifier\\s*:\\s*([a-z\\- '\\\\]+)");
    private static final Set<String> REFORGES = Set.of(
            "WITHERED", "HEROIC", "FABLED", "SPIRITUAL", "PRECISE", "SUSPICIOUS",
            "GILDED", "ANCIENT", "GIANT", "NECROTIC", "LOVING", "RENOWNED",
            "BLOODY", "SHADED", "WARPED", "DIRTY", "MOIL",
            "REFINED", "BLESSED", "AUSPICIOUS", "MITHRAIC", "JADED", "FLEET",
            "SPICY", "SHARP", "LEGENDARY", "ODD", "FAST", "FAIR", "EPIC", "GENTLE",
            "TOIL", "BOUNTIFUL", "STELLAR", "HEADSTRONG", "UNDEAD", "CANDIED"
    );
    private static final List<String> GEMSTONE_TYPES = List.of(
            "RUBY", "AMETHYST", "JADE", "SAPPHIRE", "AMBER", "TOPAZ",
            "JASPER", "OPAL", "ONYX", "AQUAMARINE", "CITRINE", "PERIDOT"
    );
    private static final List<String> GEMSTONE_SLOT_TYPES = List.of(
            "COMBAT", "DEFENSIVE", "MINING", "UNIVERSAL"
    );

    private final MarketSnapshotPersistenceService marketSnapshotPersistenceService;
    private final ItemRepository itemRepository;

    public AuctionHouseReadService(MarketSnapshotPersistenceService marketSnapshotPersistenceService,
                                   ItemRepository itemRepository) {
        this.marketSnapshotPersistenceService = marketSnapshotPersistenceService;
        this.itemRepository = itemRepository;
    }

    @Transactional(readOnly = true)
    public Page<AhListingDto> listListings(String itemId,
                                           AhListingSortBy sortBy,
                                           Sort.Direction sortDirection,
                                           Boolean bin,
                                           Integer minStars,
                                           Integer maxStars,
                                           String reforge,
                                           Pageable pageable) {
        Optional<MarketSnapshot> latest = marketSnapshotPersistenceService.latest();
        if (latest.isEmpty()) {
            return new PageImpl<>(List.of(), pageable, 0);
        }

        Set<String> aliases = aliasesFor(itemId);
        List<AhListingDto> listings = latest.get().auctions().stream()
                .filter(auction -> matchesItem(auction, aliases))
                .map(this::toListing)
                .filter(listing -> bin == null || listing.bin() == bin)
                .filter(listing -> minStars == null || listing.stars() >= Math.max(0, minStars))
                .filter(listing -> maxStars == null || listing.stars() <= Math.max(0, maxStars))
                .filter(listing -> reforge == null || reforge.isBlank()
                        || normalize(reforge).equals(normalize(listing.reforge())))
                .sorted(listingComparator(sortBy, sortDirection))
                .toList();

        return paginate(listings, pageable);
    }

    @Transactional(readOnly = true)
    public AhListingBreakdownDto breakdown(String itemId) {
        List<AhListingDto> listings = listListings(
                itemId,
                AhListingSortBy.PRICE,
                Sort.Direction.ASC,
                null,
                null,
                null,
                null,
                Pageable.unpaged()
        ).getContent();

        Map<String, Long> byStars = new LinkedHashMap<>();
        for (int star = 0; star <= 5; star++) {
            int s = star;
            byStars.put(String.valueOf(star), listings.stream().filter(listing -> listing.stars() == s).count());
        }
        Map<String, Long> byType = Map.of(
                "BIN", listings.stream().filter(AhListingDto::bin).count(),
                "AUCTION", listings.stream().filter(listing -> !listing.bin()).count()
        );
        Map<String, Long> byReforge = listings.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        listing -> listing.reforge() == null || listing.reforge().isBlank() ? DEFAULT_REFORGE : listing.reforge(),
                        LinkedHashMap::new,
                        java.util.stream.Collectors.counting()
                ));
        long avgPrice = Math.round(listings.stream().mapToLong(AhListingDto::price).average().orElse(0D));
        Long lowestBin = listings.stream().filter(AhListingDto::bin).map(AhListingDto::price).min(Long::compareTo).orElse(null);

        return new AhListingBreakdownDto(listings.size(), byStars, byType, byReforge, avgPrice, lowestBin);
    }

    @Transactional(readOnly = true)
    public List<AhRecentSaleDto> recentSales(String itemId, int limit) {
        int safeLimit = Math.max(1, limit);
        Optional<MarketSnapshot> latest = marketSnapshotPersistenceService.latest();
        if (latest.isEmpty()) {
            return List.of();
        }

        Set<String> aliases = aliasesFor(itemId);
        return latest.get().auctions().stream()
                .filter(auction -> matchesItem(auction, aliases))
                .filter(AuctionMarketRecord::claimed)
                .sorted(Comparator.comparingLong(AuctionMarketRecord::endTimestamp).reversed())
                .limit(safeLimit)
                .map(auction -> new AhRecentSaleDto(
                        auction.auctionUuid(),
                        auction.highestBidAmount() > 0 ? auction.highestBidAmount() : auction.startingBid(),
                        extractStars(auction.itemName()),
                        extractReforge(auction.itemName(), auction.itemLore()),
                        Instant.ofEpochMilli(auction.endTimestamp()),
                        false
                ))
                .toList();
    }

    private AhListingDto toListing(AuctionMarketRecord auction) {
        long estimatedValue = auction.highestBidAmount() > 0 ? auction.highestBidAmount() : auction.startingBid();
        String name = auction.itemName() == null ? "" : auction.itemName();
        return new AhListingDto(
                auction.auctionUuid(),
                normalize(name),
                name,
                auction.startingBid(),
                List.of(),
                auction.tier(),
                extractStars(name),
                extractReforge(name, auction.itemLore()),
                Instant.ofEpochMilli(auction.endTimestamp()),
                false,
                estimatedValue,
                0,
                extractGemSlots(name, auction.itemLore())
        );
    }

    private Comparator<AhListingDto> listingComparator(AhListingSortBy sortBy, Sort.Direction direction) {
        AhListingSortBy safeSortBy = sortBy == null ? AhListingSortBy.PRICE : sortBy;
        Sort.Direction safeDirection = direction == null ? Sort.Direction.ASC : direction;
        Comparator<AhListingDto> comparator = switch (safeSortBy) {
            case ENDING_SOON -> Comparator.comparing(AhListingDto::endsAt);
            case ESTIMATED_VALUE -> Comparator.comparingLong(AhListingDto::estimatedValue);
            case PRICE -> Comparator.comparingLong(AhListingDto::price);
        };
        if (safeDirection == Sort.Direction.DESC) {
            comparator = comparator.reversed();
        }
        return comparator.thenComparing(AhListingDto::auctionId);
    }

    private boolean matchesItem(AuctionMarketRecord auction, Set<String> aliases) {
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

    private Set<String> aliasesFor(String itemId) {
        String normalized = normalize(itemId);
        if (normalized.isEmpty()) {
            return Set.of();
        }
        Set<String> aliases = new HashSet<>();
        addAlias(aliases, normalized);
        itemRepository.findById(normalized).ifPresent(item -> {
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

    private Page<AhListingDto> paginate(List<AhListingDto> values, Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            return new PageImpl<>(values);
        }
        int fromIndex = (int) Math.min((long) pageable.getPageNumber() * pageable.getPageSize(), values.size());
        int toIndex = Math.min(fromIndex + pageable.getPageSize(), values.size());
        List<AhListingDto> content = fromIndex >= toIndex ? List.of() : values.subList(fromIndex, toIndex);
        return new PageImpl<>(content, pageable, values.size());
    }

    private int extractStars(String displayName) {
        if (displayName == null || displayName.isBlank()) {
            return 0;
        }
        int glyphCount = (int) displayName.chars().filter(ch -> ch == '✪').count();
        if (glyphCount > 0) {
            return Math.min(5, glyphCount);
        }
        Matcher matcher = STAR_PATTERN.matcher(displayName);
        if (matcher.find()) {
            try {
                return Math.min(5, Math.max(0, Integer.parseInt(matcher.group(1))));
            } catch (NumberFormatException ignored) {
                return 0;
            }
        }
        return 0;
    }

    private String extractReforge(String displayName, String itemLore) {
        String loreReforge = extractReforgeFromLore(itemLore);
        if (loreReforge != null) {
            return loreReforge;
        }
        if (displayName == null || displayName.isBlank()) {
            return DEFAULT_REFORGE;
        }
        String normalized = normalize(displayName);
        for (String reforge : REFORGES) {
            if (normalized.startsWith(reforge + " ")) {
                return toTitle(reforge);
            }
        }
        String[] segments = normalized.split("[(),]");
        for (String segment : segments) {
            String candidate = segment.trim().replace("'", "");
            if (REFORGES.contains(candidate)) {
                return toTitle(candidate);
            }
        }
        return DEFAULT_REFORGE;
    }

    private String extractReforgeFromLore(String itemLore) {
        if (itemLore == null || itemLore.isBlank()) {
            return null;
        }
        String plain = stripFormatting(itemLore);
        Matcher matcher = MODIFIER_PATTERN.matcher(plain);
        if (!matcher.find()) {
            return null;
        }
        String raw = matcher.group(1);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String normalized = raw.replaceAll("[^A-Za-z'\\- ]", " ").trim();
        return normalized.isBlank() ? null : toTitleWords(normalized);
    }

    private List<String> extractGemSlots(String displayName, String itemLore) {
        List<String> loreSlots = extractGemSlotsFromLore(itemLore);
        if (!loreSlots.isEmpty()) {
            return loreSlots;
        }
        if (displayName == null || displayName.isBlank()) {
            return List.of();
        }
        String normalized = normalize(displayName);
        List<String> slots = new ArrayList<>();
        for (String slotType : GEMSTONE_SLOT_TYPES) {
            if (normalized.contains(slotType + " GEMSTONE SLOT") || normalized.contains(slotType + " SLOT")) {
                slots.add(toTitle(slotType));
            }
        }
        for (String gemType : GEMSTONE_TYPES) {
            if (normalized.contains(gemType + " GEMSTONE SLOT")
                    || normalized.contains(gemType + " SLOT")
                    || normalized.contains(gemType + " GEMSTONE")) {
                slots.add(toTitle(gemType));
            }
        }
        return List.copyOf(new LinkedHashSet<>(slots));
    }

    private List<String> extractGemSlotsFromLore(String itemLore) {
        if (itemLore == null || itemLore.isBlank()) {
            return List.of();
        }
        String upper = stripFormatting(itemLore).toUpperCase(Locale.ROOT);
        LinkedHashSet<String> slots = new LinkedHashSet<>();
        if (upper.contains("COMBAT SLOT")) {
            slots.add("Combat");
        }
        if (upper.contains("DEFENSIVE SLOT")) {
            slots.add("Defensive");
        }
        if (upper.contains("MINING SLOT")) {
            slots.add("Mining");
        }
        if (upper.contains("UNIVERSAL SLOT")) {
            slots.add("Universal");
        }
        for (String gemType : GEMSTONE_TYPES) {
            if (upper.contains(gemType + " SLOT")
                    || upper.contains(gemType + " GEMSTONE")
                    || upper.contains(gemType + " SLOT TYPE")) {
                slots.add(toTitle(gemType));
            }
        }
        return List.copyOf(slots);
    }

    private String stripFormatting(String value) {
        return MINECRAFT_FORMATTING_PATTERN.matcher(value).replaceAll("");
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private String toTitle(String value) {
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.isEmpty()) {
            return value;
        }
        return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
    }

    private String toTitleWords(String value) {
        String[] words = value.trim().split("\\s+");
        List<String> result = new ArrayList<>(words.length);
        for (String word : words) {
            if (word.isBlank()) {
                continue;
            }
            String[] parts = word.split("-");
            List<String> titleParts = new ArrayList<>(parts.length);
            for (String part : parts) {
                titleParts.add(toTitle(part));
            }
            result.add(String.join("-", titleParts));
        }
        return String.join(" ", result);
    }
}
