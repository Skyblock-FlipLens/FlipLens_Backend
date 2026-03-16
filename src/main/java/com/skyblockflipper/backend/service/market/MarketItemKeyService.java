package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.AuctionMarketRecord;
import com.skyblockflipper.backend.model.market.BazaarMarketRecord;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class MarketItemKeyService {

    private static final Pattern PET_LEVEL_PATTERN = Pattern.compile("^\\[Lvl\\s*(\\d+)]\\s*(.+)$");
    private static final Pattern INTERNAL_NAME_PATTERN = Pattern.compile("\"(?:internalname|id|item_id|itemId)\"\\s*:\\s*\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
    private static final Pattern SIMPLE_ID_ONLY_EXTRA_PATTERN = Pattern.compile(
            "^\\s*\\{\\s*\"(?:internalname|id|item_id|itemId)\"\\s*:\\s*\"[^\"]+\"\\s*}\\s*$",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern ENCHANT_LIKE_PATTERN = Pattern.compile("\\b[A-Z][A-Z'\\- ]{2,}\\s+[IVXLCDM]{1,6}\\b");

    public String toAuctionItemKey(AuctionMarketRecord record) {
        AuctionKeyParts keyParts = extractAuctionKeyParts(record);
        if (keyParts == null) {
            return null;
        }

        return keyParts.baseId()
                + "|T:" + keyParts.tier()
                + "|C:" + keyParts.category()
                + "|P:" + keyParts.petLevelToken()
                + "|S:" + keyParts.stars()
                + "|R:" + (keyParts.recombobulated() ? "1" : "0");
    }

    public String toAuctionAggregateItemKey(AuctionMarketRecord record) {
        AuctionKeyParts keyParts = extractAuctionKeyParts(record);
        if (keyParts == null) {
            return null;
        }
        return keyParts.baseId()
                + "|T:" + keyParts.tier()
                + "|C:" + keyParts.category()
                + "|P:" + keyParts.petLevelToken();
    }

    public boolean hasAuctionAdditionals(AuctionMarketRecord record) {
        AuctionKeyParts keyParts = extractAuctionKeyParts(record);
        if (keyParts == null) {
            return false;
        }
        return keyParts.stars() > 0
                || keyParts.recombobulated()
                || hasEnchantLikeTokens(record.itemLore())
                || hasAdditionalSignalsInExtra(record.extra());
    }

    public String toBazaarItemKey(BazaarMarketRecord record) {
        if (record == null) {
            return null;
        }
        return normalizeBazaarProductId(record.productId());
    }

    public String normalizeBazaarProductId(String productId) {
        if (productId == null || productId.isBlank()) {
            return null;
        }
        return productId.trim().toUpperCase(Locale.ROOT);
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

    private String normalizeIdentifier(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        normalized = normalized.replaceAll("^\\[LVL\\s*\\d+]\\s*", "");
        return normalized.replace(' ', '_');
    }

    private AuctionKeyParts extractAuctionKeyParts(AuctionMarketRecord record) {
        if (record == null) {
            return null;
        }
        ParsedItemName parsedItemName = parseItemName(record.itemName());
        String baseId = firstNonBlank(
                parseBaseIdFromExtra(record.extra()),
                normalizeIdentifier(parsedItemName.baseName()),
                normalizeIdentifier(record.itemName())
        );

        String tier = normalizeIdentifier(record.tier());
        String category = normalizeIdentifier(record.category());
        int stars = countStars(record.itemName()) + countStars(record.itemLore());
        boolean recombobulated = containsIgnoreCase(record.itemLore(), "RARITY UPGRADED")
                || containsIgnoreCase(record.extra(), "recombobulated");
        String petLevel = parsedItemName.petLevel() == null ? "-" : parsedItemName.petLevel().toString();

        return new AuctionKeyParts(
                baseId,
                tier.isBlank() ? "UNKNOWN" : tier,
                category.isBlank() ? "UNKNOWN" : category,
                petLevel,
                stars,
                recombobulated
        );
    }

    private int countStars(String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        int stars = 0;
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '✪') {
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

    private boolean hasEnchantLikeTokens(String itemLore) {
        if (itemLore == null || itemLore.isBlank()) {
            return false;
        }
        String normalizedLore = stripMinecraftFormatting(itemLore).toUpperCase(Locale.ROOT);
        return ENCHANT_LIKE_PATTERN.matcher(normalizedLore).find();
    }

    private boolean hasAdditionalSignalsInExtra(String extra) {
        if (extra == null || extra.isBlank()) {
            return false;
        }
        String[] candidates = {extra.replace("\\\"", "\""), extra};
        for (String candidate : candidates) {
            if (candidate == null || candidate.isBlank()) {
                continue;
            }
            if (SIMPLE_ID_ONLY_EXTRA_PATTERN.matcher(candidate).matches()) {
                continue;
            }
            return true;
        }
        return false;
    }

    private String stripMinecraftFormatting(String text) {
        return text.replaceAll("§.", " ");
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

    private record ParsedItemName(String baseName, Integer petLevel) {
    }

    private record AuctionKeyParts(
            String baseId,
            String tier,
            String category,
            String petLevelToken,
            int stars,
            boolean recombobulated
    ) {
    }
}
