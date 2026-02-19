package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class MarketFlipMapper {

    private static final long DEFAULT_MARKET_STEP_SECONDS = 30L;
    private static final int DEFAULT_AUCTION_DURATION_HOURS = 12;
    private static final int DEFAULT_STACK_AMOUNT = 1;
    private static final double MIN_BAZAAR_EDGE_RATIO = 1.015D;
    private static final double MIN_AUCTION_EDGE_RATIO = 1.05D;
    private static final int MIN_AUCTION_SAMPLE_SIZE = 3;

    private final ObjectMapper objectMapper;

    public MarketFlipMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<Flip> fromMarketSnapshot(UnifiedFlipInputSnapshot snapshot) {
        if (snapshot == null) {
            return List.of();
        }

        List<Flip> generated = new ArrayList<>();
        for (Map.Entry<String, UnifiedFlipInputSnapshot.BazaarQuote> entry : snapshot.bazaarQuotes().entrySet()) {
            Flip flip = buildBazaarFlip(entry.getKey(), entry.getValue());
            if (flip != null) {
                generated.add(flip);
            }
        }
        for (Map.Entry<String, UnifiedFlipInputSnapshot.AuctionQuote> entry : snapshot.auctionQuotesByItem().entrySet()) {
            Flip flip = buildAuctionFlip(entry.getKey(), entry.getValue());
            if (flip != null) {
                generated.add(flip);
            }
        }
        return generated;
    }

    private Flip buildBazaarFlip(String itemId, UnifiedFlipInputSnapshot.BazaarQuote quote) {
        if (itemId == null || itemId.isBlank() || quote == null) {
            return null;
        }
        if (quote.buyPrice() <= 0D || quote.sellPrice() <= 0D) {
            return null;
        }
        if ((quote.sellPrice() / quote.buyPrice()) < MIN_BAZAAR_EDGE_RATIO) {
            return null;
        }

        String buyParams = buildParamsJson(itemId, "BAZAAR", DEFAULT_STACK_AMOUNT, null);
        String sellParams = buildParamsJson(itemId, "BAZAAR", DEFAULT_STACK_AMOUNT, null);
        return new Flip(
                UUID.randomUUID(),
                FlipType.BAZAAR,
                List.of(
                        Step.forBuyMarketBased(DEFAULT_MARKET_STEP_SECONDS, buyParams),
                        Step.forSellMarketBased(DEFAULT_MARKET_STEP_SECONDS, sellParams)
                ),
                itemId,
                List.of()
        );
    }

    private Flip buildAuctionFlip(String itemId, UnifiedFlipInputSnapshot.AuctionQuote quote) {
        if (itemId == null || itemId.isBlank() || quote == null) {
            return null;
        }
        if (quote.lowestStartingBid() <= 0L || quote.averageObservedPrice() <= 0D) {
            return null;
        }
        if (quote.sampleSize() < MIN_AUCTION_SAMPLE_SIZE) {
            return null;
        }
        if ((quote.averageObservedPrice() / quote.lowestStartingBid()) < MIN_AUCTION_EDGE_RATIO) {
            return null;
        }

        String buyParams = buildParamsJson(itemId, "AUCTION", DEFAULT_STACK_AMOUNT, null);
        String sellParams = buildParamsJson(itemId, "AUCTION", DEFAULT_STACK_AMOUNT, DEFAULT_AUCTION_DURATION_HOURS);
        return new Flip(
                UUID.randomUUID(),
                FlipType.AUCTION,
                List.of(
                        Step.forBuyMarketBased(DEFAULT_MARKET_STEP_SECONDS, buyParams),
                        Step.forSellMarketBased(DEFAULT_MARKET_STEP_SECONDS, sellParams)
                ),
                itemId,
                List.of()
        );
    }

    private String buildParamsJson(String itemId, String market, int amount, Integer durationHours) {
        ObjectNode node = objectMapper.createObjectNode()
                .put("itemId", itemId)
                .put("amount", amount)
                .put("market", market);
        if (durationHours != null) {
            node.put("durationHours", durationHours);
        }
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JacksonException ignored) {
            StringBuilder fallback = new StringBuilder()
                    .append("{\"itemId\":\"")
                    .append(escapeJson(itemId))
                    .append("\",\"amount\":")
                    .append(amount)
                    .append(",\"market\":\"")
                    .append(escapeJson(market))
                    .append("\"");
            if (durationHours != null) {
                fallback.append(",\"durationHours\":").append(durationHours);
            }
            return fallback.append("}").toString();
        }
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
