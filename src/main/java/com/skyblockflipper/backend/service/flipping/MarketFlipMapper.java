package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Policy.FlipEligibilityPolicy;
import com.skyblockflipper.backend.model.Flipping.Step;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class MarketFlipMapper {

    private static final long DEFAULT_MARKET_STEP_SECONDS = 30L;
    private static final int DEFAULT_AUCTION_DURATION_HOURS = 12;
    private static final int DEFAULT_STACK_AMOUNT = 1;
    private static final Logger log = LoggerFactory.getLogger(MarketFlipMapper.class);

    private final ObjectMapper objectMapper;
    private final FlipEligibilityPolicy flipEligibilityPolicy;

    public MarketFlipMapper(ObjectMapper objectMapper, FlipEligibilityPolicy flipEligibilityPolicy) {
        this.objectMapper = objectMapper;
        this.flipEligibilityPolicy = flipEligibilityPolicy;
    }

    public List<Flip> fromMarketSnapshot(UnifiedFlipInputSnapshot snapshot) {
        if (snapshot == null) {
            return List.of();
        }

        int expectedSize = snapshot.bazaarQuotes().size() + snapshot.auctionQuotesByItem().size();
        List<Flip> generated = new ArrayList<>(Math.max(expectedSize, 0));
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
        if (itemId == null || itemId.isBlank()) {
            return null;
        }
        if (!flipEligibilityPolicy.isBazaarFlipEligible(quote)) {
            return null;
        }

        String buyParams = buildParamsJson(itemId, "BAZAAR", DEFAULT_STACK_AMOUNT, null);
        String sellParams = buildParamsJson(itemId, "BAZAAR", DEFAULT_STACK_AMOUNT, null);
        return new Flip(
                null,
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
        if (itemId == null || itemId.isBlank()) {
            return null;
        }
        String ineligibilityReason = flipEligibilityPolicy.auctionIneligibilityReason(quote);
        if (ineligibilityReason != null) {
            log.debug("Rejected auction flip candidate itemId={} reason={} lowest={} secondLowest={} median={} p25={} sampleSize={}",
                    itemId,
                    ineligibilityReason,
                    quote == null ? null : quote.lowestStartingBid(),
                    quote == null ? null : quote.secondLowestStartingBid(),
                    quote == null ? null : quote.medianObservedPrice(),
                    quote == null ? null : quote.p25ObservedPrice(),
                    quote == null ? null : quote.sampleSize());
            return null;
        }

        String buyParams = buildParamsJson(itemId, "AUCTION", DEFAULT_STACK_AMOUNT, null);
        String sellParams = buildParamsJson(itemId, "AUCTION", DEFAULT_STACK_AMOUNT, DEFAULT_AUCTION_DURATION_HOURS);
        long auctionSellDurationSeconds = DEFAULT_AUCTION_DURATION_HOURS * 3600L;
        return new Flip(
                null,
                FlipType.AUCTION,
                List.of(
                        Step.forBuyMarketBased(DEFAULT_MARKET_STEP_SECONDS, buyParams),
                        Step.forSellMarketBased(auctionSellDurationSeconds, sellParams)
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
