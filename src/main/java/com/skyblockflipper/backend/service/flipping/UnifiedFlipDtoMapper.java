package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.*;

@Component
public class UnifiedFlipDtoMapper {

    private static final int DEFAULT_AUCTION_DURATION_HOURS = 12;
    private static final Set<Integer> AUCTION_DURATION_PRESETS_HOURS = Set.of(1, 6, 12, 24, 48);
    private static final long CLAIM_TAX_MIN_REMAINING_COINS = 1_000_000L;

    private static final double LIQUIDITY_TIME_SCALE_HOURS = 1.0D;
    private static final double LIQUIDITY_SPREAD_SCALE = 0.02D;
    private static final double EXECUTION_SPREAD_CAP = 0.05D;
    private static final double EXECUTION_TIME_CAP_HOURS = 6.0D;
    private static final double MAX_TIME_FOR_SCORING_HOURS = 24.0D;
    private static final double EXECUTION_SPREAD_WEIGHT = 0.5D;
    private static final double EXECUTION_TIME_WEIGHT = 0.5D;
    private static final double STRUCTURAL_ILLIQUIDITY_PENALTY = 10D;

    private static final Logger log = LoggerFactory.getLogger(UnifiedFlipDtoMapper.class);
    private final ObjectMapper objectMapper;
    private final FlipRiskScorer flipRiskScorer;

    public UnifiedFlipDtoMapper(ObjectMapper objectMapper, FlipRiskScorer flipRiskScorer) {
        this.objectMapper = objectMapper;
        this.flipRiskScorer = flipRiskScorer;
    }

    public UnifiedFlipDto toDto(Flip flip) {
        return toDto(flip, FlipCalculationContext.standard(new UnifiedFlipInputSnapshot(Instant.now(), null, null)));
    }

    public UnifiedFlipDto toDto(Flip flip, FlipCalculationContext context) {
        if (flip == null) {
            return null;
        }

        FlipCalculationContext safeContext = context == null
                ? FlipCalculationContext.standard(new UnifiedFlipInputSnapshot(Instant.now(), null, null))
                : context;
        UnifiedFlipInputSnapshot snapshot = safeContext.marketSnapshot() == null
                ? new UnifiedFlipInputSnapshot(Instant.now(), null, null)
                : safeContext.marketSnapshot();

        LinkedHashSet<String> partialReasons = new LinkedHashSet<>();
        if (snapshot.bazaarQuotes().isEmpty() && snapshot.auctionQuotesByItem().isEmpty()) {
            partialReasons.add("MISSING_MARKET_SNAPSHOT");
        }
        if (safeContext.electionPartial()) {
            partialReasons.add("MISSING_ELECTION_DATA");
        }

        PricingComputation pricing = computePricing(flip, snapshot, safeContext, partialReasons);
        long minCapitalConstraint = resolveMinCapitalConstraint(flip.getConstraints());
        long requiredCapital = Math.max(minCapitalConstraint, Math.max(pricing.currentPriceBaseline(), pricing.peakExposure()));

        long expectedProfit = pricing.grossRevenue() - pricing.totalInputCost() - pricing.totalFees();
        Long fees = pricing.totalFees();
        Double roi = computeRoi(requiredCapital, expectedProfit);
        Double roiPerHour = computeRoiPerHour(roi, flip.getTotalDuration().toSeconds());

        return new UnifiedFlipDto(
                flip.getId(),
                flip.getFlipType(),
                mapInputItems(flip.getSteps()),
                mapOutputItems(flip),
                requiredCapital,
                expectedProfit,
                roi,
                roiPerHour,
                flip.getTotalDuration().toSeconds(),
                fees,
                pricing.liquidityScore(),
                pricing.riskScore(),
                snapshot.snapshotTimestamp(),
                !partialReasons.isEmpty(),
                List.copyOf(partialReasons),
                mapSteps(flip.getSteps()),
                mapConstraints(flip.getConstraints())
        );
    }

    private List<UnifiedFlipDto.ItemStackDto> mapInputItems(List<Step> steps) {
        if (steps == null || steps.isEmpty()) {
            return List.of();
        }

        Map<String, Integer> itemCounts = new LinkedHashMap<>();
        for (Step step : steps) {
            if (step == null || step.getType() != StepType.BUY) {
                continue;
            }
            ParsedItemStack parsed = parseItemStack(step.getParamsJson());
            if (parsed != null) {
                itemCounts.merge(parsed.itemId(), parsed.amount(), Integer::sum);
            }
        }
        return toItemStackList(itemCounts);
    }

    private List<UnifiedFlipDto.ItemStackDto> mapOutputItems(Flip flip) {
        Map<String, Integer> itemCounts = new LinkedHashMap<>();
        if (flip.getResultItemId() != null && !flip.getResultItemId().isBlank()) {
            itemCounts.put(flip.getResultItemId(), 1);
        }

        List<Step> steps = flip.getSteps();
        if (steps != null) {
            for (Step step : steps) {
                if (step == null || step.getType() != StepType.SELL) {
                    continue;
                }
                ParsedItemStack parsed = parseItemStack(step.getParamsJson());
                if (parsed != null) {
                    itemCounts.merge(parsed.itemId(), parsed.amount(), Integer::sum);
                }
            }
        }
        return toItemStackList(itemCounts);
    }

    private List<UnifiedFlipDto.StepDto> mapSteps(List<Step> steps) {
        if (steps == null || steps.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto.StepDto> result = new ArrayList<>(steps.size());
        for (Step step : steps) {
            if (step == null) {
                continue;
            }
            result.add(new UnifiedFlipDto.StepDto(
                    step.getType(),
                    step.getDurationType(),
                    step.getBaseDurationSeconds(),
                    step.getDurationFactor(),
                    step.getResource(),
                    step.getResourceUnits(),
                    step.getSchedulingPolicy(),
                    step.getParamsJson()
            ));
        }
        return List.copyOf(result);
    }

    private List<UnifiedFlipDto.ConstraintDto> mapConstraints(List<Constraint> constraints) {
        if (constraints == null || constraints.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto.ConstraintDto> result = new ArrayList<>(constraints.size());
        for (Constraint constraint : constraints) {
            if (constraint == null) {
                continue;
            }
            result.add(new UnifiedFlipDto.ConstraintDto(
                    constraint.getType(),
                    constraint.getStringValue(),
                    constraint.getIntValue(),
                    constraint.getLongValue()
            ));
        }
        return List.copyOf(result);
    }

    private long resolveMinCapitalConstraint(List<Constraint> constraints) {
        if (constraints == null || constraints.isEmpty()) {
            return 0L;
        }
        return constraints.stream()
                .filter(constraint -> constraint != null && constraint.getType() == ConstraintType.MIN_CAPITAL)
                .map(Constraint::getLongValue)
                .filter(value -> value != null && value > 0)
                .max(Comparator.naturalOrder())
                .orElse(0L);
    }

    private Double computeRoi(long requiredCapital, long expectedProfit) {
        if (requiredCapital <= 0) {
            return null;
        }
        return (double) expectedProfit / requiredCapital;
    }

    private Double computeRoiPerHour(Double roi, long durationSeconds) {
        if (roi == null || durationSeconds <= 0) {
            return null;
        }
        return roi * (3600D / durationSeconds);
    }

    private PricingComputation computePricing(Flip flip,
                                              UnifiedFlipInputSnapshot snapshot,
                                              FlipCalculationContext context,
                                              LinkedHashSet<String> partialReasons) {
        List<Double> legLiquidityScores = new ArrayList<>();
        List<Double> legExecutionRiskScores = new ArrayList<>();
        List<String> bazaarSignalItemIds = new ArrayList<>();

        long runningExposure = 0L;
        long peakExposure = 0L;
        long currentPriceBaseline = 0L;
        long totalInputCost = 0L;
        long grossRevenue = 0L;
        long totalFees = 0L;
        double inputFillHours = 0D;
        double outputFillHours = 0D;
        double craftDelayHours = 0D;

        List<Step> steps = flip.getSteps() == null ? List.of() : flip.getSteps();
        boolean hasExplicitSellStep = false;
        for (Step step : steps) {
            if (step == null || step.getType() == null) {
                continue;
            }

            if (step.getType() == StepType.BUY) {
                ParsedItemStack parsed = parseItemStack(step.getParamsJson());
                if (parsed == null) {
                    partialReasons.add("INVALID_BUY_PARAMS");
                    continue;
                }

                PriceQuote quote = resolveBuyPriceQuote(parsed, snapshot, partialReasons);
                if (quote == null) {
                    continue;
                }

                long stepCost = ceilToLong(quote.unitPrice() * parsed.amount());
                currentPriceBaseline += stepCost;
                totalInputCost += stepCost;
                runningExposure += stepCost;
                peakExposure = Math.max(peakExposure, runningExposure);
                Double fillTimeHours = updateSignals(
                        quote,
                        parsed.amount(),
                        TradeSide.BUY,
                        resolveStepDurationHours(step),
                        safeContextFeatures(context),
                        legLiquidityScores,
                        legExecutionRiskScores,
                        bazaarSignalItemIds,
                        partialReasons
                );
                if (fillTimeHours != null) {
                    inputFillHours += fillTimeHours;
                }
                continue;
            }

            if (step.getType() == StepType.SELL) {
                hasExplicitSellStep = true;
                ParsedItemStack parsed = parseItemStack(step.getParamsJson());
                if (parsed == null) {
                    partialReasons.add("INVALID_SELL_PARAMS");
                    continue;
                }

                SellComputation sellComputation = computeSell(parsed, step.getParamsJson(), snapshot, context, partialReasons);
                if (sellComputation == null) {
                    continue;
                }

                grossRevenue += sellComputation.grossRevenue();
                totalFees += sellComputation.totalFees();
                runningExposure += sellComputation.upfrontFees();
                peakExposure = Math.max(peakExposure, runningExposure);
                runningExposure = Math.max(0L, runningExposure - sellComputation.netProceeds());
                Double fillTimeHours = updateSignals(
                        sellComputation.quote(),
                        parsed.amount(),
                        TradeSide.SELL,
                        sellComputation.executionFillHours(),
                        safeContextFeatures(context),
                        legLiquidityScores,
                        legExecutionRiskScores,
                        bazaarSignalItemIds,
                        partialReasons
                );
                if (fillTimeHours != null) {
                    outputFillHours += fillTimeHours;
                }
                continue;
            }

            craftDelayHours += Math.max(0D, (step.getBaseDurationSeconds() == null ? 0L : step.getBaseDurationSeconds()) / 3600D);
        }

        if (!hasExplicitSellStep && flip.getResultItemId() != null && !flip.getResultItemId().isBlank()) {
            ParsedItemStack implicitSell = ParsedItemStack.implicitSell(flip.getResultItemId());
            SellComputation sellComputation = computeSell(
                    implicitSell,
                    null,
                    snapshot,
                    context,
                    partialReasons
            );
            if (sellComputation != null) {
                grossRevenue += sellComputation.grossRevenue();
                totalFees += sellComputation.totalFees();
                runningExposure += sellComputation.upfrontFees();
                peakExposure = Math.max(peakExposure, runningExposure);
                Double fillTimeHours = updateSignals(
                        sellComputation.quote(),
                        implicitSell.amount(),
                        TradeSide.SELL,
                        sellComputation.executionFillHours(),
                        safeContextFeatures(context),
                        legLiquidityScores,
                        legExecutionRiskScores,
                        bazaarSignalItemIds,
                        partialReasons
                );
                if (fillTimeHours != null) {
                    outputFillHours += fillTimeHours;
                }
            } else {
                partialReasons.add("MISSING_OUTPUT_PRICE:" + flip.getResultItemId());
            }
        }

        Double liquidityScore = minValue(legLiquidityScores);
        Double riskScore = flipRiskScorer.computeTotalRiskScore(
                legExecutionRiskScores,
                inputFillHours,
                outputFillHours,
                craftDelayHours,
                safeContextFeatures(context),
                bazaarSignalItemIds
        );

        return new PricingComputation(
                totalInputCost,
                grossRevenue,
                totalFees,
                currentPriceBaseline,
                peakExposure,
                liquidityScore,
                riskScore
        );
    }

    private SellComputation computeSell(ParsedItemStack parsed,
                                        String paramsJson,
                                        UnifiedFlipInputSnapshot snapshot,
                                        FlipCalculationContext context,
                                        LinkedHashSet<String> partialReasons) {
        PriceQuote quote = resolveSellPriceQuote(parsed, snapshot, partialReasons);
        if (quote == null) {
            return null;
        }

        long grossRevenue = floorToLong(quote.unitPrice() * parsed.amount());
        long upfrontFees = 0L;
        long totalFees = 0L;
        Double executionFillHours = null;

        if (quote.source() == MarketSource.BAZAAR) {
            totalFees = ceilToLong(grossRevenue * context.bazaarTaxRate());
        } else if (quote.source() == MarketSource.AUCTION) {
            int durationHours = parseAuctionDurationHours(paramsJson, partialReasons);
            AuctionFees auctionFees = computeAuctionFees(grossRevenue, durationHours, context.auctionTaxMultiplier());
            upfrontFees = auctionFees.listingFee() + auctionFees.durationFee();
            totalFees = auctionFees.totalFee();
            executionFillHours = (double) durationHours;
        }

        long netProceeds = Math.max(0L, grossRevenue - totalFees);
        return new SellComputation(quote, grossRevenue, upfrontFees, totalFees, netProceeds, executionFillHours);
    }

    private int parseAuctionDurationHours(String paramsJson, LinkedHashSet<String> partialReasons) {
        if (paramsJson == null || paramsJson.isBlank()) {
            return DEFAULT_AUCTION_DURATION_HOURS;
        }
        try {
            JsonNode node = objectMapper.readTree(paramsJson);
            JsonNode durationNode = node.path("durationHours");
            int durationHours;
            if (durationNode.isInt() || durationNode.isLong()) {
                durationHours = durationNode.asInt();
            } else if (durationNode.isString()) {
                durationHours = Integer.parseInt(durationNode.asString().trim());
            } else {
                return DEFAULT_AUCTION_DURATION_HOURS;
            }

            if (!AUCTION_DURATION_PRESETS_HOURS.contains(durationHours)) {
                partialReasons.add("UNSUPPORTED_AUCTION_DURATION_PRESET");
                return DEFAULT_AUCTION_DURATION_HOURS;
            }
            return durationHours;
        } catch (Exception ex) {
            partialReasons.add("INVALID_AUCTION_DURATION");
            return DEFAULT_AUCTION_DURATION_HOURS;
        }
    }

    private AuctionFees computeAuctionFees(long grossRevenue, int durationHours, double taxMultiplier) {
        double listingRate = resolveAuctionListingRate(grossRevenue);
        long baseListingFee = ceilToLong(grossRevenue * listingRate);
        long listingFee = ceilToLong(baseListingFee * taxMultiplier);

        long baseDurationFee = auctionDurationFee(durationHours);
        long durationFee = ceilToLong(baseDurationFee * taxMultiplier);

        long baseClaimTax = computeAuctionClaimTax(grossRevenue);
        long claimTax = Math.min(
                baseClaimTax,
                Math.max(0L, grossRevenue - CLAIM_TAX_MIN_REMAINING_COINS)
        );

        long totalFee = listingFee + durationFee + claimTax;
        return new AuctionFees(listingFee, durationFee, claimTax, totalFee);
    }

    private long computeAuctionClaimTax(long grossRevenue) {
        if (grossRevenue <= CLAIM_TAX_MIN_REMAINING_COINS) {
            return 0L;
        }
        long onePercent = ceilToLong(grossRevenue * 0.01D);
        long cap = Math.max(0L, grossRevenue - CLAIM_TAX_MIN_REMAINING_COINS);
        return Math.min(onePercent, cap);
    }

    private double resolveAuctionListingRate(long grossRevenue) {
        if (grossRevenue < 10_000_000L) {
            return 0.01D;
        }
        if (grossRevenue < 100_000_000L) {
            return 0.02D;
        }
        return 0.025D;
    }

    private long auctionDurationFee(int durationHours) {
        return switch (durationHours) {
            case 1 -> 20L;
            case 6 -> 45L;
            case 24 -> 350L;
            case 48 -> 1200L;
            default -> 100L;
        };
    }

    private PriceQuote resolveBuyPriceQuote(ParsedItemStack parsed,
                                            UnifiedFlipInputSnapshot snapshot,
                                            LinkedHashSet<String> partialReasons) {
        String itemId = parsed.itemId();
        if (parsed.marketPreference() == MarketPreference.NPC) {
            if (parsed.npcUnitPrice() != null && parsed.npcUnitPrice() > 0) {
                return new PriceQuote(itemId, parsed.npcUnitPrice(), MarketSource.NPC, null, null);
            }
            partialReasons.add("MISSING_NPC_PRICE:" + itemId);
            return null;
        }

        UnifiedFlipInputSnapshot.BazaarQuote bazaarQuote = snapshot.bazaarQuotes().get(itemId);
        UnifiedFlipInputSnapshot.AuctionQuote auctionQuote = snapshot.auctionQuotesByItem().get(itemId);
        boolean hasBazaar = bazaarQuote != null && bazaarQuote.buyPrice() > 0;
        boolean hasAuction = auctionQuote != null && auctionQuote.lowestStartingBid() > 0;

        if (parsed.marketPreference() == MarketPreference.BAZAAR) {
            if (hasBazaar) {
                return new PriceQuote(itemId, bazaarQuote.buyPrice(), MarketSource.BAZAAR, bazaarQuote, null);
            }
            partialReasons.add("MISSING_INPUT_PRICE_BAZAAR:" + itemId);
            return null;
        }

        if (parsed.marketPreference() == MarketPreference.AUCTION) {
            if (hasAuction) {
                return new PriceQuote(itemId, auctionQuote.lowestStartingBid(), MarketSource.AUCTION, null, auctionQuote);
            }
            partialReasons.add("MISSING_INPUT_PRICE_AUCTION:" + itemId);
            return null;
        }

        if (hasBazaar && hasAuction) {
            partialReasons.add("AMBIGUOUS_INPUT_MARKET_SOURCE:" + itemId);
            return new PriceQuote(itemId, bazaarQuote.buyPrice(), MarketSource.BAZAAR, bazaarQuote, null);
        }
        if (hasBazaar) {
            return new PriceQuote(itemId, bazaarQuote.buyPrice(), MarketSource.BAZAAR, bazaarQuote, null);
        }
        if (hasAuction) {
            return new PriceQuote(itemId, auctionQuote.lowestStartingBid(), MarketSource.AUCTION, null, auctionQuote);
        }

        partialReasons.add("MISSING_INPUT_PRICE:" + itemId);
        return null;
    }

    private PriceQuote resolveSellPriceQuote(ParsedItemStack parsed,
                                             UnifiedFlipInputSnapshot snapshot,
                                             LinkedHashSet<String> partialReasons) {
        String itemId = parsed.itemId();
        if (parsed.marketPreference() == MarketPreference.NPC) {
            partialReasons.add("UNSUPPORTED_OUTPUT_MARKET_NPC:" + itemId);
            return null;
        }

        UnifiedFlipInputSnapshot.BazaarQuote bazaarQuote = snapshot.bazaarQuotes().get(itemId);
        UnifiedFlipInputSnapshot.AuctionQuote auctionQuote = snapshot.auctionQuotesByItem().get(itemId);
        boolean hasBazaar = bazaarQuote != null && bazaarQuote.sellPrice() > 0;
        boolean hasAuctionAverage = auctionQuote != null && auctionQuote.averageObservedPrice() > 0;
        boolean hasAuctionHighest = auctionQuote != null && auctionQuote.highestObservedBid() > 0;

        if (parsed.marketPreference() == MarketPreference.BAZAAR) {
            if (hasBazaar) {
                return new PriceQuote(itemId, bazaarQuote.sellPrice(), MarketSource.BAZAAR, bazaarQuote, null);
            }
            partialReasons.add("MISSING_OUTPUT_PRICE_BAZAAR:" + itemId);
            return null;
        }

        if (parsed.marketPreference() == MarketPreference.AUCTION) {
            if (hasAuctionAverage) {
                return new PriceQuote(itemId, auctionQuote.averageObservedPrice(), MarketSource.AUCTION, null, auctionQuote);
            }
            if (hasAuctionHighest) {
                return new PriceQuote(itemId, auctionQuote.highestObservedBid(), MarketSource.AUCTION, null, auctionQuote);
            }
            partialReasons.add("MISSING_OUTPUT_PRICE_AUCTION:" + itemId);
            return null;
        }

        if (hasBazaar) {
            return new PriceQuote(itemId, bazaarQuote.sellPrice(), MarketSource.BAZAAR, bazaarQuote, null);
        }
        if (hasAuctionAverage) {
            return new PriceQuote(itemId, auctionQuote.averageObservedPrice(), MarketSource.AUCTION, null, auctionQuote);
        }
        if (hasAuctionHighest) {
            return new PriceQuote(itemId, auctionQuote.highestObservedBid(), MarketSource.AUCTION, null, auctionQuote);
        }

        partialReasons.add("MISSING_OUTPUT_PRICE:" + itemId);
        return null;
    }

    private Double updateSignals(PriceQuote quote,
                                 int amount,
                                 TradeSide tradeSide,
                                 Double auctionFillHours,
                                 FlipScoreFeatureSet featureSet,
                                 List<Double> legLiquidityScores,
                                 List<Double> legExecutionRiskScores,
                                 List<String> bazaarSignalItemIds,
                                 LinkedHashSet<String> partialReasons) {
        if (quote == null) {
            return null;
        }

        if (quote.source() == MarketSource.BAZAAR && quote.bazaarQuote() != null) {
            UnifiedFlipInputSnapshot.BazaarQuote bazaar = quote.bazaarQuote();
            double spreadRel = computeRelativeSpread(bazaar);
            double fillTimeHours = computeFillTimeHours(bazaar, amount, tradeSide, partialReasons, quote.itemId());
            bazaarSignalItemIds.add(quote.itemId());

            double liquidityScore = 100D
                    * (1D / (1D + fillTimeHours / LIQUIDITY_TIME_SCALE_HOURS))
                    * (1D / (1D + spreadRel / LIQUIDITY_SPREAD_SCALE));
            FlipScoreFeatureSet.ItemTimescaleFeatures features = featureSet.get(quote.itemId());
            if (features != null && features.structurallyIlliquid()) {
                liquidityScore -= STRUCTURAL_ILLIQUIDITY_PENALTY;
            }
            legLiquidityScores.add(clamp(liquidityScore, 0D, 100D));

            double spreadRisk = clamp01(spreadRel / EXECUTION_SPREAD_CAP) * 100D;
            double timeRisk = clamp01(fillTimeHours / EXECUTION_TIME_CAP_HOURS) * 100D;
            legExecutionRiskScores.add(
                    (EXECUTION_SPREAD_WEIGHT * spreadRisk) + (EXECUTION_TIME_WEIGHT * timeRisk)
            );
            return fillTimeHours;
        }

        if (quote.source() == MarketSource.AUCTION && quote.auctionQuote() != null) {
            UnifiedFlipInputSnapshot.AuctionQuote auction = quote.auctionQuote();
            double sampleLiquidity = clamp((double) auction.sampleSize() / 20D, 0D, 1D);
            legLiquidityScores.add(sampleLiquidity * 100D);
            legExecutionRiskScores.add((1D - sampleLiquidity) * 100D);
            return auctionFillHours == null || auctionFillHours <= 0D
                    ? null
                    : clamp(auctionFillHours, 0D, MAX_TIME_FOR_SCORING_HOURS);
        }

        return null;
    }

    private FlipScoreFeatureSet safeContextFeatures(FlipCalculationContext context) {
        if (context == null || context.scoreFeatureSet() == null) {
            return FlipScoreFeatureSet.empty();
        }
        return context.scoreFeatureSet();
    }

    private Double resolveStepDurationHours(Step step) {
        if (step == null || step.getBaseDurationSeconds() == null || step.getBaseDurationSeconds() <= 0L) {
            return null;
        }
        return Math.max(0D, step.getBaseDurationSeconds() / 3600D);
    }

    private Double minValue(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.stream().filter(Objects::nonNull).min(Double::compareTo).orElse(null);
    }

    private double computeRelativeSpread(UnifiedFlipInputSnapshot.BazaarQuote bazaarQuote) {
        double high = Math.max(bazaarQuote.buyPrice(), bazaarQuote.sellPrice());
        double low = Math.min(bazaarQuote.buyPrice(), bazaarQuote.sellPrice());
        double mid = (high + low) / 2D;
        if (mid <= 0D) {
            return 1D;
        }
        return Math.max(0D, (high - low) / mid);
    }

    private double computeFillTimeHours(UnifiedFlipInputSnapshot.BazaarQuote bazaarQuote,
                                        int amount,
                                        TradeSide tradeSide,
                                        LinkedHashSet<String> partialReasons,
                                        String itemId) {
        double turnover = resolveTurnoverPerHour(bazaarQuote, tradeSide);
        if (turnover <= 0D) {
            partialReasons.add("ZERO_TURNOVER:" + itemId);
            return MAX_TIME_FOR_SCORING_HOURS;
        }

        double hours = Math.max(0D, amount) / turnover;
        if (Double.isNaN(hours) || Double.isInfinite(hours)) {
            partialReasons.add("INVALID_FILL_TIME:" + itemId);
            return MAX_TIME_FOR_SCORING_HOURS;
        }
        return clamp(hours, 0D, MAX_TIME_FOR_SCORING_HOURS);
    }

    private double resolveTurnoverPerHour(UnifiedFlipInputSnapshot.BazaarQuote bazaarQuote, TradeSide tradeSide) {
        if (tradeSide == TradeSide.BUY) {
            if (bazaarQuote.sellMovingWeek() > 0) {
                return bazaarQuote.sellMovingWeek() / 168D;
            }
            return bazaarQuote.sellVolume() / 168D;
        }
        if (bazaarQuote.buyMovingWeek() > 0) {
            return bazaarQuote.buyMovingWeek() / 168D;
        }
        return bazaarQuote.buyVolume() / 168D;
    }

    private double clamp01(double value) {
        return clamp(value, 0D, 1D);
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private long ceilToLong(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0L;
        }
        return (long) Math.ceil(value);
    }

    private long floorToLong(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0L;
        }
        return (long) Math.floor(value);
    }

    private List<UnifiedFlipDto.ItemStackDto> toItemStackList(Map<String, Integer> counts) {
        if (counts.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto.ItemStackDto> result = new ArrayList<>(counts.size());
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            result.add(new UnifiedFlipDto.ItemStackDto(entry.getKey(), entry.getValue()));
        }
        return List.copyOf(result);
    }

    private ParsedItemStack parseItemStack(String paramsJson) {
        if (paramsJson == null || paramsJson.isBlank()) {
            log.debug("ParsedItemStack parse skipped: reason=missing_or_blank_params_json rawParamsJson='{}' parsedType={} objectMapper={}",
                    paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(paramsJson);
            JsonNode itemNode = node.path("itemId");
            if (!itemNode.isString()) {
                log.warn("ParsedItemStack parse failed: reason=missing_or_invalid_itemId rawParamsJson='{}' parsedType={} objectMapper={}",
                        paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
                return null;
            }
            String itemId = itemNode.asString();
            if (itemId.isBlank()) {
                log.warn("ParsedItemStack parse failed: reason=blank_itemId rawParamsJson='{}' parsedType={} objectMapper={}",
                        paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
                return null;
            }
            int amount = 1;
            JsonNode amountNode = node.path("amount");
            if (amountNode.isInt() || amountNode.isLong()) {
                amount = amountNode.asInt();
            } else if (amountNode.isString()) {
                try {
                    amount = Integer.parseInt(amountNode.asString().trim());
                } catch (NumberFormatException e) {
                    log.warn("ParsedItemStack amount parse fallback: reason=invalid_amount_format rawAmount='{}' rawParamsJson='{}' parsedType={} objectMapper={}",
                            amountNode.asString(), paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName(), e);
                }
            } else if (amountNode.isMissingNode() || amountNode.isNull()) {
                log.debug("ParsedItemStack amount defaulted: reason=missing_amount rawParamsJson='{}' parsedType={} objectMapper={}",
                        paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
            } else {
                log.warn("ParsedItemStack amount defaulted: reason=unsupported_amount_type amountNode='{}' rawParamsJson='{}' parsedType={} objectMapper={}",
                        amountNode, paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
            }
            MarketPreference marketPreference = parseMarketPreference(node);
            Double npcUnitPrice = null;
            if (marketPreference == MarketPreference.NPC) {
                npcUnitPrice = readPositiveDouble(node, "unitPrice", "npcUnitPrice", "npcPrice", "price", "coinCost");
            }
            return new ParsedItemStack(itemId, Math.max(1, amount), marketPreference, npcUnitPrice);
        } catch (Exception e) {
            log.warn("ParsedItemStack parse failed: reason=exception_during_json_parse rawParamsJson='{}' parsedType={} objectMapper={}",
                    paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName(), e);
            return null;
        }
    }

    private MarketPreference parseMarketPreference(JsonNode node) {
        String market = "";
        JsonNode marketNode = node.path("market");
        if (marketNode.isString()) {
            market = marketNode.asString("");
        } else {
            JsonNode sourceNode = node.path("source");
            if (sourceNode.isString()) {
                market = sourceNode.asString("");
            }
        }
        if (market == null || market.isBlank()) {
            return MarketPreference.ANY;
        }
        String normalized = market.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "BAZAAR" -> MarketPreference.BAZAAR;
            case "AUCTION" -> MarketPreference.AUCTION;
            case "NPC", "NPC_SHOP" -> MarketPreference.NPC;
            default -> MarketPreference.ANY;
        };
    }

    private Double readPositiveDouble(JsonNode node, String... keys) {
        for (String key : keys) {
            JsonNode valueNode = node.path(key);
            if (valueNode.isNumber()) {
                double value = valueNode.asDouble();
                if (value > 0) {
                    return value;
                }
                continue;
            }
            if (valueNode.isString()) {
                try {
                    double value = Double.parseDouble(valueNode.asString().trim());
                    if (value > 0) {
                        return value;
                    }
                } catch (NumberFormatException ignored) {
                    // Continue with fallback keys.
                }
            }
        }
        return null;
    }

    private enum TradeSide {
        BUY,
        SELL
    }

    private enum MarketSource {
        BAZAAR,
        AUCTION,
        NPC
    }

    private enum MarketPreference {
        ANY,
        BAZAAR,
        AUCTION,
        NPC
    }

    private record PricingComputation(
            long totalInputCost,
            long grossRevenue,
            long totalFees,
            long currentPriceBaseline,
            long peakExposure,
            Double liquidityScore,
            Double riskScore
    ) {
    }

    private record PriceQuote(
            String itemId,
            double unitPrice,
            MarketSource source,
            UnifiedFlipInputSnapshot.BazaarQuote bazaarQuote,
            UnifiedFlipInputSnapshot.AuctionQuote auctionQuote
    ) {
    }

    private record AuctionFees(
            long listingFee,
            long durationFee,
            long claimTax,
            long totalFee
    ) {
    }

    private record SellComputation(
            PriceQuote quote,
            long grossRevenue,
            long upfrontFees,
            long totalFees,
            long netProceeds,
            Double executionFillHours
    ) {
    }

    private record ParsedItemStack(
            String itemId,
            int amount,
            MarketPreference marketPreference,
            Double npcUnitPrice
    ) {
        private static ParsedItemStack implicitSell(String itemId) {
            return new ParsedItemStack(itemId, 1, MarketPreference.ANY, null);
        }
    }
}
