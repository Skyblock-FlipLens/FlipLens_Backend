package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.dto.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.*;

@Component
public class UnifiedFlipDtoMapper {

    private static final int DEFAULT_AUCTION_DURATION_HOURS = 12;
    private static final Set<Integer> AUCTION_DURATION_PRESETS_HOURS = Set.of(1, 6, 12, 24, 48);

    private static final double LIQUIDITY_TIME_SCALE_HOURS = 1.0D;
    private static final double LIQUIDITY_SPREAD_SCALE = 0.02D;
    private static final double EXECUTION_SPREAD_CAP = 0.05D;
    private static final double EXECUTION_TIME_CAP_HOURS = 6.0D;
    private static final double MAX_TIME_FOR_SCORING_HOURS = 24.0D;
    private static final double EXECUTION_SPREAD_WEIGHT = 0.5D;
    private static final double EXECUTION_TIME_WEIGHT = 0.5D;
    private static final double STRUCTURAL_ILLIQUIDITY_PENALTY = 10D;
    private static final double DEPTH_SHORTAGE_PRICE_IMPACT = 0.35D;

    private static final Logger log = LoggerFactory.getLogger(UnifiedFlipDtoMapper.class);
    private final ObjectMapper objectMapper;
    private final FlipRiskScorer flipRiskScorer;
    private final FlipEconomicsService flipEconomicsService;

    public UnifiedFlipDtoMapper(ObjectMapper objectMapper, FlipRiskScorer flipRiskScorer) {
        this(objectMapper, flipRiskScorer, new FlipEconomicsService());
    }

    @Autowired
    public UnifiedFlipDtoMapper(ObjectMapper objectMapper,
                                FlipRiskScorer flipRiskScorer,
                                FlipEconomicsService flipEconomicsService) {
        this.objectMapper = objectMapper;
        this.flipRiskScorer = flipRiskScorer;
        this.flipEconomicsService = flipEconomicsService;
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

        StepParseCache stepParseCache = StepParseCache.create();
        PricingComputation pricing = computePricing(flip, snapshot, safeContext, partialReasons, stepParseCache);
        long minCapitalConstraint = resolveMinCapitalConstraint(flip.getConstraints());
        long requiredCapital = flipEconomicsService.computeRequiredCapital(
                minCapitalConstraint,
                pricing.currentPriceBaseline(),
                pricing.peakExposure()
        );

        long expectedProfit = flipEconomicsService.computeExpectedProfit(
                pricing.grossRevenue(),
                pricing.totalInputCost(),
                pricing.totalFees()
        );
        Long fees = pricing.totalFees();
        Double roi = flipEconomicsService.computeRoi(requiredCapital, expectedProfit);
        Double roiPerHour = flipEconomicsService.computeRoiPerHour(roi, flip.getTotalDuration().toSeconds());

        return new UnifiedFlipDto(
                flip.getId(),
                flip.getFlipType(),
                mapInputItems(flip.getSteps(), stepParseCache),
                mapOutputItems(flip, stepParseCache),
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

    private List<UnifiedFlipDto.ItemStackDto> mapInputItems(List<Step> steps, StepParseCache stepParseCache) {
        if (steps == null || steps.isEmpty()) {
            return List.of();
        }

        Map<String, Integer> itemCounts = new LinkedHashMap<>();
        for (Step step : steps) {
            if (step == null || step.getType() != StepType.BUY) {
                continue;
            }
            ParsedItemStack parsed = parseItemStack(step, stepParseCache);
            if (parsed != null) {
                itemCounts.merge(parsed.itemId(), parsed.amount(), Integer::sum);
            }
        }
        return toItemStackList(itemCounts);
    }

    private List<UnifiedFlipDto.ItemStackDto> mapOutputItems(Flip flip, StepParseCache stepParseCache) {
        Map<String, Integer> itemCounts = new LinkedHashMap<>();
        List<Step> steps = flip.getSteps();
        if (steps != null) {
            for (Step step : steps) {
                if (step == null || step.getType() != StepType.SELL) {
                    continue;
                }
                ParsedItemStack parsed = parseItemStack(step, stepParseCache);
                if (parsed != null) {
                    itemCounts.merge(parsed.itemId(), parsed.amount(), Integer::sum);
                }
            }
        }
        if (flip.getResultItemId() != null && !flip.getResultItemId().isBlank()) {
            itemCounts.putIfAbsent(flip.getResultItemId(), 1);
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
        LinkedHashSet<Constraint> uniqueConstraints = new LinkedHashSet<>();
        for (Constraint constraint : constraints) {
            if (constraint != null) {
                uniqueConstraints.add(constraint);
            }
        }
        List<UnifiedFlipDto.ConstraintDto> result = new ArrayList<>(uniqueConstraints.size());
        for (Constraint constraint : uniqueConstraints) {
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

    private PricingComputation computePricing(Flip flip,
                                              UnifiedFlipInputSnapshot snapshot,
                                              FlipCalculationContext context,
                                              LinkedHashSet<String> partialReasons,
                                              StepParseCache stepParseCache) {
        List<Double> inputLegLiquidityScores = new ArrayList<>();
        List<Double> outputLegLiquidityScores = new ArrayList<>();
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
                ParsedItemStack parsed = parseItemStack(step, stepParseCache);
                if (parsed == null) {
                    partialReasons.add("INVALID_BUY_PARAMS");
                    continue;
                }

                PriceQuote quote = resolveBuyPriceQuote(parsed, snapshot, partialReasons);
                if (quote == null) {
                    continue;
                }

                double depthAwareBuyUnitPrice = resolveDepthAwareUnitPrice(quote, parsed.amount(), TradeSide.BUY, partialReasons);
                long stepCost = ceilToLong(depthAwareBuyUnitPrice * parsed.amount());
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
                        inputLegLiquidityScores,
                        outputLegLiquidityScores,
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
                ParsedItemStack parsed = parseItemStack(step, stepParseCache);
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
                        inputLegLiquidityScores,
                        outputLegLiquidityScores,
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
                        inputLegLiquidityScores,
                        outputLegLiquidityScores,
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

        Double inputLiquidityScore = minValue(inputLegLiquidityScores);
        Double outputLiquidityScore = minValue(outputLegLiquidityScores);
        Double liquidityScore = conservativeLiquidity(inputLiquidityScore, outputLiquidityScore);
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

        double depthAwareSellUnitPrice = resolveDepthAwareUnitPrice(quote, parsed.amount(), TradeSide.SELL, partialReasons);
        long grossRevenue = floorToLong(depthAwareSellUnitPrice * parsed.amount());
        long upfrontFees = 0L;
        long totalFees = 0L;
        Double executionFillHours = null;

        if (quote.source() == MarketSource.BAZAAR) {
            totalFees = flipEconomicsService.computeBazaarSellFees(grossRevenue, context.bazaarTaxRate());
        } else if (quote.source() == MarketSource.AUCTION) {
            int durationHours = parseAuctionDurationHours(paramsJson, partialReasons);
            FlipEconomicsService.AuctionFeeBreakdown auctionFees = flipEconomicsService.computeAuctionFees(
                    grossRevenue,
                    durationHours,
                    context.auctionTaxMultiplier()
            );
            upfrontFees = auctionFees.listingFee() + auctionFees.durationFee();
            totalFees = auctionFees.totalFee();
            executionFillHours = (double) durationHours;
        }

        long netProceeds = Math.max(0L, grossRevenue - totalFees);
        return new SellComputation(quote, grossRevenue, upfrontFees, totalFees, netProceeds, executionFillHours);
    }

    private double resolveDepthAwareUnitPrice(PriceQuote quote,
                                              int amount,
                                              TradeSide tradeSide,
                                              LinkedHashSet<String> partialReasons) {
        if (quote == null || quote.source() != MarketSource.BAZAAR || quote.bazaarQuote() == null || amount <= 0) {
            return quote == null ? 0D : quote.unitPrice();
        }
        UnifiedFlipInputSnapshot.BazaarQuote bazaar = quote.bazaarQuote();
        long availableDepth = tradeSide == TradeSide.BUY ? bazaar.sellVolume() : bazaar.buyVolume();

        if (availableDepth <= 0L) {
            addDepthPartialReason(quote.itemId(), tradeSide, partialReasons);
            return tradeSide == TradeSide.BUY
                    ? quote.unitPrice() * (1D + DEPTH_SHORTAGE_PRICE_IMPACT)
                    : quote.unitPrice() * (1D - DEPTH_SHORTAGE_PRICE_IMPACT);
        }

        if (amount <= availableDepth) {
            return quote.unitPrice();
        }

        double coverage = clamp((double) availableDepth / amount, 0D, 1D);
        double shortage = 1D - coverage;
        double impact = clamp(shortage * DEPTH_SHORTAGE_PRICE_IMPACT, 0D, DEPTH_SHORTAGE_PRICE_IMPACT);
        addDepthPartialReason(quote.itemId(), tradeSide, partialReasons);
        return tradeSide == TradeSide.BUY
                ? quote.unitPrice() * (1D + impact)
                : quote.unitPrice() * (1D - impact);
    }

    private void addDepthPartialReason(String itemId, TradeSide tradeSide, LinkedHashSet<String> partialReasons) {
        if (itemId == null || itemId.isBlank() || partialReasons == null) {
            return;
        }
        if (tradeSide == TradeSide.BUY) {
            partialReasons.add("INSUFFICIENT_INPUT_DEPTH:" + itemId);
            return;
        }
        partialReasons.add("INSUFFICIENT_OUTPUT_DEPTH:" + itemId);
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
        double conservativeAuctionSellPrice = resolveConservativeAuctionSellUnitPrice(auctionQuote);
        boolean hasConservativeAuctionSellPrice = conservativeAuctionSellPrice > 0D;

        if (parsed.marketPreference() == MarketPreference.BAZAAR) {
            if (hasBazaar) {
                return new PriceQuote(itemId, bazaarQuote.sellPrice(), MarketSource.BAZAAR, bazaarQuote, null);
            }
            partialReasons.add("MISSING_OUTPUT_PRICE_BAZAAR:" + itemId);
            return null;
        }

        if (parsed.marketPreference() == MarketPreference.AUCTION) {
            if (hasConservativeAuctionSellPrice) {
                return new PriceQuote(itemId, conservativeAuctionSellPrice, MarketSource.AUCTION, null, auctionQuote);
            }
            partialReasons.add("MISSING_OUTPUT_PRICE_AUCTION:" + itemId);
            return null;
        }

        if (hasBazaar) {
            return new PriceQuote(itemId, bazaarQuote.sellPrice(), MarketSource.BAZAAR, bazaarQuote, null);
        }
        if (hasConservativeAuctionSellPrice) {
            return new PriceQuote(itemId, conservativeAuctionSellPrice, MarketSource.AUCTION, null, auctionQuote);
        }

        partialReasons.add("MISSING_OUTPUT_PRICE:" + itemId);
        return null;
    }

    private Double updateSignals(PriceQuote quote,
                                 int amount,
                                 TradeSide tradeSide,
                                 Double auctionFillHours,
                                 FlipScoreFeatureSet featureSet,
                                 List<Double> inputLegLiquidityScores,
                                 List<Double> outputLegLiquidityScores,
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
            addLiquidityScore(tradeSide, clamp(liquidityScore, 0D, 100D), inputLegLiquidityScores, outputLegLiquidityScores);

            double spreadRisk = clamp01(spreadRel / EXECUTION_SPREAD_CAP) * 100D;
            double timeRisk = clamp01(fillTimeHours / EXECUTION_TIME_CAP_HOURS) * 100D;
            legExecutionRiskScores.add(
                    (EXECUTION_SPREAD_WEIGHT * spreadRisk) + (EXECUTION_TIME_WEIGHT * timeRisk)
            );
            return fillTimeHours;
        }

        if (quote.source() == MarketSource.AUCTION && quote.auctionQuote() != null) {
            UnifiedFlipInputSnapshot.AuctionQuote auction = quote.auctionQuote();
            double sampleLiquidity = 1D - Math.exp(-(double) auction.sampleSize() / 12D);
            double spreadRel = computeAuctionRelativeSpread(auction);
            double spreadLiquidityFactor = 1D / (1D + (spreadRel / 0.05D));
            double liquidityScore = clamp(sampleLiquidity * spreadLiquidityFactor * 100D, 0D, 100D);
            addLiquidityScore(tradeSide, liquidityScore, inputLegLiquidityScores, outputLegLiquidityScores);

            double sampleRisk = (1D - clamp(sampleLiquidity, 0D, 1D)) * 100D;
            double spreadRisk = clamp01(spreadRel / 0.20D) * 100D;
            legExecutionRiskScores.add((0.6D * sampleRisk) + (0.4D * spreadRisk));
            return auctionFillHours == null || auctionFillHours <= 0D
                    ? null
                    : clamp(auctionFillHours, 0D, MAX_TIME_FOR_SCORING_HOURS);
        }

        return null;
    }

    private void addLiquidityScore(TradeSide tradeSide,
                                   double score,
                                   List<Double> inputLegLiquidityScores,
                                   List<Double> outputLegLiquidityScores) {
        if (tradeSide == TradeSide.BUY) {
            inputLegLiquidityScores.add(score);
            return;
        }
        outputLegLiquidityScores.add(score);
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

    private Double conservativeLiquidity(Double inputLiquidityScore, Double outputLiquidityScore) {
        if (inputLiquidityScore == null && outputLiquidityScore == null) {
            return null;
        }
        if (inputLiquidityScore == null) {
            return outputLiquidityScore;
        }
        if (outputLiquidityScore == null) {
            return inputLiquidityScore;
        }
        return Math.min(inputLiquidityScore, outputLiquidityScore);
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

    private double computeAuctionRelativeSpread(UnifiedFlipInputSnapshot.AuctionQuote quote) {
        if (quote == null || quote.lowestStartingBid() <= 0L || quote.secondLowestStartingBid() <= 0L) {
            return 1D;
        }
        double low = quote.lowestStartingBid();
        double high = Math.max(low, quote.secondLowestStartingBid());
        return Math.max(0D, (high - low) / low);
    }

    private double resolveConservativeAuctionSellUnitPrice(UnifiedFlipInputSnapshot.AuctionQuote quote) {
        if (quote == null) {
            return 0D;
        }
        if (quote.p25ObservedPrice() > 0D && quote.secondLowestStartingBid() > 0L) {
            return Math.min(quote.p25ObservedPrice(), quote.secondLowestStartingBid());
        }
        if (quote.p25ObservedPrice() > 0D) {
            return quote.p25ObservedPrice();
        }
        if (quote.secondLowestStartingBid() > 0L && quote.medianObservedPrice() > 0D) {
            return Math.min(quote.secondLowestStartingBid(), quote.medianObservedPrice() * 0.97D);
        }
        if (quote.secondLowestStartingBid() > 0L) {
            return quote.secondLowestStartingBid();
        }
        if (quote.medianObservedPrice() > 0D) {
            return quote.medianObservedPrice() * 0.97D;
        }
        if (quote.averageObservedPrice() > 0D) {
            return quote.averageObservedPrice() * 0.95D;
        }
        return quote.highestObservedBid() > 0L ? quote.highestObservedBid() : 0D;
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

    private ParsedItemStack parseItemStack(Step step, StepParseCache stepParseCache) {
        if (step == null) {
            return null;
        }
        if (stepParseCache == null) {
            return parseItemStack(step.getParamsJson());
        }
        Optional<ParsedItemStack> cached = stepParseCache.parsedItemStacks().get(step);
        if (cached != null) {
            return cached.orElse(null);
        }
        ParsedItemStack parsed = parseItemStack(step.getParamsJson());
        stepParseCache.parsedItemStacks().put(step, Optional.ofNullable(parsed));
        return parsed;
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

    private record StepParseCache(
            Map<Step, Optional<ParsedItemStack>> parsedItemStacks
    ) {
        private static StepParseCache create() {
            return new StepParseCache(new IdentityHashMap<>());
        }
    }
}
