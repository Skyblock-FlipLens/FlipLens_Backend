package com.skyblockflipper.backend.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "flipping")
public class FlippingModelProperties {

    private boolean auctionModelV2Enabled = true;
    private boolean selltimeModelEnabled = false;
    private boolean scoringV2Enabled = false;
    private boolean electionPenaltySoftened = true;
    private boolean recommendationGatesEnabled = false;
    private double minRecommendationLiquidityScore = 15D;
    private long minRecommendationExpectedProfit = 0L;
    private double minConfidenceScore = 25D;
    private boolean outlierProtectionEnabled = true;

    public boolean isAuctionModelV2Enabled() {
        return auctionModelV2Enabled;
    }

    public void setAuctionModelV2Enabled(boolean auctionModelV2Enabled) {
        this.auctionModelV2Enabled = auctionModelV2Enabled;
    }

    public boolean isSelltimeModelEnabled() {
        return selltimeModelEnabled;
    }

    public void setSelltimeModelEnabled(boolean selltimeModelEnabled) {
        this.selltimeModelEnabled = selltimeModelEnabled;
    }

    public boolean isScoringV2Enabled() {
        return scoringV2Enabled;
    }

    public void setScoringV2Enabled(boolean scoringV2Enabled) {
        this.scoringV2Enabled = scoringV2Enabled;
    }

    public boolean isElectionPenaltySoftened() {
        return electionPenaltySoftened;
    }

    public void setElectionPenaltySoftened(boolean electionPenaltySoftened) {
        this.electionPenaltySoftened = electionPenaltySoftened;
    }

    public boolean isRecommendationGatesEnabled() {
        return recommendationGatesEnabled;
    }

    public void setRecommendationGatesEnabled(boolean recommendationGatesEnabled) {
        this.recommendationGatesEnabled = recommendationGatesEnabled;
    }

    public double getMinRecommendationLiquidityScore() {
        return minRecommendationLiquidityScore;
    }

    public void setMinRecommendationLiquidityScore(double minRecommendationLiquidityScore) {
        this.minRecommendationLiquidityScore = minRecommendationLiquidityScore;
    }

    public long getMinRecommendationExpectedProfit() {
        return minRecommendationExpectedProfit;
    }

    public void setMinRecommendationExpectedProfit(long minRecommendationExpectedProfit) {
        this.minRecommendationExpectedProfit = minRecommendationExpectedProfit;
    }

    public double getMinConfidenceScore() {
        return minConfidenceScore;
    }

    public void setMinConfidenceScore(double minConfidenceScore) {
        this.minConfidenceScore = minConfidenceScore;
    }

    public boolean isOutlierProtectionEnabled() {
        return outlierProtectionEnabled;
    }

    public void setOutlierProtectionEnabled(boolean outlierProtectionEnabled) {
        this.outlierProtectionEnabled = outlierProtectionEnabled;
    }
}
