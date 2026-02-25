package com.skyblockflipper.backend.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "flipping")
public class FlippingModelProperties {

    private boolean auctionModelV2Enabled = true;
    private boolean selltimeModelEnabled = false;
    private boolean scoringV2Enabled = false;
    private boolean electionPenaltySoftened = true;

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
}
