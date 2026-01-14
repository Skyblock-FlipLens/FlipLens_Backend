package com.skyblockflipper.backend.hypixel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@AllArgsConstructor
public class Auction {
    private String uuid;
    private String auctioneer;
    @JsonProperty("profile_id")
    private String profileId;
    private List<String> coop;
    private long start;
    private long end;
    @JsonProperty("item_name")
    private String itemName;
    @JsonProperty("item_lore")
    private String itemLore;
    private String extra;
    private String category;
    private String tier;
    @JsonProperty("starting_bid")
    private long startingBid;
    private boolean claimed;
    @JsonProperty("claimed_bidders")
    private List<String> claimedBidders;
    @JsonProperty("highest_bid_amount")
    private long highestBidAmount;
    private List<Bid> bids;

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Data
    @AllArgsConstructor
    public static class Bid {
        @JsonProperty("auction_id")
        private String auctionId;
        private String bidder;
        @JsonProperty("profile_id")
        private String profileId;
        private long amount;
        private long timestamp;
    }
}
