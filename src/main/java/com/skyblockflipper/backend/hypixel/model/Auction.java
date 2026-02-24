package com.skyblockflipper.backend.hypixel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
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
    @JsonProperty("bin")
    private boolean bin;
    @JsonProperty("claimed_bidders")
    private List<String> claimedBidders;
    @JsonProperty("highest_bid_amount")
    private long highestBidAmount;
    private List<Bid> bids;

    public Auction() {
    }

    public Auction(String uuid,
                   String auctioneer,
                   String profileId,
                   List<String> coop,
                   long start,
                   long end,
                   String itemName,
                   String itemLore,
                   String extra,
                   String category,
                   String tier,
                   long startingBid,
                   boolean claimed,
                   List<String> claimedBidders,
                   long highestBidAmount,
                   List<Bid> bids) {
        this(uuid, auctioneer, profileId, coop, start, end, itemName, itemLore, extra, category, tier, startingBid, claimed, false, claimedBidders, highestBidAmount, bids);
    }

    public Auction(String uuid,
                   String auctioneer,
                   String profileId,
                   List<String> coop,
                   long start,
                   long end,
                   String itemName,
                   String itemLore,
                   String extra,
                   String category,
                   String tier,
                   long startingBid,
                   boolean claimed,
                   boolean bin,
                   List<String> claimedBidders,
                   long highestBidAmount,
                   List<Bid> bids) {
        this.uuid = uuid;
        this.auctioneer = auctioneer;
        this.profileId = profileId;
        this.coop = coop;
        this.start = start;
        this.end = end;
        this.itemName = itemName;
        this.itemLore = itemLore;
        this.extra = extra;
        this.category = category;
        this.tier = tier;
        this.startingBid = startingBid;
        this.claimed = claimed;
        this.bin = bin;
        this.claimedBidders = claimedBidders;
        this.highestBidAmount = highestBidAmount;
        this.bids = bids;
    }

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
