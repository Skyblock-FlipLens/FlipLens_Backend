package com.skyblockflipper.backend.hypixel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@AllArgsConstructor
public class AuctionResponse {
    private boolean success;
    private int page;
    private int totalPages;
    private int totalAuctions;
    private long lastUpdated;
    private List<Auction> auctions;
}
