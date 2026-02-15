package com.skyblockflipper.backend.hypixel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BazaarProduct {
    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("quick_status")
    private BazaarQuickStatus quickStatus;

    @JsonProperty("buy_summary")
    private List<BazaarSummaryEntry> buySummary;

    @JsonProperty("sell_summary")
    private List<BazaarSummaryEntry> sellSummary;
}
