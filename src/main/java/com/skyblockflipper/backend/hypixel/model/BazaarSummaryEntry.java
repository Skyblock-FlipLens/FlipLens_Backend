package com.skyblockflipper.backend.hypixel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BazaarSummaryEntry {
    @JsonProperty("amount")
    private long amount;

    @JsonProperty("pricePerUnit")
    private double pricePerUnit;

    @JsonProperty("orders")
    private int orders;
}
