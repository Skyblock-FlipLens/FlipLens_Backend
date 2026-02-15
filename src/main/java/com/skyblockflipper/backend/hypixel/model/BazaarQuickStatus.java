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
public class BazaarQuickStatus {
    @JsonProperty("buyPrice")
    private double buyPrice;

    @JsonProperty("sellPrice")
    private double sellPrice;

    @JsonProperty("buyVolume")
    private long buyVolume;

    @JsonProperty("sellVolume")
    private long sellVolume;

    @JsonProperty("buyMovingWeek")
    private long buyMovingWeek;

    @JsonProperty("sellMovingWeek")
    private long sellMovingWeek;

    @JsonProperty("buyOrders")
    private int buyOrders;

    @JsonProperty("sellOrders")
    private int sellOrders;
}
