package com.skyblockflipper.backend.hypixel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
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
}
