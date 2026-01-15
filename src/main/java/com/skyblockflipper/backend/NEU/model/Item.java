package com.skyblockflipper.backend.NEU.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class Item {

    private String id;

    private String name;

    private String minecraftId;

    private String rarity;

    private String category;

    private String lore;

    @Builder.Default
    private Map<String, Double> stats = new HashMap<>();

    @Builder.Default
    private Map<String, String> metadata = new HashMap<>();
}
