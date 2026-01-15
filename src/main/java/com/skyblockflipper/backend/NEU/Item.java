package com.skyblockflipper.backend.NEU;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.persistence.*;
import lombok.*;

import java.util.HashMap;
import java.util.Map;

@Entity
@Table(name = "items")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Item {

    @Id
    @Column(name = "id", nullable = false, updatable = false, length = 100)
    private String id;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "minecraft_id")
    private String minecraftId;

    @Column(name = "rarity")
    private String rarity;

    @Column(name = "category")
    private String category;

    @Lob
    @Column(name = "lore", columnDefinition = "TEXT")
    private String lore;

    @ElementCollection
    @CollectionTable(
            name = "item_stats",
            joinColumns = @JoinColumn(name = "item_id")
    )
    @MapKeyColumn(name = "stat_key")
    @Column(name = "stat_value")
    @Builder.Default
    private Map<String, Double> stats = new HashMap<>();

    @ElementCollection
    @CollectionTable(
            name = "item_metadata",
            joinColumns = @JoinColumn(name = "item_id")
    )
    @MapKeyColumn(name = "meta_key")
    @Column(name = "meta_value", columnDefinition = "TEXT")
    @Builder.Default
    private Map<String, String> metadata = new HashMap<>();
}
