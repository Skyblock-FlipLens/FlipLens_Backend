package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;

import java.util.List;

public record FlipTypesDto(
        List<FlipType> flipTypes
) {
    public FlipTypesDto {
        flipTypes = flipTypes == null ? List.of() : List.copyOf(flipTypes);
    }
}
