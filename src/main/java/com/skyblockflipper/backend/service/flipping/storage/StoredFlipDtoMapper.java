package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import com.skyblockflipper.backend.model.Flipping.Enums.DurationType;
import com.skyblockflipper.backend.model.Flipping.Enums.SchedulingPolicy;
import com.skyblockflipper.backend.model.Flipping.Enums.StepResource;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import com.skyblockflipper.backend.model.flippingstorage.FlipCurrentEntity;
import com.skyblockflipper.backend.model.flippingstorage.FlipDefinitionEntity;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class StoredFlipDtoMapper {

    private final ObjectMapper objectMapper;

    public StoredFlipDtoMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public UnifiedFlipDto toDto(FlipCurrentEntity current, FlipDefinitionEntity definition) {
        if (current == null || definition == null) {
            return null;
        }

        List<UnifiedFlipDto.StepDto> steps = parseSteps(definition.getStepsJson());
        List<UnifiedFlipDto.ConstraintDto> constraints = parseConstraints(definition.getConstraintsJson());
        List<UnifiedFlipDto.ItemStackDto> inputItems = mapInputItems(steps);
        List<UnifiedFlipDto.ItemStackDto> outputItems = mapOutputItems(steps, definition.getResultItemId());
        List<String> partialReasons = parsePartialReasons(current.getPartialReasonsJson());

        return new UnifiedFlipDto(
                current.getStableFlipId(),
                current.getFlipType(),
                inputItems,
                outputItems,
                current.getRequiredCapital(),
                current.getExpectedProfit(),
                current.getRoi(),
                current.getRoiPerHour(),
                current.getDurationSeconds(),
                current.getFees(),
                current.getLiquidityScore(),
                current.getRiskScore(),
                Instant.ofEpochMilli(current.getSnapshotTimestampEpochMillis()),
                current.isPartial(),
                partialReasons,
                steps,
                constraints
        );
    }

    private List<UnifiedFlipDto.StepDto> parseSteps(String stepsJson) {
        if (stepsJson == null || stepsJson.isBlank()) {
            return List.of();
        }
        try {
            JsonNode root = objectMapper.readTree(stepsJson);
            if (!root.isArray()) {
                return List.of();
            }
            List<UnifiedFlipDto.StepDto> result = new ArrayList<>();
            for (JsonNode node : root) {
                StepType type = enumValue(node.path("type").asString(""), StepType.class);
                DurationType durationType = enumValue(node.path("durationType").asString(""), DurationType.class);
                Long baseDurationSeconds = nullableLong(node.path("baseDurationSeconds"));
                Double durationFactor = nullableDouble(node.path("durationFactor"));
                StepResource resource = enumValue(node.path("resource").asString(""), StepResource.class, StepResource.NONE);
                int resourceUnits = node.path("resourceUnits").asInt(0);
                SchedulingPolicy schedulingPolicy = enumValue(
                        node.path("schedulingPolicy").asString(""),
                        SchedulingPolicy.class,
                        SchedulingPolicy.NONE
                );
                String paramsJson = node.path("paramsJson").asString("");
                result.add(new UnifiedFlipDto.StepDto(
                        type,
                        durationType,
                        baseDurationSeconds,
                        durationFactor,
                        resource,
                        resourceUnits,
                        schedulingPolicy,
                        paramsJson
                ));
            }
            return List.copyOf(result);
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private List<UnifiedFlipDto.ConstraintDto> parseConstraints(String constraintsJson) {
        if (constraintsJson == null || constraintsJson.isBlank()) {
            return List.of();
        }
        try {
            JsonNode root = objectMapper.readTree(constraintsJson);
            if (!root.isArray()) {
                return List.of();
            }
            List<UnifiedFlipDto.ConstraintDto> result = new ArrayList<>();
            for (JsonNode node : root) {
                ConstraintType type = enumValue(node.path("type").asString(""), ConstraintType.class);
                String stringValue = nullableText(node.path("stringValue"));
                Integer intValue = nullableInt(node.path("intValue"));
                Long longValue = nullableLong(node.path("longValue"));
                result.add(new UnifiedFlipDto.ConstraintDto(type, stringValue, intValue, longValue));
            }
            return List.copyOf(result);
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private List<String> parsePartialReasons(String partialReasonsJson) {
        if (partialReasonsJson == null || partialReasonsJson.isBlank()) {
            return List.of();
        }
        try {
            JsonNode root = objectMapper.readTree(partialReasonsJson);
            if (!root.isArray()) {
                return List.of();
            }
            LinkedHashSet<String> reasons = new LinkedHashSet<>();
            for (JsonNode node : root) {
                if (node.isString()) {
                    String value = node.asString("").trim();
                    if (!value.isEmpty()) {
                        reasons.add(value);
                    }
                }
            }
            return List.copyOf(reasons);
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private List<UnifiedFlipDto.ItemStackDto> mapInputItems(List<UnifiedFlipDto.StepDto> steps) {
        Map<String, Integer> itemCounts = new LinkedHashMap<>();
        for (UnifiedFlipDto.StepDto step : steps) {
            if (step == null || step.type() != StepType.BUY) {
                continue;
            }
            ParsedItemStack parsed = parseItemStack(step.paramsJson());
            if (parsed != null) {
                itemCounts.merge(parsed.itemId(), parsed.amount(), Integer::sum);
            }
        }
        return toItemStackList(itemCounts);
    }

    private List<UnifiedFlipDto.ItemStackDto> mapOutputItems(List<UnifiedFlipDto.StepDto> steps, String resultItemId) {
        Map<String, Integer> itemCounts = new LinkedHashMap<>();
        if (resultItemId != null && !resultItemId.isBlank()) {
            itemCounts.put(resultItemId, 1);
        }
        for (UnifiedFlipDto.StepDto step : steps) {
            if (step == null || step.type() != StepType.SELL) {
                continue;
            }
            ParsedItemStack parsed = parseItemStack(step.paramsJson());
            if (parsed != null) {
                itemCounts.merge(parsed.itemId(), parsed.amount(), Integer::sum);
            }
        }
        return toItemStackList(itemCounts);
    }

    private ParsedItemStack parseItemStack(String paramsJson) {
        if (paramsJson == null || paramsJson.isBlank()) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(paramsJson);
            String itemId = node.path("itemId").asString("");
            if (itemId.isBlank()) {
                return null;
            }
            int amount = Math.max(1, node.path("amount").asInt(1));
            return new ParsedItemStack(itemId, amount);
        } catch (Exception ignored) {
            return null;
        }
    }

    private List<UnifiedFlipDto.ItemStackDto> toItemStackList(Map<String, Integer> counts) {
        if (counts.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto.ItemStackDto> result = new ArrayList<>(counts.size());
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            result.add(new UnifiedFlipDto.ItemStackDto(entry.getKey(), entry.getValue()));
        }
        return List.copyOf(result);
    }

    private <E extends Enum<E>> E enumValue(String raw, Class<E> enumType) {
        return enumValue(raw, enumType, null);
    }

    private <E extends Enum<E>> E enumValue(String raw, Class<E> enumType, E fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Enum.valueOf(enumType, raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return fallback;
        }
    }

    private Long nullableLong(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.longValue();
        }
        if (node.isString()) {
            try {
                return Long.parseLong(node.asString().trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private Integer nullableInt(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.intValue();
        }
        if (node.isString()) {
            try {
                return Integer.parseInt(node.asString().trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private Double nullableDouble(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.doubleValue();
        }
        if (node.isString()) {
            try {
                return Double.parseDouble(node.asString().trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private String nullableText(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        String value = node.asString("");
        return value.isBlank() ? null : value;
    }

    private record ParsedItemStack(String itemId, int amount) {
    }
}
