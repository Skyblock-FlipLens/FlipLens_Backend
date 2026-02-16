package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class UnifiedFlipDtoMapper {

    private final ObjectMapper objectMapper;

    public UnifiedFlipDtoMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public UnifiedFlipDto toDto(Flip flip) {
        if (flip == null) {
            return null;
        }

        Long requiredCapital = resolveRequiredCapital(flip.getConstraints());
        Long expectedProfit = null;
        Long fees = null;
        Double roi = computeRoi(requiredCapital, expectedProfit);
        Double roiPerHour = computeRoiPerHour(roi, flip.getTotalDuration().toSeconds());

        return new UnifiedFlipDto(
                flip.getId(),
                flip.getFlipType(),
                mapInputItems(flip.getSteps()),
                mapOutputItems(flip),
                requiredCapital,
                expectedProfit,
                roi,
                roiPerHour,
                flip.getTotalDuration().toSeconds(),
                fees,
                null,
                null,
                null,
                mapSteps(flip.getSteps()),
                mapConstraints(flip.getConstraints())
        );
    }

    private List<UnifiedFlipDto.ItemStackDto> mapInputItems(List<Step> steps) {
        if (steps == null || steps.isEmpty()) {
            return List.of();
        }

        Map<String, Integer> itemCounts = new LinkedHashMap<>();
        for (Step step : steps) {
            if (step == null || step.getType() != StepType.BUY) {
                continue;
            }
            ParsedItemStack parsed = parseItemStack(step.getParamsJson());
            if (parsed != null) {
                itemCounts.merge(parsed.itemId(), parsed.amount(), Integer::sum);
            }
        }
        return toItemStackList(itemCounts);
    }

    private List<UnifiedFlipDto.ItemStackDto> mapOutputItems(Flip flip) {
        Map<String, Integer> itemCounts = new LinkedHashMap<>();
        if (flip.getResultItemId() != null && !flip.getResultItemId().isBlank()) {
            itemCounts.put(flip.getResultItemId(), 1);
        }

        List<Step> steps = flip.getSteps();
        if (steps != null) {
            for (Step step : steps) {
                if (step == null || step.getType() != StepType.SELL) {
                    continue;
                }
                ParsedItemStack parsed = parseItemStack(step.getParamsJson());
                if (parsed != null) {
                    itemCounts.merge(parsed.itemId(), parsed.amount(), Integer::sum);
                }
            }
        }
        return toItemStackList(itemCounts);
    }

    private List<UnifiedFlipDto.StepDto> mapSteps(List<Step> steps) {
        if (steps == null || steps.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto.StepDto> result = new ArrayList<>(steps.size());
        for (Step step : steps) {
            if (step == null) {
                continue;
            }
            result.add(new UnifiedFlipDto.StepDto(
                    step.getType(),
                    step.getDurationType(),
                    step.getBaseDurationSeconds(),
                    step.getDurationFactor(),
                    step.getResource(),
                    step.getResourceUnits(),
                    step.getSchedulingPolicy(),
                    step.getParamsJson()
            ));
        }
        return List.copyOf(result);
    }

    private List<UnifiedFlipDto.ConstraintDto> mapConstraints(List<Constraint> constraints) {
        if (constraints == null || constraints.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto.ConstraintDto> result = new ArrayList<>(constraints.size());
        for (Constraint constraint : constraints) {
            if (constraint == null) {
                continue;
            }
            result.add(new UnifiedFlipDto.ConstraintDto(
                    constraint.getType(),
                    constraint.getStringValue(),
                    constraint.getIntValue(),
                    constraint.getLongValue()
            ));
        }
        return List.copyOf(result);
    }

    private Long resolveRequiredCapital(List<Constraint> constraints) {
        if (constraints == null || constraints.isEmpty()) {
            return null;
        }
        return constraints.stream()
                .filter(constraint -> constraint != null && constraint.getType() == ConstraintType.MIN_CAPITAL)
                .map(Constraint::getLongValue)
                .filter(value -> value != null && value > 0)
                .max(Comparator.naturalOrder())
                .orElse(null);
    }

    private Double computeRoi(Long requiredCapital, Long expectedProfit) {
        if (requiredCapital == null || expectedProfit == null || requiredCapital <= 0) {
            return null;
        }
        return (double) expectedProfit / requiredCapital;
    }

    private Double computeRoiPerHour(Double roi, long durationSeconds) {
        if (roi == null || durationSeconds <= 0) {
            return null;
        }
        return roi * (3600D / durationSeconds);
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

    private ParsedItemStack parseItemStack(String paramsJson) {
        if (paramsJson == null || paramsJson.isBlank()) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(paramsJson);
            JsonNode itemNode = node.path("itemId");
            if (!itemNode.isString()) {
                return null;
            }
            String itemId = itemNode.asString();
            if (itemId.isBlank()) {
                return null;
            }
            int amount = 1;
            JsonNode amountNode = node.path("amount");
            if (amountNode.isInt() || amountNode.isLong()) {
                amount = amountNode.asInt();
            } else if (amountNode.isString()) {
                try {
                    amount = Integer.parseInt(amountNode.asString().trim());
                } catch (NumberFormatException ignored) {
                }
            }
            return new ParsedItemStack(itemId, Math.max(1, amount));
        } catch (Exception ignored) {
            return null;
        }
    }

    private record ParsedItemStack(String itemId, int amount) {
    }
}
