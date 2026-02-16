package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Enums.ConstraintType;
import com.skyblockflipper.backend.model.Flipping.Enums.StepType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(UnifiedFlipDtoMapper.class);
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
            log.debug("ParsedItemStack parse skipped: reason=missing_or_blank_params_json rawParamsJson='{}' parsedType={} objectMapper={}",
                    paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(paramsJson);
            JsonNode itemNode = node.path("itemId");
            if (!itemNode.isString()) {
                log.warn("ParsedItemStack parse failed: reason=missing_or_invalid_itemId rawParamsJson='{}' parsedType={} objectMapper={}",
                        paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
                return null;
            }
            String itemId = itemNode.asString();
            if (itemId.isBlank()) {
                log.warn("ParsedItemStack parse failed: reason=blank_itemId rawParamsJson='{}' parsedType={} objectMapper={}",
                        paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
                return null;
            }
            int amount = 1;
            JsonNode amountNode = node.path("amount");
            if (amountNode.isInt() || amountNode.isLong()) {
                amount = amountNode.asInt();
            } else if (amountNode.isString()) {
                try {
                    amount = Integer.parseInt(amountNode.asString().trim());
                } catch (NumberFormatException e) {
                    log.warn("ParsedItemStack amount parse fallback: reason=invalid_amount_format rawAmount='{}' rawParamsJson='{}' parsedType={} objectMapper={}",
                            amountNode.asString(), paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName(), e);
                }
            } else if (amountNode.isMissingNode() || amountNode.isNull()) {
                log.debug("ParsedItemStack amount defaulted: reason=missing_amount rawParamsJson='{}' parsedType={} objectMapper={}",
                        paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
            } else {
                log.warn("ParsedItemStack amount defaulted: reason=unsupported_amount_type amountNode='{}' rawParamsJson='{}' parsedType={} objectMapper={}",
                        amountNode, paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName());
            }
            return new ParsedItemStack(itemId, Math.max(1, amount));
        } catch (Exception e) {
            log.warn("ParsedItemStack parse failed: reason=exception_during_json_parse rawParamsJson='{}' parsedType={} objectMapper={}",
                    paramsJson, ParsedItemStack.class.getSimpleName(), objectMapper.getClass().getName(), e);
            return null;
        }
    }

    private record ParsedItemStack(String itemId, int amount) {
    }
}
