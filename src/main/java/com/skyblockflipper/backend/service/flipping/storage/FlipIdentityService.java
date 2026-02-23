package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.model.Flipping.Constraint;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Step;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Service
public class FlipIdentityService {

    public static final int KEY_VERSION = 1;

    private final ObjectMapper objectMapper;

    public FlipIdentityService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Identity derive(Flip flip) {
        String normalizedResultItemId = normalizeString(flip.getResultItemId());
        ArrayNode canonicalSteps = buildCanonicalSteps(flip.getSteps());
        ArrayNode canonicalConstraints = buildCanonicalConstraints(flip.getConstraints());

        ObjectNode keyPayload = JsonNodeFactory.instance.objectNode();
        keyPayload.put("keyVersion", KEY_VERSION);
        keyPayload.put("flipType", flip.getFlipType() == null ? "" : flip.getFlipType().name());
        keyPayload.put("resultItemId", normalizedResultItemId);
        keyPayload.set("steps", canonicalSteps);
        keyPayload.set("constraints", canonicalConstraints);

        String canonicalKeyPayload = writeCanonicalJson(keyPayload);
        String flipKey = "v" + KEY_VERSION + "_" + sha256(canonicalKeyPayload);
        UUID stableFlipId = UUID.nameUUIDFromBytes(("flip:" + flipKey).getBytes(StandardCharsets.UTF_8));

        return new Identity(
                flipKey,
                stableFlipId,
                flip.getFlipType(),
                normalizedResultItemId,
                writeCanonicalJson(canonicalSteps),
                writeCanonicalJson(canonicalConstraints),
                KEY_VERSION
        );
    }

    private ArrayNode buildCanonicalSteps(List<Step> steps) {
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        if (steps == null) {
            return array;
        }
        for (Step step : steps) {
            if (step == null) {
                continue;
            }
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("type", step.getType() == null ? "" : step.getType().name());
            node.put("durationType", step.getDurationType() == null ? "" : step.getDurationType().name());
            if (step.getBaseDurationSeconds() == null) {
                node.putNull("baseDurationSeconds");
            } else {
                node.put("baseDurationSeconds", step.getBaseDurationSeconds());
            }
            if (step.getDurationFactor() == null) {
                node.putNull("durationFactor");
            } else {
                node.put("durationFactor", step.getDurationFactor());
            }
            node.put("resource", step.getResource() == null ? "" : step.getResource().name());
            node.put("resourceUnits", step.getResourceUnits());
            node.put("schedulingPolicy", step.getSchedulingPolicy() == null ? "" : step.getSchedulingPolicy().name());
            if (step.getParamsJson() == null || step.getParamsJson().isBlank()) {
                node.put("paramsJson", "");
            } else {
                node.put("paramsJson", canonicalizeJson(step.getParamsJson()));
            }
            array.add(sortRecursively(node));
        }
        return array;
    }

    private ArrayNode buildCanonicalConstraints(List<Constraint> constraints) {
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        if (constraints == null) {
            return array;
        }

        List<Constraint> sorted = new ArrayList<>(constraints);
        sorted.sort(Comparator
                .comparing((Constraint c) -> c.getType() == null ? "" : c.getType().name())
                .thenComparing(c -> normalizeString(c.getStringValue()))
                .thenComparing(c -> c.getIntValue() == null ? Integer.MIN_VALUE : c.getIntValue())
                .thenComparing(c -> c.getLongValue() == null ? Long.MIN_VALUE : c.getLongValue()));

        for (Constraint constraint : sorted) {
            if (constraint == null) {
                continue;
            }
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("type", constraint.getType() == null ? "" : constraint.getType().name());
            if (constraint.getStringValue() == null) {
                node.putNull("stringValue");
            } else {
                node.put("stringValue", normalizeString(constraint.getStringValue()));
            }
            if (constraint.getIntValue() == null) {
                node.putNull("intValue");
            } else {
                node.put("intValue", constraint.getIntValue());
            }
            if (constraint.getLongValue() == null) {
                node.putNull("longValue");
            } else {
                node.put("longValue", constraint.getLongValue());
            }
            array.add(sortRecursively(node));
        }
        return array;
    }

    private String canonicalizeJson(String rawJson) {
        try {
            JsonNode parsed = objectMapper.readTree(rawJson);
            return writeCanonicalJson(parsed);
        } catch (Exception ignored) {
            return normalizeString(rawJson);
        }
    }

    private String writeCanonicalJson(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(sortRecursively(node));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to canonicalize JSON payload.", e);
        }
    }

    private JsonNode sortRecursively(JsonNode node) {
        if (node == null || node.isNull()) {
            return JsonNodeFactory.instance.nullNode();
        }
        if (node.isObject()) {
            Map<String, JsonNode> sorted = new TreeMap<>();
            for (Map.Entry<String, JsonNode> entry : node.properties()) {
                sorted.put(entry.getKey(), sortRecursively(entry.getValue()));
            }
            ObjectNode normalized = JsonNodeFactory.instance.objectNode();
            sorted.forEach(normalized::set);
            return normalized;
        }
        if (node.isArray()) {
            ArrayNode normalized = JsonNodeFactory.instance.arrayNode();
            for (JsonNode element : node) {
                normalized.add(sortRecursively(element));
            }
            return normalized;
        }
        if (node.isTextual()) {
            return JsonNodeFactory.instance.textNode(normalizeString(node.asText("")));
        }
        return node;
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private String normalizeString(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    public record Identity(
            String flipKey,
            UUID stableFlipId,
            FlipType flipType,
            String resultItemId,
            String stepsJson,
            String constraintsJson,
            int keyVersion
    ) {
    }
}
