package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.flippingstorage.FlipCurrentEntity;
import com.skyblockflipper.backend.model.flippingstorage.FlipDefinitionEntity;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoredFlipDtoMapperTest {

    @Test
    void toDtoMapsStructuredJsonAndAggregatesInputOutputItems() {
        StoredFlipDtoMapper mapper = new StoredFlipDtoMapper(new ObjectMapper());
        Instant snapshotTimestamp = Instant.parse("2026-02-20T12:00:00Z");
        FlipCurrentEntity current = new FlipCurrentEntity();
        current.setFlipKey("key-1");
        current.setStableFlipId(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
        current.setFlipType(FlipType.BAZAAR);
        current.setSnapshotTimestampEpochMillis(snapshotTimestamp.toEpochMilli());
        current.setRequiredCapital(1_000_000L);
        current.setExpectedProfit(250_000L);
        current.setRoi(0.25D);
        current.setRoiPerHour(1.0D);
        current.setDurationSeconds(120L);
        current.setFees(1_000L);
        current.setLiquidityScore(85D);
        current.setRiskScore(10D);
        current.setPartial(true);
        current.setPartialReasonsJson("[\" low_volume \",\"low_volume\",\"\",\"slow_sell\"]");

        FlipDefinitionEntity definition = new FlipDefinitionEntity();
        definition.setFlipKey("key-1");
        definition.setStableFlipId(current.getStableFlipId());
        definition.setFlipType(FlipType.BAZAAR);
        definition.setResultItemId("RESULT_ITEM");
        definition.setStepsJson("""
                [
                  {"type":"buy","durationType":"market_based","baseDurationSeconds":"30","durationFactor":"1.5","resource":"none","resourceUnits":0,"schedulingPolicy":"best_effort","paramsJson":"{\\"itemId\\":\\"INK_SACK:3\\",\\"amount\\":2}"},
                  {"type":"BUY","durationType":"MARKET_BASED","baseDurationSeconds":45,"durationFactor":null,"resource":"NONE","resourceUnits":0,"schedulingPolicy":"BEST_EFFORT","paramsJson":"{\\"itemId\\":\\"INK_SACK:3\\",\\"amount\\":3}"},
                  {"type":"sell","durationType":"market_based","baseDurationSeconds":12,"durationFactor":"0.8","resource":"none","resourceUnits":0,"schedulingPolicy":"best_effort","paramsJson":"{\\"itemId\\":\\"ENCHANTED_INK_SACK\\",\\"amount\\":2}"}
                ]
                """);
        definition.setConstraintsJson("""
                [
                  {"type":"MIN_CAPITAL","stringValue":"","intValue":"42","longValue":"1000000"},
                  {"type":"UNKNOWN_TYPE","stringValue":"ignored","intValue":"x","longValue":"y"}
                ]
                """);
        definition.setKeyVersion(1);
        definition.setStepsJson(definition.getStepsJson());
        definition.setConstraintsJson(definition.getConstraintsJson());

        UnifiedFlipDto dto = mapper.toDto(current, definition);

        assertNotNull(dto);
        assertEquals(current.getStableFlipId(), dto.id());
        assertEquals(FlipType.BAZAAR, dto.flipType());
        assertEquals(snapshotTimestamp, dto.snapshotTimestamp());
        assertEquals(1_000_000L, dto.requiredCapital());
        assertEquals(250_000L, dto.expectedProfit());
        assertEquals(List.of("low_volume", "slow_sell"), dto.partialReasons());
        assertEquals(3, dto.steps().size());
        assertEquals(2, dto.constraints().size());
        assertEquals(1, dto.inputItems().size());
        assertEquals("INK_SACK:3", dto.inputItems().getFirst().itemId());
        assertEquals(5, dto.inputItems().getFirst().amount());
        assertEquals(2, dto.outputItems().size());
        Map<String, Integer> outputByItem = dto.outputItems().stream()
                .collect(Collectors.toMap(UnifiedFlipDto.ItemStackDto::itemId, UnifiedFlipDto.ItemStackDto::amount, (left, right) -> right, java.util.LinkedHashMap::new));
        assertEquals(1, outputByItem.get("RESULT_ITEM"));
        assertEquals(2, outputByItem.get("ENCHANTED_INK_SACK"));
    }

    @Test
    void toDtoHandlesInvalidJsonGracefully() {
        StoredFlipDtoMapper mapper = new StoredFlipDtoMapper(new ObjectMapper());
        FlipCurrentEntity current = new FlipCurrentEntity();
        current.setFlipKey("key-2");
        current.setStableFlipId(UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
        current.setFlipType(FlipType.AUCTION);
        current.setSnapshotTimestampEpochMillis(Instant.parse("2026-02-20T12:00:00Z").toEpochMilli());
        current.setPartial(false);
        current.setPartialReasonsJson("{\"unexpected\":true}");

        FlipDefinitionEntity definition = new FlipDefinitionEntity();
        definition.setFlipKey("key-2");
        definition.setStableFlipId(current.getStableFlipId());
        definition.setFlipType(FlipType.AUCTION);
        definition.setResultItemId("ITEM");
        definition.setStepsJson("{broken-json");
        definition.setConstraintsJson("{broken-json");

        UnifiedFlipDto dto = mapper.toDto(current, definition);

        assertNotNull(dto);
        assertTrue(dto.steps().isEmpty());
        assertTrue(dto.constraints().isEmpty());
        assertTrue(dto.inputItems().isEmpty());
        assertEquals(List.of(new UnifiedFlipDto.ItemStackDto("ITEM", 1)), dto.outputItems());
        assertTrue(dto.partialReasons().isEmpty());
    }

    @Test
    void toDtoReturnsNullWhenCurrentOrDefinitionIsMissing() {
        StoredFlipDtoMapper mapper = new StoredFlipDtoMapper(new ObjectMapper());
        assertNull(mapper.toDto(null, new FlipDefinitionEntity()));
        assertNull(mapper.toDto(new FlipCurrentEntity(), null));
    }
}
