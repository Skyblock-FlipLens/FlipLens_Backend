package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.flippingstorage.FlipCurrentEntity;
import com.skyblockflipper.backend.model.flippingstorage.FlipDefinitionEntity;
import com.skyblockflipper.backend.repository.FlipCurrentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UnifiedFlipCurrentReadServiceTest {

    @Test
    void listCurrentMapsUsingDefinitionsAndSkipsNullDtos() {
        FlipCurrentRepository currentRepository = mock(FlipCurrentRepository.class);
        StoredFlipDtoMapper dtoMapper = mock(StoredFlipDtoMapper.class);
        UnifiedFlipCurrentReadService service = new UnifiedFlipCurrentReadService(
                currentRepository,
                dtoMapper
        );
        FlipCurrentEntity currentA = current("key-a", FlipType.BAZAAR, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        FlipCurrentEntity currentB = current("key-b", FlipType.BAZAAR, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        FlipDefinitionEntity definitionA = definition("key-a", FlipType.BAZAAR, currentA.getStableFlipId());
        FlipDefinitionEntity definitionB = definition("key-b", FlipType.BAZAAR, currentB.getStableFlipId());
        UnifiedFlipDto dtoA = dto(currentA.getStableFlipId(), FlipType.BAZAAR);

        when(currentRepository.findAllWithDefinitionByFlipType(FlipType.BAZAAR))
                .thenReturn(List.of(currentDefinitionProjection(currentA, definitionA),
                        currentDefinitionProjection(currentB, definitionB)));
        when(dtoMapper.toDto(currentA, definitionA)).thenReturn(dtoA);
        when(dtoMapper.toDto(currentB, definitionB)).thenReturn(null);

        List<UnifiedFlipDto> result = service.listCurrent(FlipType.BAZAAR);

        assertEquals(1, result.size());
        assertEquals(currentA.getStableFlipId(), result.getFirst().id());
    }

    @Test
    void listCurrentPageReturnsEmptyPageWhenRepositoryPageIsEmpty() {
        FlipCurrentRepository currentRepository = mock(FlipCurrentRepository.class);
        StoredFlipDtoMapper dtoMapper = mock(StoredFlipDtoMapper.class);
        UnifiedFlipCurrentReadService service = new UnifiedFlipCurrentReadService(
                currentRepository,
                dtoMapper
        );
        Pageable pageable = PageRequest.of(0, 10);
        when(currentRepository.findAllWithDefinitionByFlipType(FlipType.AUCTION, pageable))
                .thenReturn(new PageImpl<>(List.of(), pageable, 0L));

        Page<UnifiedFlipDto> result = service.listCurrentPage(FlipType.AUCTION, pageable);

        assertEquals(0L, result.getTotalElements());
        assertTrue(result.getContent().isEmpty());
    }

    @Test
    void findByStableFlipIdReturnsMappedDtoWhenBothRowsExist() {
        FlipCurrentRepository currentRepository = mock(FlipCurrentRepository.class);
        StoredFlipDtoMapper dtoMapper = mock(StoredFlipDtoMapper.class);
        UnifiedFlipCurrentReadService service = new UnifiedFlipCurrentReadService(
                currentRepository,
                dtoMapper
        );
        UUID stableId = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc");
        FlipCurrentEntity current = current("key-c", FlipType.CRAFTING, stableId.toString());
        FlipDefinitionEntity definition = definition("key-c", FlipType.CRAFTING, stableId);
        UnifiedFlipDto mapped = dto(stableId, FlipType.CRAFTING);
        when(currentRepository.findByStableFlipIdWithDefinition(stableId))
                .thenReturn(Optional.of(currentDefinitionProjection(current, definition)));
        when(dtoMapper.toDto(current, definition)).thenReturn(mapped);

        Optional<UnifiedFlipDto> result = service.findByStableFlipId(stableId);

        assertTrue(result.isPresent());
        assertEquals(stableId, result.get().id());
    }

    @Test
    void countsByTypeStartsWithZerosAndOverridesReturnedTypes() {
        FlipCurrentRepository currentRepository = mock(FlipCurrentRepository.class);
        StoredFlipDtoMapper dtoMapper = mock(StoredFlipDtoMapper.class);
        UnifiedFlipCurrentReadService service = new UnifiedFlipCurrentReadService(
                currentRepository,
                dtoMapper
        );
        FlipCurrentRepository.FlipTypeCountProjection bazaarProjection = projection(FlipType.BAZAAR, 5L);
        FlipCurrentRepository.FlipTypeCountProjection nullProjection = projection(null, 99L);
        when(currentRepository.countByFlipType()).thenReturn(List.of(bazaarProjection, nullProjection));

        Map<FlipType, Long> result = service.countsByType();

        assertEquals(5L, result.get(FlipType.BAZAAR));
        for (FlipType flipType : FlipType.values()) {
            assertTrue(result.containsKey(flipType));
        }
    }

    @Test
    void listCurrentScoringDtosUsesCurrentRowsOnly() {
        FlipCurrentRepository currentRepository = mock(FlipCurrentRepository.class);
        StoredFlipDtoMapper dtoMapper = mock(StoredFlipDtoMapper.class);
        UnifiedFlipCurrentReadService service = new UnifiedFlipCurrentReadService(
                currentRepository,
                dtoMapper
        );
        FlipCurrentEntity current = current("key-score", FlipType.BAZAAR, "dddddddd-dddd-dddd-dddd-dddddddddddd");
        current.setExpectedProfit(1_250_000L);
        current.setRoiPerHour(2.5D);
        when(currentRepository.findAllByFlipType(FlipType.BAZAAR)).thenReturn(List.of(current));
        when(dtoMapper.parsePartialReasonsJson("[]")).thenReturn(List.of());

        List<UnifiedFlipDto> result = service.listCurrentScoringDtos(FlipType.BAZAAR);

        assertEquals(1, result.size());
        assertEquals(current.getStableFlipId(), result.getFirst().id());
        assertEquals(1_250_000L, result.getFirst().expectedProfit());
        assertTrue(result.getFirst().steps().isEmpty());
    }

    @Test
    void listCurrentByStableFlipIdsReturnsDtosInRequestedOrder() {
        FlipCurrentRepository currentRepository = mock(FlipCurrentRepository.class);
        StoredFlipDtoMapper dtoMapper = mock(StoredFlipDtoMapper.class);
        UnifiedFlipCurrentReadService service = new UnifiedFlipCurrentReadService(
                currentRepository,
                dtoMapper
        );

        UUID firstId = UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee");
        UUID secondId = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

        FlipCurrentEntity currentFirst = current("key-first", FlipType.BAZAAR, firstId.toString());
        FlipCurrentEntity currentSecond = current("key-second", FlipType.BAZAAR, secondId.toString());
        FlipDefinitionEntity definitionFirst = definition("key-first", FlipType.BAZAAR, firstId);
        FlipDefinitionEntity definitionSecond = definition("key-second", FlipType.BAZAAR, secondId);

        UnifiedFlipDto firstDto = dto(firstId, FlipType.BAZAAR);
        UnifiedFlipDto secondDto = dto(secondId, FlipType.BAZAAR);

        when(currentRepository.findAllWithDefinitionByStableFlipIds(List.of(secondId, firstId)))
                .thenReturn(List.of(
                        currentDefinitionProjection(currentFirst, definitionFirst),
                        currentDefinitionProjection(currentSecond, definitionSecond)
                ));
        when(dtoMapper.toDto(currentFirst, definitionFirst)).thenReturn(firstDto);
        when(dtoMapper.toDto(currentSecond, definitionSecond)).thenReturn(secondDto);

        List<UnifiedFlipDto> result = service.listCurrentByStableFlipIds(List.of(secondId, firstId));

        assertEquals(2, result.size());
        assertEquals(secondId, result.get(0).id());
        assertEquals(firstId, result.get(1).id());
    }

    private FlipCurrentEntity current(String key, FlipType flipType, String stableId) {
        FlipCurrentEntity entity = new FlipCurrentEntity();
        entity.setFlipKey(key);
        entity.setFlipType(flipType);
        entity.setStableFlipId(UUID.fromString(stableId));
        entity.setSnapshotTimestampEpochMillis(Instant.parse("2026-02-20T12:00:00Z").toEpochMilli());
        entity.setPartial(false);
        entity.setPartialReasonsJson("[]");
        return entity;
    }

    private FlipDefinitionEntity definition(String key, FlipType flipType, UUID stableId) {
        FlipDefinitionEntity definition = new FlipDefinitionEntity();
        definition.setFlipKey(key);
        definition.setFlipType(flipType);
        definition.setStableFlipId(stableId);
        return definition;
    }

    private UnifiedFlipDto dto(UUID id, FlipType flipType) {
        return new UnifiedFlipDto(
                id,
                flipType,
                List.of(),
                List.of(),
                1_000_000L,
                100_000L,
                0.2D,
                0.8D,
                120L,
                1_000L,
                80D,
                10D,
                Instant.parse("2026-02-20T12:00:00Z"),
                false,
                List.of(),
                List.of(),
                List.of()
        );
    }

    private FlipCurrentRepository.FlipTypeCountProjection projection(FlipType flipType, long count) {
        return new FlipCurrentRepository.FlipTypeCountProjection() {
            @Override
            public FlipType getFlipType() {
                return flipType;
            }

            @Override
            public long getCount() {
                return count;
            }
        };
    }

    private FlipCurrentRepository.CurrentDefinitionProjection currentDefinitionProjection(FlipCurrentEntity current,
                                                                                          FlipDefinitionEntity definition) {
        return new FlipCurrentRepository.CurrentDefinitionProjection() {
            @Override
            public FlipCurrentEntity getCurrent() {
                return current;
            }

            @Override
            public FlipDefinitionEntity getDefinition() {
                return definition;
            }
        };
    }
}

