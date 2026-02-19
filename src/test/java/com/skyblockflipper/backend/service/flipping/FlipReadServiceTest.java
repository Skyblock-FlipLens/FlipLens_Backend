package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class FlipReadServiceTest {

    @Test
    void listFlipsWithoutTypeFilterUsesFindAll() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip flip = mock(Flip.class);
        UnifiedFlipDto dto = sampleDto();
        Pageable pageable = PageRequest.of(0, 20);
        when(flipRepository.findAll(pageable)).thenReturn(new PageImpl<>(List.of(flip)));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flip, context)).thenReturn(dto);

        Page<UnifiedFlipDto> result = service.listFlips(null, pageable);

        assertEquals(1, result.getTotalElements());
        assertEquals(dto, result.getContent().getFirst());
        verify(flipRepository).findAll(pageable);
        verify(contextService).loadCurrentContext();
        verify(mapper).toDto(flip, context);
    }

    @Test
    void listFlipsWithTypeFilterUsesTypeQuery() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip flip = mock(Flip.class);
        UnifiedFlipDto dto = sampleDto();
        Pageable pageable = PageRequest.of(1, 10);
        when(flipRepository.findAllByFlipType(FlipType.BAZAAR, pageable)).thenReturn(new PageImpl<>(List.of(flip)));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flip, context)).thenReturn(dto);

        Page<UnifiedFlipDto> result = service.listFlips(FlipType.BAZAAR, pageable);

        assertEquals(1, result.getTotalElements());
        assertEquals(dto, result.getContent().getFirst());
        verify(flipRepository).findAllByFlipType(FlipType.BAZAAR, pageable);
        verify(contextService).loadCurrentContext();
        verify(mapper).toDto(flip, context);
    }

    @Test
    void findFlipByIdMapsEntityWhenPresent() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        UUID id = UUID.randomUUID();
        Flip flip = mock(Flip.class);
        UnifiedFlipDto dto = sampleDto();
        when(flipRepository.findById(id)).thenReturn(Optional.of(flip));
        when(flip.getSnapshotTimestampEpochMillis()).thenReturn(null);
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flip, context)).thenReturn(dto);

        Optional<UnifiedFlipDto> result = service.findFlipById(id);

        assertTrue(result.isPresent());
        assertEquals(dto, result.get());
        verify(flipRepository).findById(id);
        verify(contextService).loadCurrentContext();
        verify(mapper).toDto(flip, context);
    }

    @Test
    void findFlipByIdReturnsEmptyWhenMissing() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);

        UUID id = UUID.randomUUID();
        when(flipRepository.findById(id)).thenReturn(Optional.empty());

        Optional<UnifiedFlipDto> result = service.findFlipById(id);

        assertTrue(result.isEmpty());
        verify(flipRepository).findById(id);
        verify(contextService, never()).loadCurrentContext();
        verifyNoInteractions(mapper);
    }

    @Test
    void findFlipByIdUsesSnapshotBoundContextWhenSnapshotTimestampExists() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        UUID id = UUID.randomUUID();
        Flip flip = mock(Flip.class);
        Instant snapshotTimestamp = Instant.parse("2026-02-18T21:00:00Z");
        UnifiedFlipDto dto = sampleDto();
        when(flipRepository.findById(id)).thenReturn(Optional.of(flip));
        when(flip.getSnapshotTimestampEpochMillis()).thenReturn(snapshotTimestamp.toEpochMilli());
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(context);
        when(mapper.toDto(flip, context)).thenReturn(dto);

        Optional<UnifiedFlipDto> result = service.findFlipById(id);

        assertTrue(result.isPresent());
        assertEquals(dto, result.get());
        verify(contextService).loadContextAsOf(snapshotTimestamp);
        verify(mapper).toDto(flip, context);
    }

    @Test
    void listSupportedFlipTypesReturnsSortedEnumValues() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);

        List<FlipType> expected = Arrays.stream(FlipType.values())
                .sorted((a, b) -> a.name().compareTo(b.name()))
                .toList();

        assertEquals(expected, service.listSupportedFlipTypes().flipTypes());
    }

    @Test
    void snapshotStatsUsesLatestSnapshotWhenNoTimestampProvided() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        long snapshotEpochMillis = Instant.parse("2026-02-19T20:00:00Z").toEpochMilli();

        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        when(flipRepository.countByFlipTypeForSnapshot(snapshotEpochMillis))
                .thenReturn(List.of(new Object[]{FlipType.AUCTION, 2L}, new Object[]{FlipType.BAZAAR, 3L}));

        var stats = service.snapshotStats(null);

        assertEquals(Instant.ofEpochMilli(snapshotEpochMillis), stats.snapshotTimestamp());
        assertEquals(5L, stats.totalFlips());
        assertEquals(2L, stats.byType().stream().filter(item -> item.flipType() == FlipType.AUCTION).findFirst().orElseThrow().count());
        assertEquals(3L, stats.byType().stream().filter(item -> item.flipType() == FlipType.BAZAAR).findFirst().orElseThrow().count());
    }

    @Test
    void snapshotStatsReturnsEmptyCountsWhenNoSnapshotsExist() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);

        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.empty());

        var stats = service.snapshotStats(null);

        assertEquals(null, stats.snapshotTimestamp());
        assertEquals(0L, stats.totalFlips());
        assertEquals(FlipType.values().length, stats.byType().size());
        assertTrue(stats.byType().stream().allMatch(item -> item.count() == 0L));
    }

    private UnifiedFlipDto sampleDto() {
        return new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.BAZAAR,
                List.of(),
                List.of(),
                null,
                null,
                null,
                null,
                0L,
                null,
                null,
                null,
                Instant.now(),
                false,
                List.of(),
                List.of(),
                List.of()
        );
    }
}
