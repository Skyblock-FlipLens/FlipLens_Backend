package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.FlipCoverageDto;
import com.skyblockflipper.backend.api.FlipGoodnessDto;
import com.skyblockflipper.backend.api.OffsetLimitPageRequest;
import com.skyblockflipper.backend.api.FlipSortBy;
import com.skyblockflipper.backend.api.FlipSummaryStatsDto;
import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageProperties;
import com.skyblockflipper.backend.service.flipping.storage.OnDemandFlipSnapshotService;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipCurrentReadService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
                .sorted(Comparator.comparing(Enum::name))
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

        assertNull(stats.snapshotTimestamp());
        assertEquals(0L, stats.totalFlips());
        assertEquals(FlipType.values().length, stats.byType().size());
        assertTrue(stats.byType().stream().allMatch(item -> item.count() == 0L));
    }

    @Test
    void flipTypeCoverageUsesLatestSnapshotCounts() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);

        long snapshotEpochMillis = Instant.parse("2026-02-19T20:00:00Z").toEpochMilli();
        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        when(flipRepository.countByFlipTypeForSnapshot(snapshotEpochMillis)).thenReturn(List.of(
                new Object[]{FlipType.AUCTION, 3L},
                new Object[]{FlipType.BAZAAR, 5L},
                new Object[]{FlipType.CRAFTING, 7L},
                new Object[]{FlipType.FORGE, 11L},
                new Object[]{FlipType.FUSION, 13L}
        ));

        FlipCoverageDto result = service.flipTypeCoverage();

        assertEquals(Instant.ofEpochMilli(snapshotEpochMillis), result.snapshotTimestamp());
        assertEquals(List.of("SHARD", "FUSION"), result.excludedFlipTypes());
        assertEquals(4, result.flipTypes().size());

        EnumMap<FlipType, Long> counts = new EnumMap<>(FlipType.class);
        for (FlipCoverageDto.FlipTypeCoverageDto typeCoverageDto : result.flipTypes()) {
            counts.put(typeCoverageDto.flipType(), typeCoverageDto.latestSnapshotCount());
            assertEquals(FlipCoverageDto.CoverageStatus.SUPPORTED, typeCoverageDto.ingestion());
            assertEquals(FlipCoverageDto.CoverageStatus.SUPPORTED, typeCoverageDto.calculation());
            assertEquals(FlipCoverageDto.CoverageStatus.SUPPORTED, typeCoverageDto.persistence());
            assertEquals(FlipCoverageDto.CoverageStatus.SUPPORTED, typeCoverageDto.api());
        }

        assertEquals(3L, counts.get(FlipType.AUCTION));
        assertEquals(5L, counts.get(FlipType.BAZAAR));
        assertEquals(7L, counts.get(FlipType.CRAFTING));
        assertEquals(11L, counts.get(FlipType.FORGE));
    }

    @Test
    void flipTypeCoverageReturnsZeroCountsWhenSnapshotMissing() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);

        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.empty());

        FlipCoverageDto result = service.flipTypeCoverage();

        assertNull(result.snapshotTimestamp());
        assertEquals(List.of("SHARD", "FUSION"), result.excludedFlipTypes());
        assertEquals(4, result.flipTypes().size());
        assertTrue(result.flipTypes().stream().allMatch(type -> type.latestSnapshotCount() == 0L));
    }

    @Test
    void filterFlipsAppliesThresholdsAndSortsByLiquidity() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);

        Instant snapshotTimestamp = Instant.parse("2026-02-19T20:00:00Z");
        FlipCalculationContext context = FlipCalculationContext.standard(null);
        Flip flipA = mock(Flip.class);
        Flip flipB = mock(Flip.class);
        Flip flipC = mock(Flip.class);

        when(flipRepository.findAllBySnapshotTimestampEpochMillis(snapshotTimestamp.toEpochMilli(), Pageable.unpaged()))
                .thenReturn(new PageImpl<>(List.of(flipA, flipB, flipC)));
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(context);

        when(mapper.toDto(flipA, context)).thenReturn(sampleScoredDto(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), 95.0D, 10.0D, 2_000_000L));
        when(mapper.toDto(flipB, context)).thenReturn(sampleScoredDto(UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), 80.0D, 50.0D, 1_000_000L));
        when(mapper.toDto(flipC, context)).thenReturn(sampleScoredDto(UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"), 92.0D, 12.0D, 3_000_000L));

        Page<UnifiedFlipDto> result = service.filterFlips(
                null,
                snapshotTimestamp,
                90.0D,
                20.0D,
                1_000_000L,
                0.5D,
                1.0D,
                5_000_000L,
                false,
                FlipSortBy.LIQUIDITY_SCORE,
                Sort.Direction.DESC,
                PageRequest.of(0, 10)
        );

        assertEquals(2, result.getTotalElements());
        assertEquals(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), result.getContent().get(0).id());
        assertEquals(UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"), result.getContent().get(1).id());
    }

    @Test
    void topLiquidityFlipsSortsDescendingLiquidity() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip flipA = mock(Flip.class);
        Flip flipB = mock(Flip.class);
        when(flipRepository.findAllByFlipType(FlipType.AUCTION, Pageable.unpaged()))
                .thenReturn(new PageImpl<>(List.of(flipA, flipB)));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flipA, context)).thenReturn(sampleScoredDto(UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd"), 70.0D, 15.0D, 1_500_000L));
        when(mapper.toDto(flipB, context)).thenReturn(sampleScoredDto(UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"), 90.0D, 25.0D, 1_500_000L));

        Page<UnifiedFlipDto> result = service.topLiquidityFlips(FlipType.AUCTION, null, PageRequest.of(0, 10));

        assertEquals(2, result.getTotalElements());
        assertEquals(UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"), result.getContent().get(0).id());
        assertEquals(UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd"), result.getContent().get(1).id());
    }

    @Test
    void lowestRiskFlipsSortsAscendingRisk() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip flipA = mock(Flip.class);
        Flip flipB = mock(Flip.class);
        when(flipRepository.findAllByFlipType(FlipType.BAZAAR, Pageable.unpaged()))
                .thenReturn(new PageImpl<>(List.of(flipA, flipB)));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flipA, context)).thenReturn(sampleScoredDto(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 60.0D, 8.0D, 1_500_000L));
        when(mapper.toDto(flipB, context)).thenReturn(sampleScoredDto(UUID.fromString("99999999-9999-9999-9999-999999999999"), 95.0D, 4.0D, 1_500_000L));

        Page<UnifiedFlipDto> result = service.lowestRiskFlips(FlipType.BAZAAR, null, PageRequest.of(0, 10));

        assertEquals(2, result.getTotalElements());
        assertEquals(UUID.fromString("99999999-9999-9999-9999-999999999999"), result.getContent().get(0).id());
        assertEquals(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), result.getContent().get(1).id());
    }

    @Test
    void topGoodnessFlipsRanksByComputedGoodness() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip flipHigh = mock(Flip.class);
        Flip flipMid = mock(Flip.class);
        Flip flipLowPenalty = mock(Flip.class);
        when(flipRepository.findAll(Pageable.unpaged())).thenReturn(new PageImpl<>(List.of(flipHigh, flipMid, flipLowPenalty)));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(flipHigh, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("10101010-1010-1010-1010-101010101010"),
                5.0D, 10_000_000L, 95.0D, 5.0D, false
        ));
        when(mapper.toDto(flipMid, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("20202020-2020-2020-2020-202020202020"),
                2.0D, 1_000_000L, 80.0D, 20.0D, false
        ));
        when(mapper.toDto(flipLowPenalty, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("30303030-3030-3030-3030-303030303030"),
                2.0D, 1_000_000L, 80.0D, 20.0D, true
        ));

        Page<FlipGoodnessDto> result = service.topGoodnessFlips(null, null, 0);

        assertEquals(3, result.getTotalElements());
        assertEquals(10, result.getSize());
        assertEquals(UUID.fromString("10101010-1010-1010-1010-101010101010"), result.getContent().get(0).flip().id());
        assertEquals(UUID.fromString("20202020-2020-2020-2020-202020202020"), result.getContent().get(1).flip().id());
        assertEquals(UUID.fromString("30303030-3030-3030-3030-303030303030"), result.getContent().get(2).flip().id());
        assertTrue(result.getContent().get(2).breakdown().partialPenaltyApplied());
    }

    @Test
    void topGoodnessFlipsExcludesMissingInputPriceEntries() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip inflatedMissingInput = mock(Flip.class);
        Flip actionable = mock(Flip.class);
        when(flipRepository.findAll(Pageable.unpaged())).thenReturn(new PageImpl<>(List.of(inflatedMissingInput, actionable)));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(inflatedMissingInput, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("40404040-4040-4040-4040-404040404040"),
                5000.0D, 500_000_000L, 99.0D, 1.0D, true, List.of("MISSING_INPUT_PRICE:WILTED_BERBERIS")
        ));
        when(mapper.toDto(actionable, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("50505050-5050-5050-5050-505050505050"),
                1.5D, 500_000L, 80.0D, 20.0D, false, List.of()
        ));

        Page<FlipGoodnessDto> result = service.topGoodnessFlips(null, null, 0);

        assertEquals(1, result.getTotalElements());
        assertEquals(UUID.fromString("50505050-5050-5050-5050-505050505050"), result.getContent().getFirst().flip().id());
    }

    @Test
    void topGoodnessFlipsDoesNotApplyPenaltyForElectionOnlyPartial() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip electionOnlyPartialFlip = mock(Flip.class);
        when(flipRepository.findAll(Pageable.unpaged())).thenReturn(new PageImpl<>(List.of(electionOnlyPartialFlip)));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(electionOnlyPartialFlip, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("51515151-5151-5151-5151-515151515151"),
                2.0D, 1_000_000L, 80.0D, 20.0D, true, List.of("MISSING_ELECTION_DATA")
        ));

        Page<FlipGoodnessDto> result = service.topGoodnessFlips(null, null, 0);

        assertEquals(1, result.getTotalElements());
        assertFalse(result.getContent().getFirst().breakdown().partialPenaltyApplied());
    }

    @Test
    void topGoodnessFlipsKeepsPenaltyForMarketRelatedPartial() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip marketPartialFlip = mock(Flip.class);
        when(flipRepository.findAll(Pageable.unpaged())).thenReturn(new PageImpl<>(List.of(marketPartialFlip)));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(marketPartialFlip, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("52525252-5252-5252-5252-525252525252"),
                2.0D, 1_000_000L, 80.0D, 20.0D, true, List.of("MISSING_ELECTION_DATA", "MISSING_OUTPUT_PRICE:ITEM")
        ));

        Page<FlipGoodnessDto> result = service.topGoodnessFlips(null, null, 0);

        assertEquals(1, result.getTotalElements());
        assertTrue(result.getContent().getFirst().breakdown().partialPenaltyApplied());
    }

    @Test
    void topGoodnessFlipsUsesFixedPageSizeTen() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        List<Flip> flips = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            flips.add(mock(Flip.class));
        }
        when(flipRepository.findAllByFlipType(FlipType.BAZAAR, Pageable.unpaged()))
                .thenReturn(new PageImpl<>(flips));
        when(contextService.loadCurrentContext()).thenReturn(context);
        for (int i = 0; i < flips.size(); i++) {
            Flip flip = flips.get(i);
            long profit = i + 1L;
            UUID id = UUID.fromString(String.format("%08d-0000-0000-0000-000000000000", i + 1));
            when(mapper.toDto(flip, context)).thenReturn(sampleGoodnessDto(id, 1.0D, profit, 50.0D, 50.0D, false));
        }

        Page<FlipGoodnessDto> secondPage = service.topGoodnessFlips(FlipType.BAZAAR, null, 1);

        assertEquals(12, secondPage.getTotalElements());
        assertEquals(10, secondPage.getSize());
        assertEquals(1, secondPage.getNumber());
        assertEquals(2, secondPage.getContent().size());
    }

    @Test
    void topGoodnessFlipsPreservesOffsetWhenNormalizingPageSize() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        List<Flip> flips = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            flips.add(mock(Flip.class));
        }
        when(flipRepository.findAll(Pageable.unpaged())).thenReturn(new PageImpl<>(flips));
        when(contextService.loadCurrentContext()).thenReturn(context);
        for (int i = 0; i < flips.size(); i++) {
            Flip flip = flips.get(i);
            long profit = i + 1L;
            UUID id = UUID.fromString(String.format("%08d-0000-0000-0000-000000000000", i + 1));
            when(mapper.toDto(flip, context)).thenReturn(sampleGoodnessDto(id, 1.0D, profit, 50.0D, 50.0D, false));
        }

        Pageable requested = OffsetLimitPageRequest.of(25L, 50, Sort.by("id").ascending());
        Page<FlipGoodnessDto> result = service.topGoodnessFlips(null, null, requested);

        assertEquals(30, result.getTotalElements());
        assertEquals(10, result.getSize());
        assertEquals(25L, result.getPageable().getOffset());
        assertEquals(5, result.getContent().size());
        assertEquals(UUID.fromString("00000005-0000-0000-0000-000000000000"), result.getContent().getFirst().flip().id());
        assertEquals(Sort.by("id").ascending(), result.getSort());
    }

    @Test
    void filterFlipsExcludesMissingInputPriceEntries() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip inflatedMissingInput = mock(Flip.class);
        Flip actionable = mock(Flip.class);
        when(flipRepository.findAll(Pageable.unpaged())).thenReturn(new PageImpl<>(List.of(inflatedMissingInput, actionable)));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(inflatedMissingInput, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("60606060-6060-6060-6060-606060606060"),
                2000.0D, 200_000_000L, 90.0D, 10.0D, true, List.of("MISSING_INPUT_PRICE_AUCTION:METAL_HEART")
        ));
        when(mapper.toDto(actionable, context)).thenReturn(sampleGoodnessDto(
                UUID.fromString("70707070-7070-7070-7070-707070707070"),
                2.0D, 2_000_000L, 70.0D, 25.0D, false, List.of()
        ));

        Page<UnifiedFlipDto> result = service.filterFlips(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                FlipSortBy.EXPECTED_PROFIT,
                Sort.Direction.DESC,
                PageRequest.of(0, 10)
        );

        assertEquals(1, result.getTotalElements());
        assertEquals(UUID.fromString("70707070-7070-7070-7070-707070707070"), result.getContent().getFirst().id());
    }

    @Test
    void summaryStatsOnDemandComputesSnapshotOnceAndLocallyFiltersByType() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        UnifiedFlipCurrentReadService unifiedFlipCurrentReadService = mock(UnifiedFlipCurrentReadService.class);
        OnDemandFlipSnapshotService onDemandFlipSnapshotService = mock(OnDemandFlipSnapshotService.class);
        FlipStorageProperties flipStorageProperties = new FlipStorageProperties();
        flipStorageProperties.setReadFromNew(true);
        flipStorageProperties.setLegacyWriteEnabled(false);
        FlipReadService service = new FlipReadService(
                flipRepository,
                mapper,
                contextService,
                unifiedFlipCurrentReadService,
                onDemandFlipSnapshotService,
                flipStorageProperties
        );

        Instant snapshot = Instant.parse("2026-02-20T21:30:00Z");
        UnifiedFlipDto bazaar = new UnifiedFlipDto(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                FlipType.BAZAAR,
                List.of(),
                List.of(),
                1_000_000L,
                500_000L,
                0.5D,
                1.2D,
                120L,
                1_000L,
                80D,
                10D,
                snapshot,
                false,
                List.of(),
                List.of(),
                List.of()
        );
        UnifiedFlipDto auction = new UnifiedFlipDto(
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                FlipType.AUCTION,
                List.of(),
                List.of(),
                2_000_000L,
                100_000L,
                0.05D,
                0.3D,
                90L,
                500L,
                65D,
                15D,
                snapshot,
                false,
                List.of(),
                List.of(),
                List.of()
        );
        when(onDemandFlipSnapshotService.computeSnapshotDtos(snapshot, null)).thenReturn(List.of(bazaar, auction));

        FlipSummaryStatsDto result = service.summaryStats(FlipType.BAZAAR, snapshot);

        assertEquals(1L, result.totalActiveFlips());
        assertEquals(500_000L, result.avgProfit());
        assertEquals(0.5D, result.avgRoi());
        assertEquals(500_000L, result.bestFlipProfit());
        assertEquals(1, result.byType().size());
        assertEquals(1L, result.byType().get(FlipType.BAZAAR.name()));
        verify(onDemandFlipSnapshotService, times(1)).computeSnapshotDtos(snapshot, null);
        verify(onDemandFlipSnapshotService, never()).computeSnapshotDtos(snapshot, FlipType.BAZAAR);
        verifyNoInteractions(unifiedFlipCurrentReadService);
        verifyNoInteractions(flipRepository);
    }

    @Test
    void listFlipsCurrentStorageNormalizesIdSortToStableFlipId() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        UnifiedFlipCurrentReadService unifiedFlipCurrentReadService = mock(UnifiedFlipCurrentReadService.class);
        OnDemandFlipSnapshotService onDemandFlipSnapshotService = mock(OnDemandFlipSnapshotService.class);
        FlipStorageProperties flipStorageProperties = new FlipStorageProperties();
        flipStorageProperties.setReadFromNew(true);
        flipStorageProperties.setLegacyWriteEnabled(true);
        FlipReadService service = new FlipReadService(
                flipRepository,
                mapper,
                contextService,
                unifiedFlipCurrentReadService,
                onDemandFlipSnapshotService,
                flipStorageProperties
        );

        UnifiedFlipDto dto = sampleDto();
        when(unifiedFlipCurrentReadService.listCurrentPage(org.mockito.ArgumentMatchers.eq(FlipType.BAZAAR), org.mockito.ArgumentMatchers.any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(dto), PageRequest.of(1, 5), 1));
        Pageable requested = PageRequest.of(1, 5, Sort.by(Sort.Order.desc("id")));

        Page<UnifiedFlipDto> result = service.listFlips(FlipType.BAZAAR, null, requested);

        assertEquals(1, result.getContent().size());
        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(unifiedFlipCurrentReadService).listCurrentPage(org.mockito.ArgumentMatchers.eq(FlipType.BAZAAR), pageableCaptor.capture());
        Pageable normalized = pageableCaptor.getValue();
        Sort.Order order = normalized.getSort().getOrderFor("stableFlipId");
        assertEquals(5L, normalized.getOffset());
        assertEquals(5, normalized.getPageSize());
        assertTrue(order != null && order.isDescending());
        verifyNoInteractions(onDemandFlipSnapshotService);
        verifyNoInteractions(flipRepository);
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

    private UnifiedFlipDto sampleScoredDto(UUID id, Double liquidityScore, Double riskScore, Long expectedProfit) {
        return new UnifiedFlipDto(
                id,
                FlipType.BAZAAR,
                List.of(),
                List.of(),
                1_000_000L,
                expectedProfit,
                0.7D,
                2.5D,
                3_600L,
                15_000L,
                liquidityScore,
                riskScore,
                Instant.parse("2026-02-19T20:00:00Z"),
                false,
                List.of(),
                List.of(),
                List.of()
        );
    }

    private UnifiedFlipDto sampleGoodnessDto(UUID id,
                                             Double roiPerHour,
                                             Long expectedProfit,
                                             Double liquidityScore,
                                             Double riskScore,
                                             boolean partial) {
        return sampleGoodnessDto(id, roiPerHour, expectedProfit, liquidityScore, riskScore, partial, List.of());
    }

    private UnifiedFlipDto sampleGoodnessDto(UUID id,
                                             Double roiPerHour,
                                             Long expectedProfit,
                                             Double liquidityScore,
                                             Double riskScore,
                                             boolean partial,
                                             List<String> partialReasons) {
        return new UnifiedFlipDto(
                id,
                FlipType.BAZAAR,
                List.of(),
                List.of(),
                1_000_000L,
                expectedProfit,
                0.5D,
                roiPerHour,
                3_600L,
                10_000L,
                liquidityScore,
                riskScore,
                Instant.parse("2026-02-19T20:00:00Z"),
                partial,
                partialReasons,
                List.of(),
                List.of()
        );
    }
}
