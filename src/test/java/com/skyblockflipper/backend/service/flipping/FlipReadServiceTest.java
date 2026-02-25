package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.FlipCoverageDto;
import com.skyblockflipper.backend.api.FlipGoodnessDto;
import com.skyblockflipper.backend.api.OffsetLimitPageRequest;
import com.skyblockflipper.backend.api.FlipSortBy;
import com.skyblockflipper.backend.api.FlipSummaryStatsDto;
import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.config.properties.FlippingModelProperties;
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
    void listFlipsWithoutTypeFilterUsesTwoPhaseIdLoad() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip flip = mock(Flip.class);
        UUID flipId = UUID.fromString("90909090-9090-9090-9090-909090909090");
        UnifiedFlipDto dto = sampleDto();
        Pageable pageable = PageRequest.of(0, 20);
        when(flip.getId()).thenReturn(flipId);
        when(flipRepository.findAllIds(pageable)).thenReturn(new PageImpl<>(List.of(flipId), pageable, 1));
        when(flipRepository.findAllByIdInWithDetails(List.of(flipId))).thenReturn(List.of(flip));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flip, context)).thenReturn(dto);

        Page<UnifiedFlipDto> result = service.listFlips(null, pageable);

        assertEquals(1, result.getTotalElements());
        assertEquals(dto, result.getContent().getFirst());
        verify(flipRepository).findAllIds(pageable);
        verify(flipRepository).findAllByIdInWithDetails(List.of(flipId));
        verify(contextService).loadCurrentContext();
        verify(mapper).toDto(flip, context);
    }

    @Test
    void listFlipsWithTypeFilterUsesTwoPhaseTypeQuery() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip flip = mock(Flip.class);
        UUID flipId = UUID.fromString("91919191-9191-9191-9191-919191919191");
        UnifiedFlipDto dto = sampleDto();
        Pageable pageable = PageRequest.of(0, 10);
        when(flip.getId()).thenReturn(flipId);
        when(flipRepository.findIdsByFlipType(FlipType.BAZAAR, pageable))
                .thenReturn(new PageImpl<>(List.of(flipId), pageable, 1));
        when(flipRepository.findAllByIdInWithDetails(List.of(flipId))).thenReturn(List.of(flip));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flip, context)).thenReturn(dto);

        Page<UnifiedFlipDto> result = service.listFlips(FlipType.BAZAAR, pageable);

        assertEquals(1, result.getTotalElements());
        assertEquals(dto, result.getContent().getFirst());
        verify(flipRepository).findIdsByFlipType(FlipType.BAZAAR, pageable);
        verify(flipRepository).findAllByIdInWithDetails(List.of(flipId));
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
        UUID idA = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        UUID idB = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        UUID idC = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc");
        when(flipA.getId()).thenReturn(idA);
        when(flipB.getId()).thenReturn(idB);
        when(flipC.getId()).thenReturn(idC);

        stubLegacyPagedBySnapshot(
                flipRepository,
                snapshotTimestamp.toEpochMilli(),
                List.of(idA, idB, idC),
                List.of(flipA, flipB, flipC)
        );
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(context);

        when(mapper.toDto(flipA, context)).thenReturn(sampleScoredDto(idA, 95.0D, 10.0D, 2_000_000L));
        when(mapper.toDto(flipB, context)).thenReturn(sampleScoredDto(idB, 80.0D, 50.0D, 1_000_000L));
        when(mapper.toDto(flipC, context)).thenReturn(sampleScoredDto(idC, 92.0D, 12.0D, 3_000_000L));

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
        UUID idA = UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd");
        UUID idB = UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee");
        when(flipA.getId()).thenReturn(idA);
        when(flipB.getId()).thenReturn(idB);
        stubLegacyPagedByType(flipRepository, FlipType.AUCTION, List.of(idA, idB), List.of(flipA, flipB));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flipA, context)).thenReturn(sampleScoredDto(idA, 70.0D, 15.0D, 1_500_000L));
        when(mapper.toDto(flipB, context)).thenReturn(sampleScoredDto(idB, 90.0D, 25.0D, 1_500_000L));

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
        UUID idA = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
        UUID idB = UUID.fromString("99999999-9999-9999-9999-999999999999");
        when(flipA.getId()).thenReturn(idA);
        when(flipB.getId()).thenReturn(idB);
        stubLegacyPagedByType(flipRepository, FlipType.BAZAAR, List.of(idA, idB), List.of(flipA, flipB));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(flipA, context)).thenReturn(sampleScoredDto(idA, 60.0D, 8.0D, 1_500_000L));
        when(mapper.toDto(flipB, context)).thenReturn(sampleScoredDto(idB, 95.0D, 4.0D, 1_500_000L));

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
        UUID highId = UUID.fromString("10101010-1010-1010-1010-101010101010");
        UUID midId = UUID.fromString("20202020-2020-2020-2020-202020202020");
        UUID lowPenaltyId = UUID.fromString("30303030-3030-3030-3030-303030303030");
        when(flipHigh.getId()).thenReturn(highId);
        when(flipMid.getId()).thenReturn(midId);
        when(flipLowPenalty.getId()).thenReturn(lowPenaltyId);
        stubLegacyPagedAll(flipRepository, List.of(highId, midId, lowPenaltyId), List.of(flipHigh, flipMid, flipLowPenalty));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(flipHigh, context)).thenReturn(sampleGoodnessDto(
                highId,
                5.0D, 10_000_000L, 95.0D, 5.0D, false
        ));
        when(mapper.toDto(flipMid, context)).thenReturn(sampleGoodnessDto(
                midId,
                2.0D, 1_000_000L, 80.0D, 20.0D, false
        ));
        when(mapper.toDto(flipLowPenalty, context)).thenReturn(sampleGoodnessDto(
                lowPenaltyId,
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
        UUID inflatedId = UUID.fromString("40404040-4040-4040-4040-404040404040");
        UUID actionableId = UUID.fromString("50505050-5050-5050-5050-505050505050");
        when(inflatedMissingInput.getId()).thenReturn(inflatedId);
        when(actionable.getId()).thenReturn(actionableId);
        stubLegacyPagedAll(flipRepository, List.of(inflatedId, actionableId), List.of(inflatedMissingInput, actionable));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(inflatedMissingInput, context)).thenReturn(sampleGoodnessDto(
                inflatedId,
                5000.0D, 500_000_000L, 99.0D, 1.0D, true, List.of("MISSING_INPUT_PRICE:WILTED_BERBERIS")
        ));
        when(mapper.toDto(actionable, context)).thenReturn(sampleGoodnessDto(
                actionableId,
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
        UUID electionOnlyId = UUID.fromString("51515151-5151-5151-5151-515151515151");
        when(electionOnlyPartialFlip.getId()).thenReturn(electionOnlyId);
        stubLegacyPagedAll(flipRepository, List.of(electionOnlyId), List.of(electionOnlyPartialFlip));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(electionOnlyPartialFlip, context)).thenReturn(sampleGoodnessDto(
                electionOnlyId,
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
        UUID marketPartialId = UUID.fromString("52525252-5252-5252-5252-525252525252");
        when(marketPartialFlip.getId()).thenReturn(marketPartialId);
        stubLegacyPagedAll(flipRepository, List.of(marketPartialId), List.of(marketPartialFlip));
        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(marketPartialFlip, context)).thenReturn(sampleGoodnessDto(
                marketPartialId,
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
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            flips.add(mock(Flip.class));
            ids.add(UUID.fromString(String.format("%08d-0000-0000-0000-000000000000", i + 1)));
        }
        for (int i = 0; i < flips.size(); i++) {
            when(flips.get(i).getId()).thenReturn(ids.get(i));
        }
        stubLegacyPagedByType(flipRepository, FlipType.BAZAAR, ids, flips);
        when(contextService.loadCurrentContext()).thenReturn(context);
        for (int i = 0; i < flips.size(); i++) {
            Flip flip = flips.get(i);
            long profit = i + 1L;
            UUID id = ids.get(i);
            when(mapper.toDto(flip, context)).thenReturn(sampleGoodnessDto(id, 1.0D, profit, 50.0D, 50.0D, false));
        }

        Page<FlipGoodnessDto> secondPage = service.topGoodnessFlips(FlipType.BAZAAR, null, 1);

        assertEquals(12, secondPage.getTotalElements());
        assertEquals(10, secondPage.getSize());
        assertEquals(1, secondPage.getNumber());
        assertEquals(2, secondPage.getContent().size());
    }

    @Test
    void topGoodnessFlipsCachesRankingPerResolvedSnapshot() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Instant snapshotTimestamp = Instant.parse("2026-02-21T10:00:00Z");
        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        Flip flip = mock(Flip.class);
        UUID flipId = UUID.fromString("61616161-6161-6161-6161-616161616161");
        when(flip.getId()).thenReturn(flipId);

        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        stubLegacyPagedBySnapshot(flipRepository, snapshotEpochMillis, List.of(flipId), List.of(flip));
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(context);
        when(mapper.toDto(flip, context)).thenReturn(sampleGoodnessDto(
                flipId,
                1.0D, 1_000_000L, 70.0D, 15.0D, false
        ));

        Page<FlipGoodnessDto> first = service.topGoodnessFlips(null, null, 0);
        Page<FlipGoodnessDto> second = service.topGoodnessFlips(null, null, 0);

        assertEquals(1, first.getTotalElements());
        assertEquals(1, second.getTotalElements());
        verify(flipRepository, times(1))
                .findIdsBySnapshotTimestampEpochMillis(snapshotEpochMillis, PageRequest.of(0, 500));
        verify(contextService, times(1)).loadContextAsOf(snapshotTimestamp);
    }

    @Test
    void topGoodnessFlipsPreservesOffsetWhenNormalizingPageSize() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        List<Flip> flips = new ArrayList<>();
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            flips.add(mock(Flip.class));
            ids.add(UUID.fromString(String.format("%08d-0000-0000-0000-000000000000", i + 1)));
        }
        for (int i = 0; i < flips.size(); i++) {
            when(flips.get(i).getId()).thenReturn(ids.get(i));
        }
        stubLegacyPagedAll(flipRepository, ids, flips);
        when(contextService.loadCurrentContext()).thenReturn(context);
        for (int i = 0; i < flips.size(); i++) {
            Flip flip = flips.get(i);
            long profit = i + 1L;
            UUID id = ids.get(i);
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
    void topGoodnessFlipsCurrentStorageHydratesPagedEntries() {
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

        UUID highId = UUID.fromString("83838383-8383-8383-8383-838383838383");
        UUID lowId = UUID.fromString("84848484-8484-8484-8484-848484848484");
        UnifiedFlipDto highScoreSkeleton = sampleGoodnessDto(highId, 4.0D, 4_000_000L, 80.0D, 10.0D, false);
        UnifiedFlipDto lowScoreSkeleton = sampleGoodnessDto(lowId, 1.0D, 1_000_000L, 60.0D, 20.0D, false);
        when(unifiedFlipCurrentReadService.latestSnapshotEpochMillis())
                .thenReturn(Optional.of(Instant.parse("2026-02-25T15:00:00Z").toEpochMilli()));
        when(unifiedFlipCurrentReadService.listCurrentScoringDtos(null))
                .thenReturn(List.of(lowScoreSkeleton, highScoreSkeleton));

        UnifiedFlipDto hydratedHigh = new UnifiedFlipDto(
                highId,
                FlipType.BAZAAR,
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_DIAMOND", 16)),
                List.of(new UnifiedFlipDto.ItemStackDto("POWER_SCROLL", 1)),
                highScoreSkeleton.requiredCapital(),
                highScoreSkeleton.expectedProfit(),
                highScoreSkeleton.roi(),
                highScoreSkeleton.roiPerHour(),
                highScoreSkeleton.durationSeconds(),
                highScoreSkeleton.fees(),
                highScoreSkeleton.liquidityScore(),
                highScoreSkeleton.riskScore(),
                highScoreSkeleton.snapshotTimestamp(),
                highScoreSkeleton.partial(),
                highScoreSkeleton.partialReasons(),
                List.of(),
                List.of()
        );
        UnifiedFlipDto hydratedLow = new UnifiedFlipDto(
                lowId,
                FlipType.BAZAAR,
                List.of(new UnifiedFlipDto.ItemStackDto("WHEAT", 160)),
                List.of(new UnifiedFlipDto.ItemStackDto("HAY_BLOCK", 1)),
                lowScoreSkeleton.requiredCapital(),
                lowScoreSkeleton.expectedProfit(),
                lowScoreSkeleton.roi(),
                lowScoreSkeleton.roiPerHour(),
                lowScoreSkeleton.durationSeconds(),
                lowScoreSkeleton.fees(),
                lowScoreSkeleton.liquidityScore(),
                lowScoreSkeleton.riskScore(),
                lowScoreSkeleton.snapshotTimestamp(),
                lowScoreSkeleton.partial(),
                lowScoreSkeleton.partialReasons(),
                List.of(),
                List.of()
        );
        when(unifiedFlipCurrentReadService.listCurrentByStableFlipIds(List.of(highId, lowId)))
                .thenReturn(List.of(hydratedHigh, hydratedLow));

        Page<FlipGoodnessDto> result = service.topGoodnessFlips(null, null, 0);

        assertEquals(2, result.getTotalElements());
        assertEquals(highId, result.getContent().get(0).flip().id());
        assertEquals("POWER_SCROLL", result.getContent().get(0).flip().outputItems().getFirst().itemId());
        assertEquals(lowId, result.getContent().get(1).flip().id());
        verify(unifiedFlipCurrentReadService).listCurrentScoringDtos(null);
        verify(unifiedFlipCurrentReadService).listCurrentByStableFlipIds(List.of(highId, lowId));
        verify(unifiedFlipCurrentReadService, never()).listCurrent(null);
        verifyNoInteractions(flipRepository);
    }

    @Test
    void topGoodnessFlipsCurrentStorageCachesRankingPerLatestSnapshot() {
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

        long snapshotEpochMillis = Instant.parse("2026-02-25T15:00:00Z").toEpochMilli();
        UUID onlyId = UUID.fromString("85858585-8585-8585-8585-858585858585");
        UnifiedFlipDto scoring = sampleGoodnessDto(onlyId, 2.0D, 2_000_000L, 70.0D, 20.0D, false);
        UnifiedFlipDto hydrated = new UnifiedFlipDto(
                onlyId,
                FlipType.BAZAAR,
                List.of(new UnifiedFlipDto.ItemStackDto("INK_SACK", 64)),
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_INK_SACK", 1)),
                scoring.requiredCapital(),
                scoring.expectedProfit(),
                scoring.roi(),
                scoring.roiPerHour(),
                scoring.durationSeconds(),
                scoring.fees(),
                scoring.liquidityScore(),
                scoring.riskScore(),
                scoring.snapshotTimestamp(),
                scoring.partial(),
                scoring.partialReasons(),
                List.of(),
                List.of()
        );

        when(unifiedFlipCurrentReadService.latestSnapshotEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        when(unifiedFlipCurrentReadService.listCurrentScoringDtos(FlipType.BAZAAR)).thenReturn(List.of(scoring));
        when(unifiedFlipCurrentReadService.listCurrentByStableFlipIds(List.of(onlyId))).thenReturn(List.of(hydrated));

        Page<FlipGoodnessDto> first = service.topGoodnessFlips(FlipType.BAZAAR, null, 0);
        Page<FlipGoodnessDto> second = service.topGoodnessFlips(FlipType.BAZAAR, null, 0);

        assertEquals(1, first.getTotalElements());
        assertEquals(1, second.getTotalElements());
        verify(unifiedFlipCurrentReadService, times(1)).listCurrentScoringDtos(FlipType.BAZAAR);
        verify(unifiedFlipCurrentReadService, times(2)).listCurrentByStableFlipIds(List.of(onlyId));
    }

    @Test
    void filterFlipsCurrentStorageHydratesPagedRowsOnly() {
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

        UUID highId = UUID.fromString("88888888-8888-8888-8888-888888888888");
        UUID lowId = UUID.fromString("89898989-8989-8989-8989-898989898989");
        UnifiedFlipDto highScore = sampleGoodnessDto(highId, 3.0D, 3_000_000L, 80.0D, 10.0D, false);
        UnifiedFlipDto lowScore = sampleGoodnessDto(lowId, 1.0D, 1_000_000L, 75.0D, 15.0D, false);
        when(unifiedFlipCurrentReadService.listCurrentScoringDtos(null)).thenReturn(List.of(lowScore, highScore));

        UnifiedFlipDto hydratedHigh = new UnifiedFlipDto(
                highId,
                FlipType.BAZAAR,
                List.of(new UnifiedFlipDto.ItemStackDto("RAW_FISH", 16)),
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_RAW_FISH", 1)),
                highScore.requiredCapital(),
                highScore.expectedProfit(),
                highScore.roi(),
                highScore.roiPerHour(),
                highScore.durationSeconds(),
                highScore.fees(),
                highScore.liquidityScore(),
                highScore.riskScore(),
                highScore.snapshotTimestamp(),
                highScore.partial(),
                highScore.partialReasons(),
                List.of(),
                List.of()
        );
        when(unifiedFlipCurrentReadService.listCurrentByStableFlipIds(List.of(highId)))
                .thenReturn(List.of(hydratedHigh));

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
                PageRequest.of(0, 1)
        );

        assertEquals(2, result.getTotalElements());
        assertEquals(1, result.getContent().size());
        assertEquals(highId, result.getContent().getFirst().id());
        assertEquals("ENCHANTED_RAW_FISH", result.getContent().getFirst().outputItems().getFirst().itemId());
        verify(unifiedFlipCurrentReadService).listCurrentScoringDtos(null);
        verify(unifiedFlipCurrentReadService).listCurrentByStableFlipIds(List.of(highId));
        verifyNoInteractions(flipRepository);
    }

    @Test
    void topFlipsCurrentStorageHydratesLimitedRowsOnly() {
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

        UUID firstId = UUID.fromString("8a8a8a8a-8a8a-8a8a-8a8a-8a8a8a8a8a8a");
        UUID secondId = UUID.fromString("8b8b8b8b-8b8b-8b8b-8b8b-8b8b8b8b8b8b");
        UnifiedFlipDto firstScore = sampleGoodnessDto(firstId, 3.0D, 3_000_000L, 80.0D, 10.0D, false);
        UnifiedFlipDto secondScore = sampleGoodnessDto(secondId, 2.0D, 2_000_000L, 70.0D, 15.0D, false);
        when(unifiedFlipCurrentReadService.listCurrentScoringDtos(FlipType.BAZAAR)).thenReturn(List.of(secondScore, firstScore));

        UnifiedFlipDto hydratedFirst = new UnifiedFlipDto(
                firstId,
                FlipType.BAZAAR,
                List.of(new UnifiedFlipDto.ItemStackDto("GOLD_INGOT", 64)),
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_GOLD", 1)),
                firstScore.requiredCapital(),
                firstScore.expectedProfit(),
                firstScore.roi(),
                firstScore.roiPerHour(),
                firstScore.durationSeconds(),
                firstScore.fees(),
                firstScore.liquidityScore(),
                firstScore.riskScore(),
                firstScore.snapshotTimestamp(),
                firstScore.partial(),
                firstScore.partialReasons(),
                List.of(),
                List.of()
        );
        when(unifiedFlipCurrentReadService.listCurrentByStableFlipIds(List.of(firstId)))
                .thenReturn(List.of(hydratedFirst));

        List<UnifiedFlipDto> result = service.topFlips(
                FlipType.BAZAAR,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                1
        );

        assertEquals(1, result.size());
        assertEquals(firstId, result.getFirst().id());
        assertEquals("ENCHANTED_GOLD", result.getFirst().outputItems().getFirst().itemId());
        verify(unifiedFlipCurrentReadService).listCurrentScoringDtos(FlipType.BAZAAR);
        verify(unifiedFlipCurrentReadService).listCurrentByStableFlipIds(List.of(firstId));
        verifyNoInteractions(flipRepository);
    }

    @Test
    void topFlipsCurrentStoragePreservesEntriesWithNullIdDuringHydration() {
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

        UUID stableId = UUID.fromString("0f0f0f0f-1111-2222-3333-444444444444");
        UnifiedFlipDto nullIdScored = sampleGoodnessDto(null, 3.0D, 4_000_000L, 80.0D, 10.0D, false);
        UnifiedFlipDto stableScored = sampleGoodnessDto(stableId, 2.0D, 2_000_000L, 70.0D, 15.0D, false);
        when(unifiedFlipCurrentReadService.listCurrentScoringDtos(FlipType.BAZAAR))
                .thenReturn(List.of(stableScored, nullIdScored));

        UnifiedFlipDto hydratedStable = new UnifiedFlipDto(
                stableId,
                FlipType.BAZAAR,
                List.of(new UnifiedFlipDto.ItemStackDto("COBBLESTONE", 160)),
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_COBBLESTONE", 1)),
                stableScored.requiredCapital(),
                stableScored.expectedProfit(),
                stableScored.roi(),
                stableScored.roiPerHour(),
                stableScored.durationSeconds(),
                stableScored.fees(),
                stableScored.liquidityScore(),
                stableScored.riskScore(),
                stableScored.snapshotTimestamp(),
                stableScored.partial(),
                stableScored.partialReasons(),
                List.of(),
                List.of()
        );
        when(unifiedFlipCurrentReadService.listCurrentByStableFlipIds(List.of(stableId)))
                .thenReturn(List.of(hydratedStable));

        List<UnifiedFlipDto> result = service.topFlips(
                FlipType.BAZAAR,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                2
        );

        assertEquals(2, result.size());
        assertNull(result.get(0).id());
        assertEquals(stableId, result.get(1).id());
        verify(unifiedFlipCurrentReadService).listCurrentByStableFlipIds(List.of(stableId));
        verifyNoInteractions(flipRepository);
    }

    @Test
    void topFlipsLegacyUsesPagedSelectionAndKeepsTopN() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip firstPageLow = mock(Flip.class);
        Flip firstPageHigh = mock(Flip.class);
        Flip secondPageMid = mock(Flip.class);
        UUID lowId = UUID.fromString("8c8c8c8c-8c8c-8c8c-8c8c-8c8c8c8c8c8c");
        UUID highId = UUID.fromString("8d8d8d8d-8d8d-8d8d-8d8d-8d8d8d8d8d8d");
        UUID midId = UUID.fromString("8e8e8e8e-8e8e-8e8e-8e8e-8e8e8e8e8e8e");
        when(firstPageLow.getId()).thenReturn(lowId);
        when(firstPageHigh.getId()).thenReturn(highId);
        when(secondPageMid.getId()).thenReturn(midId);

        Pageable firstPage = PageRequest.of(0, 500);
        Pageable secondPage = PageRequest.of(1, 500);
        when(flipRepository.findAllIds(firstPage)).thenReturn(new PageImpl<>(List.of(lowId, highId), firstPage, 501));
        when(flipRepository.findAllByIdInWithDetails(List.of(lowId, highId))).thenReturn(List.of(firstPageLow, firstPageHigh));
        when(flipRepository.findAllIds(secondPage)).thenReturn(new PageImpl<>(List.of(midId), secondPage, 501));
        when(flipRepository.findAllByIdInWithDetails(List.of(midId))).thenReturn(List.of(secondPageMid));

        when(contextService.loadCurrentContext()).thenReturn(context);
        when(mapper.toDto(firstPageLow, context)).thenReturn(sampleGoodnessDto(
                lowId, 1.0D, 100_000L, 70.0D, 20.0D, false
        ));
        when(mapper.toDto(firstPageHigh, context)).thenReturn(sampleGoodnessDto(
                highId, 1.0D, 900_000L, 70.0D, 20.0D, false
        ));
        when(mapper.toDto(secondPageMid, context)).thenReturn(sampleGoodnessDto(
                midId, 1.0D, 500_000L, 70.0D, 20.0D, false
        ));

        List<UnifiedFlipDto> result = service.topFlips(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                2
        );

        assertEquals(2, result.size());
        assertEquals(highId, result.get(0).id());
        assertEquals(midId, result.get(1).id());
        verify(flipRepository).findAllIds(firstPage);
        verify(flipRepository).findAllIds(secondPage);
        verify(flipRepository, never()).findAll(Pageable.unpaged());
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
        UUID inflatedId = UUID.fromString("60606060-6060-6060-6060-606060606060");
        UUID actionableId = UUID.fromString("70707070-7070-7070-7070-707070707070");
        when(inflatedMissingInput.getId()).thenReturn(inflatedId);
        when(actionable.getId()).thenReturn(actionableId);
        stubLegacyPagedAll(flipRepository, List.of(inflatedId, actionableId), List.of(inflatedMissingInput, actionable));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(inflatedMissingInput, context)).thenReturn(sampleGoodnessDto(
                inflatedId,
                2000.0D, 200_000_000L, 90.0D, 10.0D, true, List.of("MISSING_INPUT_PRICE_AUCTION:METAL_HEART")
        ));
        when(mapper.toDto(actionable, context)).thenReturn(sampleGoodnessDto(
                actionableId,
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
    void filterFlipsExcludesInsufficientInputDepthEntries() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip shallowInputFlip = mock(Flip.class);
        Flip actionable = mock(Flip.class);
        UUID shallowId = UUID.fromString("71717171-7171-7171-7171-717171717171");
        UUID actionableId = UUID.fromString("72727272-7272-7272-7272-727272727272");
        when(shallowInputFlip.getId()).thenReturn(shallowId);
        when(actionable.getId()).thenReturn(actionableId);
        stubLegacyPagedAll(flipRepository, List.of(shallowId, actionableId), List.of(shallowInputFlip, actionable));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(shallowInputFlip, context)).thenReturn(sampleGoodnessDto(
                shallowId,
                25.0D, 25_000_000L, 85.0D, 15.0D, true, List.of("INSUFFICIENT_INPUT_DEPTH:DEPTH_ITEM")
        ));
        when(mapper.toDto(actionable, context)).thenReturn(sampleGoodnessDto(
                actionableId,
                2.0D, 1_500_000L, 70.0D, 25.0D, false, List.of()
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
        assertEquals(UUID.fromString("72727272-7272-7272-7272-727272727272"), result.getContent().getFirst().id());
    }

    @Test
    void topGoodnessFlipsScoringV2DemotesLowConfidenceOutlier() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlippingModelProperties properties = new FlippingModelProperties();
        properties.setScoringV2Enabled(true);
        properties.setRecommendationGatesEnabled(false);
        FlipReadService service = new FlipReadService(
                flipRepository, mapper, contextService, null, null, null, properties
        );
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip lowConfidenceOutlier = mock(Flip.class);
        Flip stable = mock(Flip.class);
        UUID outlierId = UUID.fromString("73737373-7373-7373-7373-737373737373");
        UUID stableId = UUID.fromString("74747474-7474-7474-7474-747474747474");
        when(lowConfidenceOutlier.getId()).thenReturn(outlierId);
        when(stable.getId()).thenReturn(stableId);
        stubLegacyPagedAll(flipRepository, List.of(outlierId, stableId), List.of(lowConfidenceOutlier, stable));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(lowConfidenceOutlier, context)).thenReturn(sampleGoodnessDto(
                outlierId,
                60.0D, 8_000_000L, 30.0D, 50.0D, true, List.of("INSUFFICIENT_OUTPUT_DEPTH:ITEM")
        ));
        when(mapper.toDto(stable, context)).thenReturn(sampleGoodnessDto(
                stableId,
                0.5D, 1_200_000L, 80.0D, 20.0D, false, List.of()
        ));

        Page<FlipGoodnessDto> result = service.topGoodnessFlips(null, null, 0);

        assertEquals(2, result.getTotalElements());
        assertEquals(UUID.fromString("74747474-7474-7474-7474-747474747474"), result.getContent().getFirst().flip().id());
    }

    @Test
    void recommendationGatesExcludeLowConfidenceFlips() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlippingModelProperties properties = new FlippingModelProperties();
        properties.setRecommendationGatesEnabled(true);
        properties.setMinRecommendationExpectedProfit(500_000L);
        properties.setMinRecommendationLiquidityScore(50D);
        properties.setMinConfidenceScore(70D);
        FlipReadService service = new FlipReadService(
                flipRepository, mapper, contextService, null, null, null, properties
        );
        FlipCalculationContext context = FlipCalculationContext.standard(null);

        Flip lowConfidence = mock(Flip.class);
        Flip good = mock(Flip.class);
        UUID lowConfidenceId = UUID.fromString("75757575-7575-7575-7575-757575757575");
        UUID goodId = UUID.fromString("76767676-7676-7676-7676-767676767676");
        when(lowConfidence.getId()).thenReturn(lowConfidenceId);
        when(good.getId()).thenReturn(goodId);
        stubLegacyPagedAll(flipRepository, List.of(lowConfidenceId, goodId), List.of(lowConfidence, good));
        when(contextService.loadCurrentContext()).thenReturn(context);

        when(mapper.toDto(lowConfidence, context)).thenReturn(sampleGoodnessDto(
                lowConfidenceId,
                2.0D, 2_000_000L, 70.0D, 25.0D, true, List.of("INSUFFICIENT_OUTPUT_DEPTH:ITEM")
        ));
        when(mapper.toDto(good, context)).thenReturn(sampleGoodnessDto(
                goodId,
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
        assertEquals(UUID.fromString("76767676-7676-7676-7676-767676767676"), result.getContent().getFirst().id());
    }

    @Test
    void summaryStatsCurrentStorageUsesScoringDtosWithoutDefinitionHydration() {
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

        UnifiedFlipDto first = sampleGoodnessDto(
                UUID.fromString("86868686-8686-8686-8686-868686868686"),
                1.0D,
                2_000_000L,
                70.0D,
                20.0D,
                false
        );
        UnifiedFlipDto second = sampleGoodnessDto(
                UUID.fromString("87878787-8787-8787-8787-878787878787"),
                0.5D,
                500_000L,
                60.0D,
                25.0D,
                false
        );
        when(unifiedFlipCurrentReadService.listCurrentScoringDtos(FlipType.BAZAAR)).thenReturn(List.of(first, second));

        EnumMap<FlipType, Long> counts = new EnumMap<>(FlipType.class);
        for (FlipType type : FlipType.values()) {
            counts.put(type, 0L);
        }
        counts.put(FlipType.BAZAAR, 2L);
        when(unifiedFlipCurrentReadService.countsByType()).thenReturn(counts);

        FlipSummaryStatsDto result = service.summaryStats(FlipType.BAZAAR, null);

        assertEquals(2L, result.totalActiveFlips());
        assertEquals(1_250_000L, result.avgProfit());
        assertEquals(0.5D, result.avgRoi());
        assertEquals(2_000_000L, result.bestFlipProfit());
        assertEquals(1, result.byType().size());
        assertEquals(2L, result.byType().get(FlipType.BAZAAR.name()));
        verify(unifiedFlipCurrentReadService).listCurrentScoringDtos(FlipType.BAZAAR);
        verify(unifiedFlipCurrentReadService, never()).listCurrent(FlipType.BAZAAR);
        verifyNoInteractions(flipRepository);
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
    void summaryStatsLegacyUsesGroupedCountQueryForByType() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper, contextService);

        long snapshotEpochMillis = Instant.parse("2026-02-20T22:00:00Z").toEpochMilli();
        FlipCalculationContext context = FlipCalculationContext.standard(null);
        Flip first = mock(Flip.class);
        Flip second = mock(Flip.class);
        UUID firstId = UUID.fromString("81818181-8181-8181-8181-818181818181");
        UUID secondId = UUID.fromString("82828282-8282-8282-8282-828282828282");
        when(first.getId()).thenReturn(firstId);
        when(second.getId()).thenReturn(secondId);

        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        stubLegacyPagedBySnapshot(flipRepository, snapshotEpochMillis, List.of(firstId, secondId), List.of(first, second));
        when(flipRepository.countByFlipTypeForSnapshot(snapshotEpochMillis)).thenReturn(List.of(
                new Object[]{FlipType.AUCTION, 5L},
                new Object[]{FlipType.BAZAAR, 3L}
        ));
        when(contextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis))).thenReturn(context);
        when(mapper.toDto(first, context)).thenReturn(sampleGoodnessDto(
                firstId,
                1.0D,
                1_000_000L,
                60.0D,
                20.0D,
                false
        ));
        when(mapper.toDto(second, context)).thenReturn(sampleGoodnessDto(
                secondId,
                0.5D,
                500_000L,
                55.0D,
                22.0D,
                false
        ));

        FlipSummaryStatsDto result = service.summaryStats(null, null);

        assertEquals(2L, result.totalActiveFlips());
        assertEquals(750_000L, result.avgProfit());
        assertEquals(0.5D, result.avgRoi());
        assertEquals(1_000_000L, result.bestFlipProfit());
        assertEquals(FlipType.values().length, result.byType().size());
        assertEquals(5L, result.byType().get(FlipType.AUCTION.name()));
        assertEquals(3L, result.byType().get(FlipType.BAZAAR.name()));
        verify(flipRepository, times(1)).countByFlipTypeForSnapshot(snapshotEpochMillis);
        verify(flipRepository, never()).findAllByFlipType(
                org.mockito.ArgumentMatchers.any(FlipType.class),
                org.mockito.ArgumentMatchers.eq(Pageable.unpaged())
        );
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

    private void stubLegacyPagedAll(FlipRepository flipRepository, List<UUID> ids, List<Flip> flips) {
        PageRequest firstPage = PageRequest.of(0, 500);
        when(flipRepository.findAllIds(firstPage)).thenReturn(new PageImpl<>(ids, firstPage, ids.size()));
        when(flipRepository.findAllByIdInWithDetails(ids)).thenReturn(flips);
    }

    private void stubLegacyPagedByType(FlipRepository flipRepository,
                                       FlipType flipType,
                                       List<UUID> ids,
                                       List<Flip> flips) {
        PageRequest firstPage = PageRequest.of(0, 500);
        when(flipRepository.findIdsByFlipType(flipType, firstPage)).thenReturn(new PageImpl<>(ids, firstPage, ids.size()));
        when(flipRepository.findAllByIdInWithDetails(ids)).thenReturn(flips);
    }

    private void stubLegacyPagedBySnapshot(FlipRepository flipRepository,
                                           long snapshotEpochMillis,
                                           List<UUID> ids,
                                           List<Flip> flips) {
        PageRequest firstPage = PageRequest.of(0, 500);
        when(flipRepository.findIdsBySnapshotTimestampEpochMillis(snapshotEpochMillis, firstPage))
                .thenReturn(new PageImpl<>(ids, firstPage, ids.size()));
        when(flipRepository.findAllByIdInWithDetails(ids)).thenReturn(flips);
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
