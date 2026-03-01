package com.skyblockflipper.backend.api.controller;

import com.skyblockflipper.backend.api.dto.*;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(FlipController.class)
class FlipControllerWebMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private FlipReadService flipReadService;

    @Test
    void listFlipsReturnsPageAndInvokesServiceWithExpectedPageableAndFlipType() throws Exception {
        UUID flipId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        UnifiedFlipDto dto = new UnifiedFlipDto(
                flipId,
                FlipType.FORGE,
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_DIAMOND", 2)),
                List.of(new UnifiedFlipDto.ItemStackDto("REFINED_DIAMOND", 1)),
                150_000L,
                null,
                null,
                null,
                3_600L,
                null,
                null,
                null,
                Instant.parse("2026-02-16T00:00:00Z"),
                false,
                List.of(),
                List.of(),
                List.of()
        );

        Pageable expectedRequest = StandardPagination.pageable(0, 2, 50, Sort.by("id").ascending());
        Page<UnifiedFlipDto> resultPage = new PageImpl<>(List.of(dto), expectedRequest, 3);
        when(flipReadService.listFlips(eq(FlipType.FORGE), any(), any(Pageable.class))).thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips")
                        .param("flipType", "FORGE")
                        .param("page", "0")
                        .param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].id").value(flipId.toString()))
                .andExpect(jsonPath("$.content[0].flipType").value("FORGE"))
                .andExpect(jsonPath("$.content[0].inputItems[0].itemId").value("ENCHANTED_DIAMOND"))
                .andExpect(jsonPath("$.content[0].outputItems[0].itemId").value("REFINED_DIAMOND"))
                .andExpect(jsonPath("$.number").value(0))
                .andExpect(jsonPath("$.size").value(2))
                .andExpect(jsonPath("$.totalElements").value(3))
                .andExpect(jsonPath("$.totalPages").value(2));

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        ArgumentCaptor<FlipType> flipTypeCaptor = ArgumentCaptor.forClass(FlipType.class);
        ArgumentCaptor<Instant> snapshotTimestampCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(flipReadService, times(1))
                .listFlips(flipTypeCaptor.capture(), snapshotTimestampCaptor.capture(), pageableCaptor.capture());

        assertEquals(FlipType.FORGE, flipTypeCaptor.getValue());
        assertNull(snapshotTimestampCaptor.getValue());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(0, pageable.getOffset());
        assertEquals(2, pageable.getPageSize());
        assertEquals(Sort.by("id").ascending(), pageable.getSort());
    }

    @Test
    void listFlipTypesReturnsEnumList() throws Exception {
        when(flipReadService.listSupportedFlipTypes())
                .thenReturn(new FlipTypesDto(List.of(FlipType.AUCTION, FlipType.BAZAAR, FlipType.CRAFTING)));

        mockMvc.perform(get("/api/v1/flips/types"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.flipTypes.length()").value(3))
                .andExpect(jsonPath("$.flipTypes[0]").value("AUCTION"))
                .andExpect(jsonPath("$.flipTypes[1]").value("BAZAAR"))
                .andExpect(jsonPath("$.flipTypes[2]").value("CRAFTING"));

        verify(flipReadService, times(1)).listSupportedFlipTypes();
    }

    @Test
    void snapshotStatsReturnsCountsForRequestedSnapshot() throws Exception {
        Instant snapshotTimestamp = Instant.parse("2026-02-19T20:00:00Z");
        when(flipReadService.snapshotStats(snapshotTimestamp))
                .thenReturn(new FlipSnapshotStatsDto(
                        snapshotTimestamp,
                        5L,
                        List.of(
                                new FlipSnapshotStatsDto.FlipTypeCountDto(FlipType.AUCTION, 2L),
                                new FlipSnapshotStatsDto.FlipTypeCountDto(FlipType.BAZAAR, 3L)
                        )
                ));

        mockMvc.perform(get("/api/v1/flips/stats")
                        .param("snapshotTimestamp", "2026-02-19T20:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.snapshotTimestamp").value("2026-02-19T20:00:00Z"))
                .andExpect(jsonPath("$.totalFlips").value(5))
                .andExpect(jsonPath("$.byType.length()").value(2))
                .andExpect(jsonPath("$.byType[0].flipType").value("AUCTION"))
                .andExpect(jsonPath("$.byType[0].count").value(2))
                .andExpect(jsonPath("$.byType[1].flipType").value("BAZAAR"))
                .andExpect(jsonPath("$.byType[1].count").value(3));

        verify(flipReadService, times(1)).snapshotStats(snapshotTimestamp);
    }

    @Test
    void flipTypeCoverageReturnsMatrix() throws Exception {
        Instant snapshotTimestamp = Instant.parse("2026-02-19T20:00:00Z");
        when(flipReadService.flipTypeCoverage())
                .thenReturn(new FlipCoverageDto(
                        snapshotTimestamp,
                        List.of("SHARD", "FUSION"),
                        List.of(
                                new FlipCoverageDto.FlipTypeCoverageDto(
                                        FlipType.AUCTION,
                                        FlipCoverageDto.CoverageStatus.SUPPORTED,
                                        FlipCoverageDto.CoverageStatus.SUPPORTED,
                                        FlipCoverageDto.CoverageStatus.SUPPORTED,
                                        FlipCoverageDto.CoverageStatus.SUPPORTED,
                                        7L,
                                        "Generated from Hypixel market snapshots via MarketFlipMapper."
                                ),
                                new FlipCoverageDto.FlipTypeCoverageDto(
                                        FlipType.CRAFTING,
                                        FlipCoverageDto.CoverageStatus.SUPPORTED,
                                        FlipCoverageDto.CoverageStatus.SUPPORTED,
                                        FlipCoverageDto.CoverageStatus.SUPPORTED,
                                        FlipCoverageDto.CoverageStatus.SUPPORTED,
                                        11L,
                                        "Generated from NEU recipes via RecipeToFlipMapper."
                                )
                        )
                ));

        mockMvc.perform(get("/api/v1/flips/coverage"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.snapshotTimestamp").value("2026-02-19T20:00:00Z"))
                .andExpect(jsonPath("$.excludedFlipTypes.length()").value(2))
                .andExpect(jsonPath("$.excludedFlipTypes[0]").value("SHARD"))
                .andExpect(jsonPath("$.excludedFlipTypes[1]").value("FUSION"))
                .andExpect(jsonPath("$.flipTypes.length()").value(2))
                .andExpect(jsonPath("$.flipTypes[0].flipType").value("AUCTION"))
                .andExpect(jsonPath("$.flipTypes[0].ingestion").value("SUPPORTED"))
                .andExpect(jsonPath("$.flipTypes[0].latestSnapshotCount").value(7))
                .andExpect(jsonPath("$.flipTypes[1].flipType").value("CRAFTING"))
                .andExpect(jsonPath("$.flipTypes[1].latestSnapshotCount").value(11));

        verify(flipReadService, times(1)).flipTypeCoverage();
    }

    @Test
    void filterFlipsSupportsLiquidityAndRiskParams() throws Exception {
        UUID flipId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        UnifiedFlipDto dto = new UnifiedFlipDto(
                flipId,
                FlipType.BAZAAR,
                List.of(),
                List.of(),
                500_000L,
                250_000L,
                0.5D,
                3.0D,
                1_200L,
                3_000L,
                89.5D,
                12.0D,
                Instant.parse("2026-02-19T20:00:00Z"),
                false,
                List.of(),
                List.of(),
                List.of()
        );

        Pageable expectedRequest = StandardPagination.pageable(0, 5, 50, Sort.by("id").ascending());
        Page<UnifiedFlipDto> resultPage = new PageImpl<>(List.of(dto), expectedRequest, 1);
        when(flipReadService.filterFlips(
                eq(FlipType.BAZAAR),
                eq(Instant.parse("2026-02-19T20:00:00Z")),
                eq(80.0D),
                eq(20.0D),
                eq(100_000L),
                eq(0.2D),
                eq(1.0D),
                eq(1_000_000L),
                eq(false),
                eq(FlipSortBy.LIQUIDITY_SCORE),
                eq(Sort.Direction.DESC),
                any(Pageable.class)
        )).thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips/filter")
                        .param("flipType", "BAZAAR")
                        .param("snapshotTimestamp", "2026-02-19T20:00:00Z")
                        .param("minLiquidityScore", "80.0")
                        .param("maxRiskScore", "20.0")
                        .param("minExpectedProfit", "100000")
                        .param("minRoi", "0.2")
                        .param("minRoiPerHour", "1.0")
                        .param("maxRequiredCapital", "1000000")
                        .param("partial", "false")
                        .param("sortBy", "LIQUIDITY_SCORE")
                        .param("sortDirection", "DESC")
                        .param("page", "0")
                        .param("size", "5"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].id").value(flipId.toString()))
                .andExpect(jsonPath("$.content[0].liquidityScore").value(89.5))
                .andExpect(jsonPath("$.content[0].riskScore").value(12.0));

        verify(flipReadService, times(1)).filterFlips(
                eq(FlipType.BAZAAR),
                eq(Instant.parse("2026-02-19T20:00:00Z")),
                eq(80.0D),
                eq(20.0D),
                eq(100_000L),
                eq(0.2D),
                eq(1.0D),
                eq(1_000_000L),
                eq(false),
                eq(FlipSortBy.LIQUIDITY_SCORE),
                eq(Sort.Direction.DESC),
                any(Pageable.class)
        );
    }

    @Test
    void topLiquidityEndpointDelegatesToService() throws Exception {
        Page<UnifiedFlipDto> resultPage = new PageImpl<>(List.of(), StandardPagination.pageable(0, 10, 50, Sort.by("id").ascending()), 0);
        when(flipReadService.topLiquidityFlips(eq(FlipType.AUCTION), any(), any(Pageable.class))).thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips/top/liquidity")
                        .param("flipType", "AUCTION")
                        .param("page", "0")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.content.length()").value(0));

        verify(flipReadService, times(1)).topLiquidityFlips(eq(FlipType.AUCTION), any(), any(Pageable.class));
    }

    @Test
    void lowRiskEndpointDelegatesToService() throws Exception {
        Page<UnifiedFlipDto> resultPage = new PageImpl<>(List.of(), StandardPagination.pageable(0, 10, 50, Sort.by("id").ascending()), 0);
        when(flipReadService.lowestRiskFlips(eq(FlipType.BAZAAR), any(), any(Pageable.class))).thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips/top/low-risk")
                        .param("flipType", "BAZAAR")
                        .param("page", "0")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.content.length()").value(0));

        verify(flipReadService, times(1)).lowestRiskFlips(eq(FlipType.BAZAAR), any(), any(Pageable.class));
    }

    @Test
    void topGoodnessEndpointReturnsRankedFlips() throws Exception {
        UnifiedFlipDto dto = new UnifiedFlipDto(
                UUID.fromString("33333333-3333-3333-3333-333333333333"),
                FlipType.BAZAAR,
                List.of(),
                List.of(),
                1_000_000L,
                250_000L,
                0.25D,
                1.5D,
                3_600L,
                10_000L,
                88.0D,
                12.0D,
                Instant.parse("2026-02-19T20:00:00Z"),
                false,
                List.of(),
                List.of(),
                List.of()
        );
        FlipGoodnessDto ranked = new FlipGoodnessDto(
                dto,
                84.42D,
                new FlipGoodnessDto.GoodnessBreakdown(95.0D, 60.0D, 88.0D, 88.0D, false)
        );
        Pageable expectedRequest = OffsetLimitPageRequest.of(26L, 20, Sort.by("id").ascending());
        Page<FlipGoodnessDto> resultPage = new PageImpl<>(List.of(ranked), expectedRequest, 11);
        when(flipReadService.topGoodnessFlips(eq(FlipType.BAZAAR), eq(Instant.parse("2026-02-19T20:00:00Z")), any(Pageable.class)))
                .thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips/top/best")
                        .param("flipType", "BAZAAR")
                        .param("snapshotTimestamp", "2026-02-19T20:00:00Z")
                        .param("getbypage", "1"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].goodnessScore").value(84.42))
                .andExpect(jsonPath("$.content[0].flip.id").value("33333333-3333-3333-3333-333333333333"))
                .andExpect(jsonPath("$.content[0].flip.flipType").value("BAZAAR"))
                .andExpect(jsonPath("$.content[0].breakdown.partialPenaltyApplied").value(false))
                .andExpect(jsonPath("$.size").value(20))
                .andExpect(jsonPath("$.number").value(1));

        verify(flipReadService, times(1))
                .topGoodnessFlips(eq(FlipType.BAZAAR), eq(Instant.parse("2026-02-19T20:00:00Z")), any(Pageable.class));
    }

    @Test
    void topGoodnessEndpointWithoutPageReturnsTopSix() throws Exception {
        Page<FlipGoodnessDto> resultPage = new PageImpl<>(List.of());
        when(flipReadService.topGoodnessFlips(eq(FlipType.BAZAAR), any(), any(Pageable.class))).thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips/top/best")
                        .param("flipType", "BAZAAR"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON));

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(flipReadService).topGoodnessFlips(eq(FlipType.BAZAAR), any(), pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(0L, pageable.getOffset());
        assertEquals(6, pageable.getPageSize());
    }

    @Test
    void topGoodnessEndpointGetByPageOneUsesTwentySixToFortyFiveWindow() throws Exception {
        Page<FlipGoodnessDto> resultPage = new PageImpl<>(List.of());
        when(flipReadService.topGoodnessFlips(eq(FlipType.BAZAAR), any(), any(Pageable.class))).thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips/top/best")
                        .param("flipType", "BAZAAR")
                        .param("getbypage", "1"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON));

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(flipReadService, times(1)).topGoodnessFlips(eq(FlipType.BAZAAR), any(), pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(26L, pageable.getOffset());
        assertEquals(20, pageable.getPageSize());
    }

    @Test
    void topGoodnessEndpointGetByPageZeroUsesSixToTwentyFiveWindow() throws Exception {
        Page<FlipGoodnessDto> resultPage = new PageImpl<>(List.of());
        when(flipReadService.topGoodnessFlips(eq(FlipType.BAZAAR), any(), any(Pageable.class))).thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips/top/best")
                        .param("flipType", "BAZAAR")
                        .param("getbypage", "0"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON));

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(flipReadService, times(1)).topGoodnessFlips(eq(FlipType.BAZAAR), any(), pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(6L, pageable.getOffset());
        assertEquals(20, pageable.getPageSize());
    }
}


