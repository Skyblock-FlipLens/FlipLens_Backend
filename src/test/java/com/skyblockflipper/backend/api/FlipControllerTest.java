package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlipControllerTest {

    @Test
    void listFlipsDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        Pageable pageable = RangePagination.pageable(0, 49, 50, Sort.by("id").ascending());
        UnifiedFlipDto dto = sampleDto();
        Page<UnifiedFlipDto> expected = new PageImpl<>(List.of(dto), pageable, 1);

        when(service.listFlips(FlipType.FORGE, null, pageable)).thenReturn(expected);

        Page<UnifiedFlipDto> response = controller.listFlips(FlipType.FORGE, null, 0, 49);

        assertEquals(expected, response);
        verify(service).listFlips(FlipType.FORGE, null, pageable);
    }

    @Test
    void listFlipsWithSnapshotDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        Pageable pageable = RangePagination.pageable(0, 49, 50, Sort.by("id").ascending());
        Instant snapshotTimestamp = Instant.parse("2026-02-18T21:00:00Z");
        UnifiedFlipDto dto = sampleDto();
        Page<UnifiedFlipDto> expected = new PageImpl<>(List.of(dto), pageable, 1);

        when(service.listFlips(FlipType.FORGE, snapshotTimestamp, pageable)).thenReturn(expected);

        Page<UnifiedFlipDto> response = controller.listFlips(FlipType.FORGE, snapshotTimestamp, 0, 49);

        assertEquals(expected, response);
        verify(service).listFlips(FlipType.FORGE, snapshotTimestamp, pageable);
    }

    @Test
    void filterFlipsDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        Pageable pageable = RangePagination.pageable(
                10,
                59,
                50,
                Sort.by(Sort.Direction.DESC, FlipSortBy.LIQUIDITY_SCORE.toFieldName())
        );
        Instant snapshotTimestamp = Instant.parse("2026-02-18T21:00:00Z");
        Page<UnifiedFlipDto> expected = new PageImpl<>(List.of(sampleDto()), pageable, 1);

        when(service.filterFlips(
                FlipType.BAZAAR,
                snapshotTimestamp,
                60.0D,
                30.0D,
                1_000_000L,
                0.5D,
                2.0D,
                5_000_000L,
                false,
                FlipSortBy.LIQUIDITY_SCORE,
                Sort.Direction.DESC,
                pageable
        )).thenReturn(expected);

        Page<UnifiedFlipDto> response = controller.filterFlips(
                FlipType.BAZAAR,
                snapshotTimestamp,
                60.0D,
                30.0D,
                1_000_000L,
                0.5D,
                2.0D,
                5_000_000L,
                false,
                FlipSortBy.LIQUIDITY_SCORE,
                Sort.Direction.DESC,
                10,
                59
        );

        assertEquals(expected, response);
        verify(service).filterFlips(
                FlipType.BAZAAR,
                snapshotTimestamp,
                60.0D,
                30.0D,
                1_000_000L,
                0.5D,
                2.0D,
                5_000_000L,
                false,
                FlipSortBy.LIQUIDITY_SCORE,
                Sort.Direction.DESC,
                pageable
        );
    }

    @Test
    void topLiquidityFlipsDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        Pageable pageable = RangePagination.pageable(0, 49, 50, Sort.unsorted());
        Instant snapshotTimestamp = Instant.parse("2026-02-18T21:00:00Z");
        Page<UnifiedFlipDto> expected = new PageImpl<>(List.of(sampleDto()), pageable, 1);

        when(service.topLiquidityFlips(FlipType.AUCTION, snapshotTimestamp, pageable)).thenReturn(expected);

        Page<UnifiedFlipDto> response = controller.topLiquidityFlips(FlipType.AUCTION, snapshotTimestamp, 0, 49);

        assertEquals(expected, response);
        verify(service).topLiquidityFlips(FlipType.AUCTION, snapshotTimestamp, pageable);
    }

    @Test
    void lowestRiskFlipsDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        Pageable pageable = RangePagination.pageable(0, 49, 50, Sort.by("id").ascending());
        Instant snapshotTimestamp = Instant.parse("2026-02-18T21:00:00Z");
        Page<UnifiedFlipDto> expected = new PageImpl<>(List.of(sampleDto()), pageable, 1);

        when(service.lowestRiskFlips(FlipType.AUCTION, snapshotTimestamp, pageable)).thenReturn(expected);

        Page<UnifiedFlipDto> response = controller.lowestRiskFlips(FlipType.AUCTION, snapshotTimestamp, 0, 49);

        assertEquals(expected, response);
        verify(service).lowestRiskFlips(FlipType.AUCTION, snapshotTimestamp, pageable);
    }

    @Test
    void topGoodnessFlipsDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        Instant snapshotTimestamp = Instant.parse("2026-02-18T21:00:00Z");
        Pageable pageable = RangePagination.pageable(20, 29, 10, Sort.by("id").ascending());
        Page<FlipGoodnessDto> expected = new PageImpl<>(List.of(
                new FlipGoodnessDto(sampleDto(), 87.3D,
                        new FlipGoodnessDto.GoodnessBreakdown(90D, 70D, 80D, 85D, false))
        ));

        when(service.topGoodnessFlips(FlipType.BAZAAR, snapshotTimestamp, pageable)).thenReturn(expected);

        Page<FlipGoodnessDto> response = controller.topGoodnessFlips(FlipType.BAZAAR, snapshotTimestamp, 20, 29);

        assertEquals(expected, response);
        verify(service).topGoodnessFlips(FlipType.BAZAAR, snapshotTimestamp, pageable);
    }

    @Test
    void flipTypeCoverageDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        FlipCoverageDto expected = new FlipCoverageDto(
                Instant.parse("2026-02-19T20:00:00Z"),
                List.of("SHARD", "FUSION"),
                List.of(
                        new FlipCoverageDto.FlipTypeCoverageDto(
                                FlipType.AUCTION,
                                FlipCoverageDto.CoverageStatus.SUPPORTED,
                                FlipCoverageDto.CoverageStatus.SUPPORTED,
                                FlipCoverageDto.CoverageStatus.SUPPORTED,
                                FlipCoverageDto.CoverageStatus.SUPPORTED,
                                2L,
                                "Generated from Hypixel market snapshots via MarketFlipMapper."
                        )
                )
        );

        when(service.flipTypeCoverage()).thenReturn(expected);

        FlipCoverageDto response = controller.flipTypeCoverage();

        assertEquals(expected, response);
        verify(service).flipTypeCoverage();
    }

    @Test
    void listFlipTypesDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        FlipTypesDto expected = new FlipTypesDto(List.of(FlipType.AUCTION, FlipType.BAZAAR));

        when(service.listSupportedFlipTypes()).thenReturn(expected);

        FlipTypesDto response = controller.listFlipTypes();

        assertEquals(expected, response);
        verify(service).listSupportedFlipTypes();
    }

    @Test
    void snapshotStatsDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        Instant snapshotTimestamp = Instant.parse("2026-02-18T21:00:00Z");
        FlipSnapshotStatsDto expected = new FlipSnapshotStatsDto(
                snapshotTimestamp,
                10L,
                List.of(new FlipSnapshotStatsDto.FlipTypeCountDto(FlipType.AUCTION, 4L))
        );

        when(service.snapshotStats(snapshotTimestamp)).thenReturn(expected);

        FlipSnapshotStatsDto response = controller.snapshotStats(snapshotTimestamp);

        assertEquals(expected, response);
        verify(service).snapshotStats(snapshotTimestamp);
    }

    @Test
    void getFlipByIdReturnsOkWhenFound() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        UUID id = UUID.randomUUID();
        UnifiedFlipDto dto = sampleDto();

        when(service.findFlipById(id)).thenReturn(Optional.of(dto));

        ResponseEntity<UnifiedFlipDto> response = controller.getFlipById(id);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(dto, response.getBody());
        verify(service).findFlipById(id);
    }

    @Test
    void getFlipByIdReturns404WhenMissing() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        UUID id = UUID.randomUUID();

        when(service.findFlipById(id)).thenReturn(Optional.empty());

        ResponseEntity<UnifiedFlipDto> response = controller.getFlipById(id);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        verify(service).findFlipById(id);
    }

    private UnifiedFlipDto sampleDto() {
        return new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.FORGE,
                List.of(),
                List.of(),
                null,
                null,
                null,
                null,
                120L,
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
