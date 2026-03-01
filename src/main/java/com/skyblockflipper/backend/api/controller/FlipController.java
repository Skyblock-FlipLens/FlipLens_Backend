package com.skyblockflipper.backend.api.controller;

import com.skyblockflipper.backend.api.dto.*;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/flips")
@RequiredArgsConstructor
public class FlipController {

    private static final int TOP_BEST_FEATURED_COUNT = 6;
    private static final int TOP_BEST_PAGED_START_OFFSET = 6;
    private static final int TOP_BEST_PAGE_SIZE = 20;

    private final FlipReadService flipReadService;

    @GetMapping("/coverage")
    public FlipCoverageDto flipTypeCoverage() {
        return flipReadService.flipTypeCoverage();
    }

    @GetMapping("/types")
    public FlipTypesDto listFlipTypes() {
        return flipReadService.listSupportedFlipTypes();
    }

    @GetMapping("/stats")
    public Object stats(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @RequestParam(required = false, defaultValue = "false") boolean legacySnapshot
    ) {
        if (legacySnapshot || (snapshotTimestamp != null && flipType == null)) {
            return flipReadService.snapshotStats(snapshotTimestamp);
        }
        return flipReadService.summaryStats(flipType, snapshotTimestamp);
    }

    @GetMapping("/stats/snapshot")
    public FlipSnapshotStatsDto snapshotStats(
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp
    ) {
        return flipReadService.snapshotStats(snapshotTimestamp);
    }

    @GetMapping
    public Page<UnifiedFlipDto> listFlips(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size
    ) {
        Pageable pageable = StandardPagination.pageable(page, size, 50, Sort.by("id").ascending());
        return flipReadService.listFlips(flipType, snapshotTimestamp, pageable);
    }

    @GetMapping("/filter")
    public Page<UnifiedFlipDto> filterFlips(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @RequestParam(required = false) Double minLiquidityScore,
            @RequestParam(required = false) Double maxRiskScore,
            @RequestParam(required = false) Long minExpectedProfit,
            @RequestParam(required = false) Double minRoi,
            @RequestParam(required = false) Double minRoiPerHour,
            @RequestParam(required = false) Long maxRequiredCapital,
            @RequestParam(required = false) Boolean partial,
            @RequestParam(defaultValue = "EXPECTED_PROFIT") FlipSortBy sortBy,
            @RequestParam(defaultValue = "DESC") Sort.Direction sortDirection,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size
    ) {
        FlipSortBy effectiveSortBy = sortBy == null ? FlipSortBy.EXPECTED_PROFIT : sortBy;
        Sort.Direction effectiveSortDirection = sortDirection == null ? Sort.Direction.DESC : sortDirection;
        Sort requestedSort = Sort.by(effectiveSortDirection, effectiveSortBy.toFieldName());
        Pageable pageable = StandardPagination.pageable(page, size, 50, requestedSort);
        return flipReadService.filterFlips(
                flipType,
                snapshotTimestamp,
                minLiquidityScore,
                maxRiskScore,
                minExpectedProfit,
                minRoi,
                minRoiPerHour,
                maxRequiredCapital,
                partial,
                sortBy,
                sortDirection,
                pageable
        );
    }

    @GetMapping("/top")
    public java.util.List<UnifiedFlipDto> topFlips(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @RequestParam(required = false) Double minLiquidityScore,
            @RequestParam(required = false) Double maxRiskScore,
            @RequestParam(required = false) Long minExpectedProfit,
            @RequestParam(required = false) Double minRoi,
            @RequestParam(required = false) Double minRoiPerHour,
            @RequestParam(required = false) Long maxRequiredCapital,
            @RequestParam(required = false) Boolean partial,
            @RequestParam(defaultValue = "6") int limit
    ) {
        return flipReadService.topFlips(
                flipType,
                snapshotTimestamp,
                minLiquidityScore,
                maxRiskScore,
                minExpectedProfit,
                minRoi,
                minRoiPerHour,
                maxRequiredCapital,
                partial,
                limit
        );
    }

    @GetMapping("/top/liquidity")
    public Page<UnifiedFlipDto> topLiquidityFlips(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size
    ) {
        // Sorting is enforced in FlipReadService.topLiquidityFlips/filterFlips.
        Pageable pageable = StandardPagination.pageable(page, size, 50, Sort.unsorted());
        return flipReadService.topLiquidityFlips(flipType, snapshotTimestamp, pageable);
    }

    @GetMapping("/top/low-risk")
    public Page<UnifiedFlipDto> lowestRiskFlips(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size
    ) {
        Pageable pageable = StandardPagination.pageable(page, size, 50, Sort.by("id").ascending());
        return flipReadService.lowestRiskFlips(flipType, snapshotTimestamp, pageable);
    }

    @GetMapping("/top/best")
    public Page<FlipGoodnessDto> topGoodnessFlips(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @RequestParam(name = "getbypage", required = false) Integer getbypage
    ) {
        Pageable pageable;
        if (getbypage != null) {
            int safePage = Math.max(0, getbypage);
            long offset = TOP_BEST_PAGED_START_OFFSET + (long) safePage * TOP_BEST_PAGE_SIZE;
            pageable = OffsetLimitPageRequest.of(offset, TOP_BEST_PAGE_SIZE, Sort.by("id").ascending());
        } else {
            pageable = OffsetLimitPageRequest.of(0L, TOP_BEST_FEATURED_COUNT, Sort.by("id").ascending());
        }
        return flipReadService.topGoodnessFlips(flipType, snapshotTimestamp, pageable);
    }

    @GetMapping("/{id}")
    public ResponseEntity<UnifiedFlipDto> getFlipById(@PathVariable UUID id) {
        return flipReadService.findFlipById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}


