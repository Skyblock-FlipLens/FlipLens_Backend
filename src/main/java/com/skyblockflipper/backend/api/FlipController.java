package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
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
            @PageableDefault(size = 50, sort = "id") Pageable pageable
    ) {
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
            @PageableDefault(size = 50, sort = "id") Pageable pageable
    ) {
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
            @PageableDefault(size = 50, sort = "id") Pageable pageable
    ) {
        return flipReadService.topLiquidityFlips(flipType, snapshotTimestamp, pageable);
    }

    @GetMapping("/top/low-risk")
    public Page<UnifiedFlipDto> lowestRiskFlips(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @PageableDefault(size = 50, sort = "id") Pageable pageable
    ) {
        return flipReadService.lowestRiskFlips(flipType, snapshotTimestamp, pageable);
    }

    @GetMapping("/top/best")
    public Page<FlipGoodnessDto> topGoodnessFlips(
            @RequestParam(required = false) FlipType flipType,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant snapshotTimestamp,
            @RequestParam(defaultValue = "0") int page
    ) {
        return flipReadService.topGoodnessFlips(flipType, snapshotTimestamp, page);
    }

    @GetMapping("/{id}")
    public ResponseEntity<UnifiedFlipDto> getFlipById(@PathVariable UUID id) {
        return flipReadService.findFlipById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
