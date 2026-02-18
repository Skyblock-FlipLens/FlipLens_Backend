package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import com.skyblockflipper.backend.service.market.MarketSnapshotReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/api/v1/snapshots")
@RequiredArgsConstructor
public class SnapshotController {

    private final MarketSnapshotReadService marketSnapshotReadService;
    private final FlipReadService flipReadService;

    @GetMapping
    public Page<MarketSnapshotDto> listSnapshots(
            @PageableDefault(size = 100, sort = "snapshotTimestampEpochMillis", direction = Sort.Direction.DESC) Pageable pageable
    ) {
        return marketSnapshotReadService.listSnapshots(pageable);
    }

    @GetMapping("/{snapshotEpochMillis}/flips")
    public Page<UnifiedFlipDto> listFlipsForSnapshot(
            @PathVariable long snapshotEpochMillis,
            @RequestParam(required = false) FlipType flipType,
            @PageableDefault(size = 50, sort = "id") Pageable pageable
    ) {
        return flipReadService.listFlips(flipType, Instant.ofEpochMilli(snapshotEpochMillis), pageable);
    }
}
