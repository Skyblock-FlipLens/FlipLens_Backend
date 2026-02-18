package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.format.annotation.DateTimeFormat;
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

    @GetMapping("/{id}")
    public ResponseEntity<UnifiedFlipDto> getFlipById(@PathVariable UUID id) {
        return flipReadService.findFlipById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
