package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageBackfillService;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageParityService;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageProperties;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipCurrentReadService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/internal/admin/instrumentation/flip-storage")
@RequiredArgsConstructor
public class FlipStorageAdminController {

    private final AdminAccessGuard adminAccessGuard;
    private final FlipStorageProperties flipStorageProperties;
    private final FlipStorageParityService flipStorageParityService;
    private final FlipStorageBackfillService flipStorageBackfillService;
    private final FlipRepository flipRepository;
    private final UnifiedFlipCurrentReadService unifiedFlipCurrentReadService;

    @GetMapping("/config")
    public Map<String, Object> config(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("generatedAt", Instant.now().toString());
        response.put("dualWriteEnabled", flipStorageProperties.isDualWriteEnabled());
        response.put("readFromNew", flipStorageProperties.isReadFromNew());
        response.put("legacyWriteEnabled", flipStorageProperties.isLegacyWriteEnabled());
        response.put("topSnapshotMaterializationEnabled", flipStorageProperties.isTopSnapshotMaterializationEnabled());
        response.put("snapshotItemStateCaptureEnabled", flipStorageProperties.isSnapshotItemStateCaptureEnabled());
        response.put("trendRelativeThreshold", flipStorageProperties.getTrendRelativeThreshold());
        response.put("trendScoreDeltaThreshold", flipStorageProperties.getTrendScoreDeltaThreshold());
        response.put("paritySampleSize", flipStorageProperties.getParitySampleSize());
        response.put("legacyLatestSnapshotEpochMillis", flipRepository.findMaxSnapshotTimestampEpochMillis().orElse(null));
        response.put("currentLatestSnapshotEpochMillis", unifiedFlipCurrentReadService.latestSnapshotEpochMillis().orElse(null));
        return response;
    }

    @GetMapping("/parity/latest")
    public FlipStorageParityService.FlipStorageParityReport latestParity(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        return flipStorageParityService.latestParityReport();
    }

    @PostMapping("/backfill/latest")
    public FlipStorageBackfillService.BackfillResult backfillLatest(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        return flipStorageBackfillService.backfillLatestLegacySnapshot();
    }

    @PostMapping("/backfill/{snapshotEpochMillis}")
    public FlipStorageBackfillService.BackfillResult backfillSnapshot(HttpServletRequest request,
                                                                      @PathVariable long snapshotEpochMillis) {
        adminAccessGuard.validate(request);
        return flipStorageBackfillService.backfillSnapshot(snapshotEpochMillis);
    }
}
