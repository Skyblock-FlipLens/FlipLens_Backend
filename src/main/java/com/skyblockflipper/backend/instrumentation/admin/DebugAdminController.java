package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.config.Jobs.SourceJobs;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@RestController
@RequestMapping("/internal/admin/debug")
@RequiredArgsConstructor
@Slf4j
public class DebugAdminController {

    private final AdminAccessGuard adminAccessGuard;
    private final SourceJobs sourceJobs;
    private final AtomicReference<Instant> lastCompactSnapshotsTrigger = new AtomicReference<>();
    private final AtomicReference<Instant> lastCopyRepoTrigger = new AtomicReference<>();

    @PostMapping("/trigger/compact-snapshots")
    public Map<String, Object> triggerCompactSnapshots(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        Instant triggeredAt = Instant.now();
        log.info("Manual debug trigger: compactSnapshots from {}", request.getRemoteAddr());
        sourceJobs.compactSnapshots();
        lastCompactSnapshotsTrigger.set(triggeredAt);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "triggered");
        response.put("job", "compactSnapshots");
        response.put("triggeredAtUtc", triggeredAt.toString());
        return response;
    }

    @PostMapping("/trigger/copy-repo")
    public Map<String, Object> triggerCopyRepo(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        Instant triggeredAt = Instant.now();
        log.info("Manual debug trigger: copyRepoDaily from {}", request.getRemoteAddr());
        sourceJobs.copyRepoDaily();
        lastCopyRepoTrigger.set(triggeredAt);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "triggered");
        response.put("job", "copyRepoDaily");
        response.put("triggeredAtUtc", triggeredAt.toString());
        return response;
    }

    @GetMapping("/monitoring")
    public Map<String, Object> monitoring(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        Runtime runtime = Runtime.getRuntime();
        long heapUsed = runtime.totalMemory() - runtime.freeMemory();
        Map<String, Object> triggerTimestamps = new LinkedHashMap<>();
        triggerTimestamps.put("compactSnapshots", toIso(lastCompactSnapshotsTrigger.get()));
        triggerTimestamps.put("copyRepoDaily", toIso(lastCopyRepoTrigger.get()));

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("timestampUtc", Instant.now().toString());
        response.put("heapUsedBytes", heapUsed);
        response.put("heapMaxBytes", runtime.maxMemory());
        response.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());
        response.put("availableProcessors", runtime.availableProcessors());
        response.put("lastManualTriggerTimestampsUtc", triggerTimestamps);
        return response;
    }

    private String toIso(Instant value) {
        return value == null ? null : value.toString();
    }
}
