package com.skyblockflipper.backend.instrumentation.admin;

import com.skyblockflipper.backend.instrumentation.InstrumentationProperties;
import com.skyblockflipper.backend.instrumentation.JfrBlockingReportService;
import com.skyblockflipper.backend.instrumentation.JfrRecordingManager;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@RestController
@RequestMapping("/internal/admin/instrumentation")
@RequiredArgsConstructor
public class InstrumentationAdminController {

    private final AdminAccessGuard adminAccessGuard;
    private final JfrRecordingManager jfrRecordingManager;
    private final JfrBlockingReportService jfrBlockingReportService;
    private final InstrumentationProperties properties;

    @PostMapping("/jfr/snapshot")
    public Map<String, Object> dumpSnapshot(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        Path dump = jfrRecordingManager.dumpSnapshot();
        return Map.of("snapshot", dump.toString(), "createdAt", Instant.now().toString());
    }

    @GetMapping("/jfr/report/latest")
    public Map<String, Object> latestReport(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        return jfrBlockingReportService.summarize(jfrRecordingManager.latestRecordingFile());
    }

    @PostMapping("/async-profiler/run")
    public Map<String, Object> runAsyncProfiler(HttpServletRequest request) {
        adminAccessGuard.validate(request);
        if (!properties.getAsyncProfiler().isEnabled()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "async-profiler integration is disabled");
        }
        try {
            Files.createDirectories(properties.getAsyncProfiler().getOutputDir());
            String pid = Long.toString(ProcessHandle.current().pid());
            Process process = new ProcessBuilder(
                    properties.getAsyncProfiler().getScriptPath(),
                    pid,
                    properties.getAsyncProfiler().getOutputDir().toString(),
                    Instant.now().toString().replace(':', '-')
            ).redirectErrorStream(true).start();
            int exit = process.waitFor();
            String output = new String(process.getInputStream().readAllBytes());
            if (exit != 0) {
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, output);
            }
            List<Path> files;
            try (Stream<Path> stream = Files.list(properties.getAsyncProfiler().getOutputDir())) {
                files = stream
                        .sorted((a, b) -> Long.compare(b.toFile().lastModified(), a.toFile().lastModified()))
                        .limit(3)
                        .toList();
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("status", "completed");
            result.put("latestArtifacts", files.stream().map(Path::toString).toList());
            result.put("scriptOutput", output);
            return result;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, exception.getMessage(), exception);
        } catch (IOException exception) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, exception.getMessage(), exception);
        }
    }
}
