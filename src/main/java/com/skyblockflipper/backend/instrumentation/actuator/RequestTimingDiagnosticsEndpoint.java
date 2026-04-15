package com.skyblockflipper.backend.instrumentation.actuator;

import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
@Endpoint(id = "requestTimings")
@Profile("!compactor")
@RequiredArgsConstructor
public class RequestTimingDiagnosticsEndpoint {

    private final RequestTimingDiagnosticsService diagnosticsService;
    private final RequestTimingDiagnosticsProperties properties;

    @ReadOperation
    public Map<String, Object> requestTimings(@Nullable Integer historyLimit) {
        RequestTimingDiagnosticsDto.Snapshot latest = diagnosticsService.getLastSnapshot();
        Map<String, Object> response = baseResponse(latest == null ? "NO_DATA" : "OK");
        response.put("latest", latest);
        if (historyLimit != null) {
            int sanitizedLimit = sanitizeHistoryLimit(historyLimit);
            response.put("historyLimit", sanitizedLimit);
            response.put("history", diagnosticsService.readRecentSnapshots(sanitizedLimit));
        }
        return response;
    }

    private Map<String, Object> baseResponse(String status) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", status);
        response.put("enabled", properties.isEnabled());
        response.put("fileOutputEnabled", properties.getOutput().isEnabled());
        response.put("outputFile", properties.getOutput().getFile() == null ? null : properties.getOutput().getFile().toString());
        response.put("historyReadLimitMax", Math.max(1, properties.getHistoryReadLimitMax()));
        return response;
    }

    private int sanitizeHistoryLimit(int historyLimit) {
        return Math.min(Math.max(1, properties.getHistoryReadLimitMax()), Math.max(1, historyLimit));
    }
}
