package com.skyblockflipper.backend.instrumentation.actuator;

import lombok.RequiredArgsConstructor;
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
    public Object requestTimings(Integer historyLimit) {
        if (historyLimit == null) {
            RequestTimingDiagnosticsDto.Snapshot snapshot = diagnosticsService.getLastSnapshot();
            if (snapshot == null) {
                return baseResponse("NO_DATA");
            }
            return snapshot;
        }

        int sanitizedLimit = Math.min(Math.max(1, properties.getHistoryReadLimitMax()), Math.max(1, historyLimit));
        Map<String, Object> response = baseResponse("OK");
        response.put("historyLimit", sanitizedLimit);
        response.put("latest", diagnosticsService.getLastSnapshot());
        response.put("history", diagnosticsService.readRecentSnapshots(sanitizedLimit));
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
}
