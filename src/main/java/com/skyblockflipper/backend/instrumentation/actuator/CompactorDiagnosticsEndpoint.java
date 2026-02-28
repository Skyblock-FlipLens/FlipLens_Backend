package com.skyblockflipper.backend.instrumentation.actuator;

import com.skyblockflipper.backend.compactor.diagnostics.CompactorDiagnosticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Endpoint(id = "diagnostics")
@Profile("compactor")
@RequiredArgsConstructor
public class CompactorDiagnosticsEndpoint {

    private final CompactorDiagnosticsService diagnosticsService;

    @ReadOperation
    public Object diagnostics() {
        var snapshot = diagnosticsService.getLastSnapshot();
        if (snapshot == null) {
            return Map.of("status", "NO_DATA");
        }
        return snapshot;
    }
}
