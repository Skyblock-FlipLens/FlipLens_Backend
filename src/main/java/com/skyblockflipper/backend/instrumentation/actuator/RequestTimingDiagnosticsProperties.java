package com.skyblockflipper.backend.instrumentation.actuator;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "instrumentation.request-timings")
public class RequestTimingDiagnosticsProperties {

    private boolean enabled = true;
    private Duration interval = Duration.ofSeconds(60);
    private int historyReadLimitMax = 50;
    private final Output output = new Output();

    @Getter
    @Setter
    public static class Output {
        private boolean enabled = true;
        private Path file = Path.of("var", "profiling", "diagnostics", "request-timings.jsonl");
    }
}
