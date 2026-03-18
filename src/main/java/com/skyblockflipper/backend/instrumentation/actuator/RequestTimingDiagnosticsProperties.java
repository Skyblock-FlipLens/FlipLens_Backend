package com.skyblockflipper.backend.instrumentation.actuator;

import jakarta.annotation.PostConstruct;
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

    @PostConstruct
    public void validate() {
        if (interval == null || interval.compareTo(Duration.ofMillis(1)) < 0) {
            throw new IllegalArgumentException("instrumentation.request-timings.interval must be at least 1ms");
        }
        if (historyReadLimitMax <= 0) {
            throw new IllegalArgumentException("instrumentation.request-timings.history-read-limit-max must be greater than 0");
        }
        output.validate();
    }

    @Getter
    @Setter
    public static class Output {
        private boolean enabled = true;
        private Path file = Path.of("var", "profiling", "diagnostics", "request-timings.jsonl");
        private int maxHistoryLines = 1440;

        void validate() {
            if (maxHistoryLines <= 0) {
                throw new IllegalArgumentException("instrumentation.request-timings.output.max-history-lines must be greater than 0");
            }
        }
    }
}
