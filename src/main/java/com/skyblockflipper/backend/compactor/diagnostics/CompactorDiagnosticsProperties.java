package com.skyblockflipper.backend.compactor.diagnostics;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "diagnostics")
public class CompactorDiagnosticsProperties {

    private boolean enabled = true;
    private Duration interval = Duration.ofSeconds(60);
    private final Api api = new Api();
    private final Db db = new Db();

    @Getter
    @Setter
    public static class Api {
        private String baseUrl = "http://api:1881";
        private Duration connectTimeout = Duration.ofSeconds(2);
        private Duration readTimeout = Duration.ofSeconds(2);
    }

    @Getter
    @Setter
    public static class Db {
        private boolean enabled = true;
        private Duration statementTimeout = Duration.ofSeconds(2);
    }
}
