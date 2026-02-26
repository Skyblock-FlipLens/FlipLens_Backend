package com.skyblockflipper.backend.service.market;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;

@Service
@Slf4j
public class CompactionRequestService {

    private final JdbcTemplate jdbcTemplate;
    private final String channel;

    public CompactionRequestService(JdbcTemplate jdbcTemplate,
                                    @Value("${config.compactor.channel:compaction}") String configuredChannel) {
        this.jdbcTemplate = jdbcTemplate;
        this.channel = sanitizeChannel(configuredChannel);
    }

    @Transactional
    public Map<String, Object> request(String requestedBy) {
        String safeRequestedBy = requestedBy == null || requestedBy.isBlank() ? "unknown" : requestedBy;
        int updated = jdbcTemplate.update("""
                insert into compaction_control (id, requested, requested_at, requested_by)
                values (1, true, now(), ?)
                on conflict (id) do update
                set requested = true,
                    requested_at = now(),
                    requested_by = excluded.requested_by
                """, safeRequestedBy);
        jdbcTemplate.execute("notify " + channel + ", 'run'");

        Instant requestedAt = Instant.now();
        log.info("Compaction requested by={} updated={} channel={} at={}",
                safeRequestedBy,
                updated,
                channel,
                requestedAt);
        return Map.of(
                "status", "requested",
                "requestedBy", safeRequestedBy,
                "requestedAtUtc", requestedAt.toString()
        );
    }

    private String sanitizeChannel(String configuredChannel) {
        String candidate = configuredChannel == null ? "" : configuredChannel.trim();
        if (candidate.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return candidate;
        }
        log.warn("Invalid compactor channel '{}' configured; falling back to 'compaction'", configuredChannel);
        return "compaction";
    }
}
