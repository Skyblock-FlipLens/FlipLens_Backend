package com.skyblockflipper.backend.repository;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FlipStorageIndexMigrationTest {

    @Test
    void migrationDeclaresTargetedFlipStorageIndexes() throws Exception {
        ClassPathResource resource = new ClassPathResource("db/migration/V3__flip_storage_perf_hardening_indexes.sql");
        String sql = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8).toLowerCase();

        assertTrue(sql.contains("create index if not exists idx_flip_snapshot_ts_flip_type"));
        assertTrue(sql.contains("create index if not exists idx_flip_current_flip_type_stable_flip_id"));
        assertTrue(sql.contains("create index if not exists idx_flip_trend_segment_flip_key_valid_to"));
    }
}
