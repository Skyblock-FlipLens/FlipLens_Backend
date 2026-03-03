package com.skyblockflipper.backend.service.market.partitioning;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AggregateRollupServiceTest {

    @Test
    void rollupDailyForTableExecutesBzUpsertForBzTable() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any())).thenReturn(1);
        AggregateRollupService service = new AggregateRollupService(jdbcTemplate);

        service.rollupDailyForTable("bz_item_snapshot", LocalDate.of(2026, 3, 3));

        verify(jdbcTemplate, times(1)).update(anyString(), any(), any(), any(), any(), any());
    }

    @Test
    void rollupDailyForTableExecutesAhUpsertForAhTable() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any())).thenReturn(1);
        AggregateRollupService service = new AggregateRollupService(jdbcTemplate);

        service.rollupDailyForTable("ah_item_snapshot", LocalDate.of(2026, 3, 3));

        verify(jdbcTemplate, times(1)).update(anyString(), any(), any(), any(), any(), any());
    }

    @Test
    void rollupDailyForTableSkipsUnsupportedTable() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        AggregateRollupService service = new AggregateRollupService(jdbcTemplate);

        service.rollupDailyForTable("market_snapshot", LocalDate.of(2026, 3, 3));

        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any(), any());
    }
}
