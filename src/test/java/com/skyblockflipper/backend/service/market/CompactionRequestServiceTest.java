package com.skyblockflipper.backend.service.market;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CompactionRequestServiceTest {

    @Test
    void requestUsesConfiguredChannelAndRequester() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.update(anyString(), org.mockito.ArgumentMatchers.<Object[]>any())).thenReturn(1);
        CompactionRequestService service = new CompactionRequestService(jdbcTemplate, "compaction_ops");

        Map<String, Object> result = service.request("api:127.0.0.1");

        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate).update(anyString(), argsCaptor.capture());
        assertEquals("api:127.0.0.1", argsCaptor.getValue()[0]);
        verify(jdbcTemplate).execute("notify compaction_ops, 'run'");
        assertEquals("requested", result.get("status"));
        assertEquals("api:127.0.0.1", result.get("requestedBy"));
        assertNotNull(result.get("requestedAtUtc"));
    }

    @Test
    void requestFallsBackToUnknownRequesterAndDefaultChannel() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.update(anyString(), org.mockito.ArgumentMatchers.<Object[]>any())).thenReturn(1);
        CompactionRequestService service = new CompactionRequestService(jdbcTemplate, "compaction;drop");

        Map<String, Object> result = service.request("  ");

        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate).update(anyString(), argsCaptor.capture());
        assertEquals("unknown", argsCaptor.getValue()[0]);
        verify(jdbcTemplate).execute("notify compaction, 'run'");
        assertEquals("unknown", result.get("requestedBy"));
    }
}
