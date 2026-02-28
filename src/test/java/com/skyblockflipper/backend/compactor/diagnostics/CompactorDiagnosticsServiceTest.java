package com.skyblockflipper.backend.compactor.diagnostics;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CompactorDiagnosticsServiceTest {

    @Test
    void startAndStopDoNothingWhenDisabled() {
        DataSource dataSource = mock(DataSource.class);
        CompactorDiagnosticsProperties properties = new CompactorDiagnosticsProperties();
        properties.setEnabled(false);
        CompactorDiagnosticsService service = new CompactorDiagnosticsService(dataSource, properties, new ObjectMapper());

        service.start();

        assertFalse(service.isRunning());
        service.stop();
        assertFalse(service.isRunning());
    }

    @Test
    void collectSnapshotRestoresStatementTimeoutAndHandlesMissingPgStatStatements() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);

        ResultSet showTimeoutRs = mock(ResultSet.class);
        when(showTimeoutRs.next()).thenReturn(true, false);
        when(showTimeoutRs.getString(1)).thenReturn("1s");

        ResultSet topRunningRs = mock(ResultSet.class);
        when(topRunningRs.next()).thenReturn(true, false);
        when(topRunningRs.getLong("pid")).thenReturn(77L);
        when(topRunningRs.getString("state")).thenReturn("active");
        when(topRunningRs.getString("wait_event_type")).thenReturn("IO");
        when(topRunningRs.getString("wait_event")).thenReturn("WalSync");
        when(topRunningRs.getString("running_for")).thenReturn("00:00:02");
        when(topRunningRs.getString("query")).thenReturn("select 1");

        ResultSet locksRs = mock(ResultSet.class);
        when(locksRs.next()).thenReturn(true, false);
        when(locksRs.getBoolean("granted")).thenReturn(false);

        ResultSet vacuumRs = mock(ResultSet.class);
        when(vacuumRs.next()).thenReturn(true, false);
        when(vacuumRs.getString("relname")).thenReturn("flip_step");
        when(vacuumRs.getLong("n_live_tup")).thenReturn(100L);
        when(vacuumRs.getLong("n_dead_tup")).thenReturn(20L);
        when(vacuumRs.getString("last_autovacuum")).thenReturn("2026-02-28 20:00:00");
        when(vacuumRs.getString("last_vacuum")).thenReturn(null);
        when(vacuumRs.getLong("autovacuum_count")).thenReturn(2L);
        when(vacuumRs.getLong("vacuum_count")).thenReturn(0L);

        ResultSet cacheRs = mock(ResultSet.class);
        when(cacheRs.next()).thenReturn(true, false);
        when(cacheRs.getDouble("hit_ratio_pct")).thenReturn(99.5D);
        when(cacheRs.wasNull()).thenReturn(false);

        SQLException missingPgStatStatements = new SQLException("relation pg_stat_statements does not exist", "42P01");
        when(statement.executeQuery(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0, String.class);
            if ("SHOW statement_timeout".equals(sql)) {
                return showTimeoutRs;
            }
            if (sql.contains("FROM pg_stat_activity") && sql.contains("state <> 'idle'")) {
                return topRunningRs;
            }
            if (sql.contains("FROM pg_locks")) {
                return locksRs;
            }
            if (sql.contains("FROM pg_stat_user_tables")) {
                return vacuumRs;
            }
            if (sql.contains("FROM pg_stat_database")) {
                return cacheRs;
            }
            if (sql.contains("FROM pg_stat_statements")) {
                throw missingPgStatStatements;
            }
            throw new IllegalArgumentException("Unexpected SQL: " + sql);
        });

        CompactorDiagnosticsProperties properties = new CompactorDiagnosticsProperties();
        properties.setEnabled(true);
        properties.getApi().setBaseUrl(" ");
        properties.getDb().setEnabled(true);
        properties.getDb().setStatementTimeout(Duration.ofSeconds(2));
        CompactorDiagnosticsService service = new CompactorDiagnosticsService(dataSource, properties, new ObjectMapper());

        CompactorDiagnosticsDto.Snapshot snapshot = invokeCollectSnapshot(service);

        assertNotNull(snapshot);
        assertEquals("DOWN", snapshot.apiHealth().status());
        assertTrue(snapshot.errors().stream().anyMatch(e -> e.startsWith("api_probe_failed:missing_base_url")));
        assertTrue(snapshot.dbWaitSummary().containsKey("WalSync"));
        assertTrue(snapshot.dbWaitSummary().containsKey("lock_waiting"));
        assertEquals(1, snapshot.topLongQueries().size());
        assertEquals(1, snapshot.vacuumHotTables().size());
        assertEquals(99.5D, snapshot.cacheHitRatio());
        assertFalse(snapshot.pgStatStatementsAvailable());
        assertEquals(List.of(), snapshot.topStatements());

        verify(connection, atLeastOnce()).setReadOnly(true);
        verify(statement).execute("SET statement_timeout TO '2000ms'");
        verify(statement).execute("SET statement_timeout TO '1s'");
    }

    private CompactorDiagnosticsDto.Snapshot invokeCollectSnapshot(CompactorDiagnosticsService service) throws Exception {
        Method method = CompactorDiagnosticsService.class.getDeclaredMethod("collectSnapshot");
        method.setAccessible(true);
        return (CompactorDiagnosticsDto.Snapshot) method.invoke(service);
    }
}
