package com.skyblockflipper.backend.compactor.diagnostics;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CompactorDiagnosticsServiceTest {

    @Test
    void startAndStopDoNothingWhenDisabled() {
        DataSource dataSource = Mockito.mock(DataSource.class);
        CompactorDiagnosticsProperties properties = new CompactorDiagnosticsProperties();
        properties.setEnabled(false);
        CompactorDiagnosticsService service = new CompactorDiagnosticsService(dataSource, properties, new ObjectMapper());

        service.start();

        assertFalse(service.isRunning());
        service.stop();
        assertFalse(service.isRunning());
        verifyNoInteractions(dataSource);
    }

    @Test
    void collectSnapshotRestoresStatementTimeoutAndHandlesMissingPgStatStatements() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);

        ResultSet showTimeoutRs = Mockito.mock(ResultSet.class);
        when(showTimeoutRs.next()).thenReturn(true, false);
        when(showTimeoutRs.getString(1)).thenReturn("1s");

        ResultSet topRunningRs = Mockito.mock(ResultSet.class);
        when(topRunningRs.next()).thenReturn(true, false);
        when(topRunningRs.getLong("pid")).thenReturn(77L);
        when(topRunningRs.getString("state")).thenReturn("active");
        when(topRunningRs.getString("wait_event_type")).thenReturn("IO");
        when(topRunningRs.getString("wait_event")).thenReturn("WalSync");
        when(topRunningRs.getString("running_for")).thenReturn("00:00:02");
        when(topRunningRs.getString("query")).thenReturn("select 1");

        ResultSet locksRs = Mockito.mock(ResultSet.class);
        when(locksRs.next()).thenReturn(true, false);
        when(locksRs.getBoolean("granted")).thenReturn(false);

        ResultSet vacuumRs = Mockito.mock(ResultSet.class);
        when(vacuumRs.next()).thenReturn(true, false);
        when(vacuumRs.getString("relname")).thenReturn("flip_step");
        when(vacuumRs.getLong("n_live_tup")).thenReturn(100L);
        when(vacuumRs.getLong("n_dead_tup")).thenReturn(20L);
        when(vacuumRs.getString("last_autovacuum")).thenReturn("2026-02-28 20:00:00");
        when(vacuumRs.getString("last_vacuum")).thenReturn(null);
        when(vacuumRs.getLong("autovacuum_count")).thenReturn(2L);
        when(vacuumRs.getLong("vacuum_count")).thenReturn(0L);

        ResultSet cacheRs = Mockito.mock(ResultSet.class);
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

        CompactorDiagnosticsDto.Snapshot snapshot = service.collectSnapshot();

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

    @Test
    void probeApiHealthReturnsUpAndUsesTrimmedActuatorHealthUrl() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        CompactorDiagnosticsProperties properties = new CompactorDiagnosticsProperties();
        properties.getApi().setBaseUrl("  http://example.test///  ");
        CompactorDiagnosticsService service = new CompactorDiagnosticsService(dataSource, properties, new ObjectMapper());
        HttpClient client = Mockito.mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> response = Mockito.mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.body()).thenReturn("{\"status\":\"UP\",\"details\":{\"db\":\"ok\"}}");
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        setField(service, "apiClient", client);

        List<String> errors = new ArrayList<>();
        CompactorDiagnosticsDto.ApiHealth apiHealth = (CompactorDiagnosticsDto.ApiHealth) invokePrivate(
                service,
                "probeApiHealth",
                new Class<?>[]{List.class},
                errors
        );

        assertEquals("UP", apiHealth.status());
        assertEquals(200, apiHealth.httpStatus());
        assertEquals("ok", apiHealth.details().get("db"));
        assertTrue(errors.isEmpty());
        verify(client).send(Mockito.argThat(request -> request.uri().toString().equals("http://example.test/actuator/health")),
                any(HttpResponse.BodyHandler.class));
    }

    @Test
    void probeApiHealthReturnsDownForUnhealthyResponse() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        CompactorDiagnosticsProperties properties = new CompactorDiagnosticsProperties();
        properties.getApi().setBaseUrl("http://example.test");
        CompactorDiagnosticsService service = new CompactorDiagnosticsService(dataSource, properties, new ObjectMapper());
        HttpClient client = Mockito.mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> response = Mockito.mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(503);
        when(response.body()).thenReturn("{\"status\":\"OUT_OF_SERVICE\"}");
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        setField(service, "apiClient", client);

        CompactorDiagnosticsDto.ApiHealth apiHealth = (CompactorDiagnosticsDto.ApiHealth) invokePrivate(
                service,
                "probeApiHealth",
                new Class<?>[]{List.class},
                new ArrayList<String>()
        );

        assertEquals("DOWN", apiHealth.status());
        assertEquals("health_status_OUT_OF_SERVICE", apiHealth.error());
    }

    @Test
    void runDiagnosticsSafelyStoresSnapshotWhenServiceIsRunning() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        CompactorDiagnosticsProperties properties = new CompactorDiagnosticsProperties();
        properties.setEnabled(true);
        properties.getDb().setEnabled(false);
        properties.getApi().setBaseUrl(" ");
        CompactorDiagnosticsService service = new CompactorDiagnosticsService(dataSource, properties, new ObjectMapper());
        setField(service, "running", true);

        invokePrivate(service, "runDiagnosticsSafely");

        assertNotNull(service.getLastSnapshot());
        assertTrue(service.getLastSnapshot().errors().stream().anyMatch(error -> error.contains("missing_base_url")));
    }

    @Test
    void writeSnapshotToFileAppendsJsonLinesAndGetApiClientCachesInstance() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        CompactorDiagnosticsProperties properties = new CompactorDiagnosticsProperties();
        properties.getApi().setConnectTimeout(Duration.ZERO);
        Path tempFile = Files.createTempDirectory("compactor-diagnostics-test")
                .resolve("nested")
                .resolve("diagnostics.jsonl");
        properties.getOutput().setEnabled(true);
        properties.getOutput().setFile(tempFile);
        CompactorDiagnosticsService service = new CompactorDiagnosticsService(dataSource, properties, new ObjectMapper());

        invokePrivate(service, "writeSnapshotToFile", new Class<?>[]{String.class}, "{\"ok\":1}");
        invokePrivate(service, "writeSnapshotToFile", new Class<?>[]{String.class}, "{\"ok\":2}");

        List<String> lines = Files.readAllLines(tempFile);
        assertEquals(List.of("{\"ok\":1}", "{\"ok\":2}"), lines);

        HttpClient first = (HttpClient) invokePrivate(service, "getApiClient");
        HttpClient second = (HttpClient) invokePrivate(service, "getApiClient");
        assertSame(first, second);
    }

    @Test
    void writeSnapshotToFileHandlesMissingPathWithoutThrowing() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        CompactorDiagnosticsProperties properties = new CompactorDiagnosticsProperties();
        properties.getOutput().setEnabled(true);
        properties.getOutput().setFile(null);
        CompactorDiagnosticsService service = new CompactorDiagnosticsService(dataSource, properties, new ObjectMapper());

        assertDoesNotThrow(() -> invokePrivate(service, "writeSnapshotToFile", new Class<?>[]{String.class}, "{\"ok\":1}"));
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private static Object invokePrivate(Object target, String methodName) throws Exception {
        return invokePrivate(target, methodName, new Class<?>[0]);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
