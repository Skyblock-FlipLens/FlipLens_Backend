package com.skyblockflipper.backend.compactor;

import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CompactorDaemonTest {

    @Test
    void tryRunIfRequestedSkipsWhenNothingClaimed() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(jdbcTemplate.query(anyString(), any(ResultSetExtractor.class))).thenReturn(Boolean.FALSE);
        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );
        setRunning(daemon, true);

        invokePrivate(daemon, "tryRunIfRequested");

        verify(dataSource, never()).getConnection();
        verify(marketDataProcessingService, never()).compactSnapshots();
    }

    @Test
    void tryRunIfRequestedRequeuesWhenLockHeld() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(jdbcTemplate.query(anyString(), any(ResultSetExtractor.class))).thenReturn(Boolean.TRUE);
        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class))).thenAnswer(invocation -> {
            ResultSetExtractor<Object> extractor = invocation.getArgument(2);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true);
            when(rs.getBoolean("requested")).thenReturn(true);
            when(rs.getTimestamp("last_run_at")).thenReturn(null);
            return extractor.extractData(rs);
        });
        Connection connection = mockLockConnection(false, true);
        when(dataSource.getConnection()).thenReturn(connection);
        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );
        setRunning(daemon, true);

        invokePrivate(daemon, "tryRunIfRequested");

        verify(jdbcTemplate, never()).update("update compaction_control set requested = true where id = 1");
        verify(marketDataProcessingService, never()).compactSnapshots();
    }

    @Test
    void tryRunIfRequestedPersistsStatusOnSuccess() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(jdbcTemplate.query(anyString(), any(ResultSetExtractor.class))).thenReturn(Boolean.TRUE);
        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class))).thenAnswer(invocation -> {
            ResultSetExtractor<Object> extractor = invocation.getArgument(2);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true);
            when(rs.getBoolean("requested")).thenReturn(true);
            when(rs.getTimestamp("last_run_at")).thenReturn(null);
            return extractor.extractData(rs);
        });
        when(marketDataProcessingService.compactSnapshots())
                .thenReturn(new MarketSnapshotPersistenceService.SnapshotCompactionResult(10, 4, 6));
        Connection connection = mockLockConnection(true, true);
        when(dataSource.getConnection()).thenReturn(connection);
        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );
        setRunning(daemon, true);

        invokePrivate(daemon, "tryRunIfRequested");

        verify(marketDataProcessingService).compactSnapshots();
        verify(jdbcTemplate, atLeastOnce()).update(contains("last_run_ok = true"), org.mockito.ArgumentMatchers.<Object[]>any());
    }

    @Test
    void tryRunIfRequestedSkipsWhenLocalCompactionAlreadyInProgress() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );
        setRunning(daemon, true);
        setField(daemon, "claimedCompactionInProgress", new java.util.concurrent.atomic.AtomicBoolean(true));

        invokePrivate(daemon, "tryRunIfRequested");

        verify(jdbcTemplate, never()).query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class));
        verify(dataSource, never()).getConnection();
        verify(marketDataProcessingService, never()).compactSnapshots();
    }

    @Test
    void stopCanBeFollowedByStartAgain() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(jdbcTemplate.update(contains("insert into compaction_control"))).thenReturn(1);
        when(jdbcTemplate.queryForObject("select requested from compaction_control where id = 1", Boolean.class))
                .thenReturn(Boolean.TRUE);
        when(dataSource.getConnection()).thenThrow(new SQLTimeoutException("pool busy"));

        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );

        daemon.start();
        daemon.stop();
        daemon.start();
        daemon.stop();

        assertFalse(daemon.isRunning());
    }

    @Test
    void startDoesNotHardFailWhenControlRowCreationIsUnavailable() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(jdbcTemplate.update(contains("insert into compaction_control")))
                .thenThrow(new RuntimeException("relation compaction_control does not exist"));
        when(dataSource.getConnection()).thenThrow(new SQLTimeoutException("pool busy"));

        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );

        daemon.start();
        invokePrivate(daemon, "tryRunIfRequested");
        daemon.stop();

        assertFalse(daemon.isRunning());
        verify(marketDataProcessingService, never()).compactSnapshots();
    }

    @Test
    void ensureCompactionControlConsistentRequeuesWhenRequestedFalse() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(jdbcTemplate.update(contains("insert into compaction_control"))).thenReturn(1);
        when(jdbcTemplate.queryForObject("select requested from compaction_control where id = 1", Boolean.class))
                .thenReturn(Boolean.FALSE);

        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );

        invokePrivate(daemon, "ensureCompactionControlConsistent", new Class<?>[]{boolean.class}, true);

        verify(jdbcTemplate).update("update compaction_control set requested = true where id = 1 and requested = false");
    }

    @Test
    void sanitizeChannelFallsBackForInvalidIdentifier() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);

        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "bad-channel-name",
                60_000L,
                500L,
                500L,
                912345678L
        );

        Field channelField = CompactorDaemon.class.getDeclaredField("channel");
        channelField.setAccessible(true);
        assertEquals("compaction", channelField.get(daemon));
    }

    @Test
    void probeApiReadinessBlocksDisallowedHost() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );
        setField(daemon, "apiReadinessEnabled", true);
        setField(daemon, "apiReadinessUrl", "http://example.org:1880/actuator/compactionReadiness");
        setField(daemon, "apiReadinessAllowedHosts", "skyblockflipper-api,localhost,127.0.0.1");

        Object result = invokePrivate(daemon, "probeApiReadiness");
        Method stateMethod = result.getClass().getDeclaredMethod("state");
        Object state = stateMethod.invoke(result);
        Method reasonMethod = result.getClass().getDeclaredMethod("reason");
        String reason = (String) reasonMethod.invoke(result);
        assertEquals("UNKNOWN", String.valueOf(state));
        assertEquals("api_readiness_url_blocked", reason);
    }

    @Test
    void probeApiReadinessReturnsUnknownWhenDisabled() throws Exception {
        CompactorDaemon daemon = newDaemon();
        setField(daemon, "apiReadinessEnabled", false);

        Object result = invokePrivate(daemon, "probeApiReadiness");

        assertEquals("UNKNOWN", String.valueOf(result.getClass().getDeclaredMethod("state").invoke(result)));
        assertEquals("api_readiness_disabled", result.getClass().getDeclaredMethod("reason").invoke(result));
    }

    @Test
    void probeApiReadinessReturnsUnknownWhenUrlBlank() throws Exception {
        CompactorDaemon daemon = newDaemon();
        setField(daemon, "apiReadinessEnabled", true);
        setField(daemon, "apiReadinessUrl", "   ");

        Object result = invokePrivate(daemon, "probeApiReadiness");

        assertEquals("UNKNOWN", String.valueOf(result.getClass().getDeclaredMethod("state").invoke(result)));
        assertEquals("api_readiness_url_missing", result.getClass().getDeclaredMethod("reason").invoke(result));
    }

    @Test
    void parseAndValidateApiReadinessUriRejectsUnsafeOrMalformedUris() throws Exception {
        CompactorDaemon daemon = newDaemon();
        setField(daemon, "apiReadinessAllowedHosts", "api,localhost");

        assertNull(invokePrivate(daemon, "parseAndValidateApiReadinessUri", new Class<?>[]{String.class}, "ftp://api:1880/actuator/compactionReadiness"));
        assertNull(invokePrivate(daemon, "parseAndValidateApiReadinessUri", new Class<?>[]{String.class}, "http://api:1880/actuator/compactionReadiness?x=1"));
        assertNull(invokePrivate(daemon, "parseAndValidateApiReadinessUri", new Class<?>[]{String.class}, "http://user@api:1880/actuator/compactionReadiness"));
        assertNull(invokePrivate(daemon, "parseAndValidateApiReadinessUri", new Class<?>[]{String.class}, "not-a-uri"));
    }

    @Test
    void parseAndValidateApiReadinessUriAcceptsAllowedHostCaseInsensitive() throws Exception {
        CompactorDaemon daemon = newDaemon();
        setField(daemon, "apiReadinessAllowedHosts", "API,localhost");

        Object result = invokePrivate(
                daemon,
                "parseAndValidateApiReadinessUri",
                new Class<?>[]{String.class},
                "http://api:1880/actuator/compactionReadiness"
        );

        assertNotNull(result);
        assertEquals(
                URI.create("http://api:1880/actuator/compactionReadiness"),
                result
        );
    }

    @Test
    void probeApiReadinessReturnsHttpStatusReason() throws Exception {
        CompactorDaemon daemon = newDaemon();
        HttpClient httpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(503);
        when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        setField(daemon, "apiReadinessUrl", "http://api:1880/actuator/compactionReadiness");
        setField(daemon, "apiReadinessAllowedHosts", "api,localhost");
        setField(daemon, "apiReadinessHttpClient", httpClient);
        setField(daemon, "apiReadinessHttpClientConnectTimeoutMillis", 500L);

        Object result = invokePrivate(daemon, "probeApiReadiness");

        assertEquals("UNKNOWN", String.valueOf(result.getClass().getDeclaredMethod("state").invoke(result)));
        assertEquals("api_http_503", result.getClass().getDeclaredMethod("reason").invoke(result));
    }

    @Test
    void probeApiReadinessParsesReadyAndNotReadyStates() throws Exception {
        CompactorDaemon daemon = newDaemon();
        HttpClient httpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> readyResponse = mock(HttpResponse.class);
        when(readyResponse.statusCode()).thenReturn(200);
        when(readyResponse.body()).thenReturn("{\"ok\":true,\"reason\":\"low_load\"}");
        @SuppressWarnings("unchecked")
        HttpResponse<String> notReadyResponse = mock(HttpResponse.class);
        when(notReadyResponse.statusCode()).thenReturn(200);
        when(notReadyResponse.body()).thenReturn("{\"ok\":false}");
        when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(readyResponse, notReadyResponse);
        setField(daemon, "apiReadinessUrl", "http://api:1880/actuator/compactionReadiness");
        setField(daemon, "apiReadinessAllowedHosts", "api,localhost");
        setField(daemon, "apiReadinessHttpClient", httpClient);
        setField(daemon, "apiReadinessHttpClientConnectTimeoutMillis", 500L);

        Object ready = invokePrivate(daemon, "probeApiReadiness");
        Object notReady = invokePrivate(daemon, "probeApiReadiness");

        assertEquals("READY", String.valueOf(ready.getClass().getDeclaredMethod("state").invoke(ready)));
        assertEquals("low_load", ready.getClass().getDeclaredMethod("reason").invoke(ready));
        assertEquals("NOT_READY", String.valueOf(notReady.getClass().getDeclaredMethod("state").invoke(notReady)));
        assertEquals("not_ready", notReady.getClass().getDeclaredMethod("reason").invoke(notReady));
    }

    @Test
    void probeApiReadinessReturnsUnknownWhenHttpCallFails() throws Exception {
        CompactorDaemon daemon = newDaemon();
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenThrow(new RuntimeException("network"));
        setField(daemon, "apiReadinessUrl", "http://api:1880/actuator/compactionReadiness");
        setField(daemon, "apiReadinessAllowedHosts", "api,localhost");
        setField(daemon, "apiReadinessHttpClient", httpClient);
        setField(daemon, "apiReadinessHttpClientConnectTimeoutMillis", 500L);

        Object result = invokePrivate(daemon, "probeApiReadiness");

        assertEquals("UNKNOWN", String.valueOf(result.getClass().getDeclaredMethod("state").invoke(result)));
        assertEquals("api_unreachable", result.getClass().getDeclaredMethod("reason").invoke(result));
    }

    @Test
    void probeApiReadinessRestoresInterruptAndThrowsOnInterruptedException() throws Exception {
        CompactorDaemon daemon = newDaemon();
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenThrow(new InterruptedException("stop"));
        setField(daemon, "apiReadinessUrl", "http://api:1880/actuator/compactionReadiness");
        setField(daemon, "apiReadinessAllowedHosts", "api,localhost");
        setField(daemon, "apiReadinessHttpClient", httpClient);
        setField(daemon, "apiReadinessHttpClientConnectTimeoutMillis", 500L);

        InvocationTargetException invocationTargetException = assertThrows(
                InvocationTargetException.class,
                () -> invokePrivate(daemon, "probeApiReadiness")
        );
        assertInstanceOf(IllegalStateException.class, invocationTargetException.getCause());
        assertTrue(Thread.currentThread().isInterrupted());
        assertTrue(Thread.interrupted());
    }

    @Test
    void probeDbReadinessReturnsDisabledStateWhenTurnedOff() throws Exception {
        CompactorDaemon daemon = newDaemon();
        setField(daemon, "dbReadinessEnabled", false);

        Object readiness = invokePrivate(daemon, "probeDbReadiness");

        assertEquals(true, readiness.getClass().getDeclaredMethod("ready").invoke(readiness));
        assertEquals("db_readiness_disabled", readiness.getClass().getDeclaredMethod("reason").invoke(readiness));
    }

    @Test
    void probeDbReadinessMapsResultSetAndThresholds() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("active")).thenReturn(1L);
        when(resultSet.getLong("waiting")).thenReturn(0L);
        when(resultSet.getLong("lock_waits")).thenReturn(0L);
        when(jdbcTemplate.query(anyString(), any(ResultSetExtractor.class))).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ResultSetExtractor<Object> extractor = invocation.getArgument(1);
            return extractor.extractData(resultSet);
        });
        CompactorDaemon daemon = new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );

        Object readiness = invokePrivate(daemon, "probeDbReadiness");

        assertEquals(true, readiness.getClass().getDeclaredMethod("ready").invoke(readiness));
        assertEquals(true, readiness.getClass().getDeclaredMethod("strongReady").invoke(readiness));
        assertEquals("db_ready", readiness.getClass().getDeclaredMethod("reason").invoke(readiness));
    }

    @Test
    void runAdaptiveDecisionRequestsFallbackWhenLastRunMissing() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        CompactorDaemon daemon = newDaemon(jdbcTemplate);
        setRunning(daemon, true);
        setField(daemon, "apiReadinessEnabled", false);
        setField(daemon, "dbReadinessEnabled", false);
        when(jdbcTemplate.update(contains("insert into compaction_control"))).thenReturn(1);
        when(jdbcTemplate.update(contains("set requested = true"), org.mockito.ArgumentMatchers.eq("compactor:fallback"))).thenReturn(1);
        stubControlAndDbQueries(jdbcTemplate, false, null, 0L, 0L, 0L);

        invokePrivate(daemon, "runAdaptiveDecision");

        verify(jdbcTemplate).update(contains("set requested = true"), org.mockito.ArgumentMatchers.eq("compactor:fallback"));
        verify(jdbcTemplate).execute("notify compaction, 'run'");
    }

    @Test
    void runAdaptiveDecisionSkipsWhenRequestAlreadyPending() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        CompactorDaemon daemon = newDaemon(jdbcTemplate);
        setRunning(daemon, true);
        setField(daemon, "apiReadinessEnabled", false);
        setField(daemon, "dbReadinessEnabled", false);
        when(jdbcTemplate.update(contains("insert into compaction_control"))).thenReturn(1);
        stubControlAndDbQueries(jdbcTemplate, true, Instant.parse("2026-02-27T10:00:00Z"), 0L, 0L, 0L);

        invokePrivate(daemon, "runAdaptiveDecision");

        verify(jdbcTemplate, never()).update(contains("set requested = true"), Optional.ofNullable(any()));
        verify(jdbcTemplate, never()).execute(contains("notify"));
    }

    @Test
    void runAdaptiveDecisionSkipsWhenMinIntervalNotReached() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        CompactorDaemon daemon = newDaemon(jdbcTemplate);
        setRunning(daemon, true);
        setField(daemon, "apiReadinessEnabled", false);
        setField(daemon, "dbReadinessEnabled", false);
        setField(daemon, "minIntervalSeconds", 30L);
        setField(daemon, "fallbackIntervalSeconds", 60L);
        when(jdbcTemplate.update(contains("insert into compaction_control"))).thenReturn(1);
        stubControlAndDbQueries(jdbcTemplate, false, Instant.now().minusSeconds(5), 0L, 0L, 0L);

        invokePrivate(daemon, "runAdaptiveDecision");

        verify(jdbcTemplate, never()).update(contains("set requested = true"), Optional.ofNullable(any()));
        verify(jdbcTemplate, never()).execute(contains("notify"));
    }

    @Test
    void runAdaptiveDecisionAllowsLowLoadDbOnlyWhenApiUnknownAndDbStrongReady() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        CompactorDaemon daemon = newDaemon(jdbcTemplate);
        setRunning(daemon, true);
        setField(daemon, "minIntervalSeconds", 30L);
        setField(daemon, "fallbackIntervalSeconds", 120L);
        setField(daemon, "apiReadinessEnabled", true);
        setField(daemon, "apiReadinessUrl", "http://api:1880/actuator/compactionReadiness");
        setField(daemon, "apiReadinessAllowedHosts", "api,localhost");
        when(jdbcTemplate.update(contains("insert into compaction_control"))).thenReturn(1);
        when(jdbcTemplate.update(contains("set requested = true"), org.mockito.ArgumentMatchers.eq("compactor:low_load_db_only")))
                .thenReturn(1);
        stubControlAndDbQueries(jdbcTemplate, false, Instant.now().minusSeconds(40), 1L, 0L, 0L);

        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenThrow(new RuntimeException("unreachable"));
        setField(daemon, "apiReadinessHttpClient", httpClient);
        setField(daemon, "apiReadinessHttpClientConnectTimeoutMillis", 500L);

        invokePrivate(daemon, "runAdaptiveDecision");

        verify(jdbcTemplate).update(contains("set requested = true"), org.mockito.ArgumentMatchers.eq("compactor:low_load_db_only"));
        verify(jdbcTemplate).execute("notify compaction, 'run'");
    }

    @Test
    void runAdaptiveDecisionSkipsWhenApiReportsNotReady() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        CompactorDaemon daemon = newDaemon(jdbcTemplate);
        setRunning(daemon, true);
        setField(daemon, "minIntervalSeconds", 30L);
        setField(daemon, "fallbackIntervalSeconds", 120L);
        setField(daemon, "apiReadinessEnabled", true);
        setField(daemon, "apiReadinessUrl", "http://api:1880/actuator/compactionReadiness");
        setField(daemon, "apiReadinessAllowedHosts", "api,localhost");
        when(jdbcTemplate.update(contains("insert into compaction_control"))).thenReturn(1);
        stubControlAndDbQueries(jdbcTemplate, false, Instant.now().minusSeconds(40), 1L, 0L, 0L);

        HttpClient httpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.body()).thenReturn("{\"ok\":false,\"reason\":\"busy\"}");
        when(httpClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        setField(daemon, "apiReadinessHttpClient", httpClient);
        setField(daemon, "apiReadinessHttpClientConnectTimeoutMillis", 500L);

        invokePrivate(daemon, "runAdaptiveDecision");

        verify(jdbcTemplate, never()).update(contains("set requested = true"), Optional.ofNullable(any()));
        verify(jdbcTemplate, never()).execute(contains("notify"));
    }

    @Test
    void drainNotificationsHandlesNullArraysAndSqlErrors() throws Exception {
        CompactorDaemon daemon = newDaemon();
        PGConnection pgConnection = mock(PGConnection.class);
        when(pgConnection.getNotifications(500)).thenReturn(new PGNotification[]{mock(PGNotification.class), mock(PGNotification.class)});
        int count = (int) invokePrivate(
                daemon,
                "drainNotifications",
                new Class<?>[]{PGConnection.class, long.class},
                pgConnection,
                500L
        );
        assertEquals(2, count);

        when(pgConnection.getNotifications(500)).thenReturn(null);
        int nullCount = (int) invokePrivate(
                daemon,
                "drainNotifications",
                new Class<?>[]{PGConnection.class, long.class},
                pgConnection,
                500L
        );
        assertEquals(0, nullCount);

        when(pgConnection.getNotifications(500)).thenThrow(new SQLException("broken"));
        int errorCount = (int) invokePrivate(
                daemon,
                "drainNotifications",
                new Class<?>[]{PGConnection.class, long.class},
                pgConnection,
                500L
        );
        assertEquals(0, errorCount);
    }

    @Test
    void tryUnwrapPgConnectionReturnsNullOnFailure() throws Exception {
        CompactorDaemon daemon = newDaemon();
        Connection connection = mock(Connection.class);
        when(connection.unwrap(PGConnection.class)).thenThrow(new SQLException("no unwrap"));

        Object result = invokePrivate(
                daemon,
                "tryUnwrapPgConnection",
                new Class<?>[]{Connection.class},
                connection
        );

        assertNull(result);
    }

    @Test
    void sleepQuietlyRestoresInterruptStatus() throws Exception {
        CompactorDaemon daemon = newDaemon();
        Thread.currentThread().interrupt();

        invokePrivate(daemon, "sleepQuietly", new Class<?>[]{long.class}, 1_000L);

        assertTrue(Thread.currentThread().isInterrupted());
        assertTrue(Thread.interrupted());
    }

    @Test
    void shutdownExecutorCoversNormalForcedAndInterruptedPaths() throws Exception {
        CompactorDaemon daemon = newDaemon();

        boolean nullResult = (boolean) invokePrivate(
                daemon,
                "shutdownExecutor",
                new Class<?>[]{ExecutorService.class, String.class},
                null,
                "x"
        );
        assertTrue(nullResult);

        ExecutorService graceful = mock(ExecutorService.class);
        when(graceful.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(true);
        boolean gracefulResult = (boolean) invokePrivate(
                daemon,
                "shutdownExecutor",
                new Class<?>[]{ExecutorService.class, String.class},
                graceful,
                "graceful"
        );
        assertTrue(gracefulResult);

        ExecutorService forced = mock(ExecutorService.class);
        when(forced.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(false);
        when(forced.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);
        boolean forcedResult = (boolean) invokePrivate(
                daemon,
                "shutdownExecutor",
                new Class<?>[]{ExecutorService.class, String.class},
                forced,
                "forced"
        );
        assertTrue(forcedResult);
        verify(forced).shutdownNow();

        ExecutorService interrupted = mock(ExecutorService.class);
        when(interrupted.awaitTermination(10, TimeUnit.SECONDS)).thenThrow(new InterruptedException("stop"));
        boolean interruptedResult = (boolean) invokePrivate(
                daemon,
                "shutdownExecutor",
                new Class<?>[]{ExecutorService.class, String.class},
                interrupted,
                "interrupted"
        );
        assertFalse(interruptedResult);
        verify(interrupted).shutdownNow();
        assertTrue(Thread.currentThread().isInterrupted());
        assertTrue(Thread.interrupted());
    }

    private CompactorDaemon newDaemon() {
        return newDaemon(mock(JdbcTemplate.class));
    }

    private CompactorDaemon newDaemon(JdbcTemplate jdbcTemplate) {
        DataSource dataSource = mock(DataSource.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        return new CompactorDaemon(
                dataSource,
                jdbcTemplate,
                marketDataProcessingService,
                new ObjectMapper(),
                "compaction",
                60_000L,
                500L,
                500L,
                912345678L
        );
    }

    private Connection mockLockConnection(boolean lockAcquired, boolean unlockResult) throws Exception {
        Connection connection = mock(Connection.class);

        PreparedStatement lockStatement = mock(PreparedStatement.class);
        ResultSet lockResultSet = mock(ResultSet.class);
        when(connection.prepareStatement("select pg_try_advisory_lock(?)")).thenReturn(lockStatement);
        when(lockStatement.executeQuery()).thenReturn(lockResultSet);
        when(lockResultSet.next()).thenReturn(true);
        when(lockResultSet.getBoolean(1)).thenReturn(lockAcquired);

        PreparedStatement unlockStatement = mock(PreparedStatement.class);
        ResultSet unlockResultSet = mock(ResultSet.class);
        when(connection.prepareStatement("select pg_advisory_unlock(?)")).thenReturn(unlockStatement);
        when(unlockStatement.executeQuery()).thenReturn(unlockResultSet);
        when(unlockResultSet.next()).thenReturn(true);
        when(unlockResultSet.getBoolean(1)).thenReturn(unlockResult);
        return connection;
    }

    private void setRunning(CompactorDaemon daemon, boolean running) throws Exception {
        Field field = CompactorDaemon.class.getDeclaredField("running");
        field.setAccessible(true);
        field.setBoolean(daemon, running);
    }

    private void setField(CompactorDaemon daemon, String fieldName, Object value) throws Exception {
        Field field = CompactorDaemon.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(daemon, value);
    }

    private void stubControlAndDbQueries(JdbcTemplate jdbcTemplate,
                                         boolean requested,
                                         Instant lastRunAt,
                                         long active,
                                         long waiting,
                                         long lockWaits) {
        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class))).thenAnswer(invocation -> {
            ResultSetExtractor<Object> extractor = invocation.getArgument(2);
            ResultSet rs = mock(ResultSet.class);

            when(rs.next()).thenReturn(true);
            when(rs.getBoolean("requested")).thenReturn(requested);
            when(rs.getTimestamp("last_run_at")).thenReturn(lastRunAt == null ? null : Timestamp.from(lastRunAt));
            return extractor.extractData(rs);
        });
        when(jdbcTemplate.query(anyString(), any(ResultSetExtractor.class))).thenAnswer(invocation -> {
            String sql = ((String) invocation.getArgument(0)).toLowerCase(Locale.ROOT);
            ResultSetExtractor<Object> extractor = invocation.getArgument(1);
            ResultSet rs = mock(ResultSet.class);

            if (sql.contains("from pg_stat_activity")) {
                when(rs.next()).thenReturn(true);
                when(rs.getLong("active")).thenReturn(active);
                when(rs.getLong("waiting")).thenReturn(waiting);
                when(rs.getLong("lock_waits")).thenReturn(lockWaits);
                return extractor.extractData(rs);
            }

            return null;
        });
    }

    private Object invokePrivate(Object target, String methodName) throws Exception {
        return invokePrivate(target, methodName, new Class<?>[0]);
    }

    private Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
