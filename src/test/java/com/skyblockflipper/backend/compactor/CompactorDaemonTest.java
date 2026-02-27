package com.skyblockflipper.backend.compactor;

import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        verify(jdbcTemplate).update("update compaction_control set requested = true where id = 1");
        verify(marketDataProcessingService, never()).compactSnapshots();
    }

    @Test
    void tryRunIfRequestedPersistsStatusOnSuccess() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        MarketDataProcessingService marketDataProcessingService = mock(MarketDataProcessingService.class);
        when(jdbcTemplate.query(anyString(), any(ResultSetExtractor.class))).thenReturn(Boolean.TRUE);
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
        Method reasonMethod = result.getClass().getDeclaredMethod("reason");
        String reason = (String) reasonMethod.invoke(result);
        assertEquals("api_readiness_url_blocked", reason);
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

    private Object invokePrivate(Object target, String methodName) throws Exception {
        return invokePrivate(target, methodName, new Class<?>[0]);
    }

    private Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
