package com.skyblockflipper.backend.compactor;

import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Profile("compactor")
@Slf4j
public class CompactorDaemon implements SmartLifecycle {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final MarketDataProcessingService marketDataProcessingService;
    private final String channel;
    private final long safetyTickIntervalMillis;
    private final long listenPollIntervalMillis;
    private final long listenReconnectDelayMillis;
    private final long advisoryLockKey;

    private final ExecutorService listenExecutor;
    private final ScheduledExecutorService safetyTickExecutor;
    private final AtomicBoolean claimedCompactionInProgress = new AtomicBoolean(false);
    private volatile boolean running;

    public CompactorDaemon(DataSource dataSource,
                           JdbcTemplate jdbcTemplate,
                           MarketDataProcessingService marketDataProcessingService,
                           @Value("${config.compactor.channel:compaction}") String channel,
                           @Value("${config.compactor.safety-tick-interval-ms:60000}") long safetyTickIntervalMillis,
                           @Value("${config.compactor.listen-poll-interval-ms:500}") long listenPollIntervalMillis,
                           @Value("${config.compactor.listen-reconnect-delay-ms:2000}") long listenReconnectDelayMillis,
                           @Value("${config.compactor.advisory-lock-key:912345678}") long advisoryLockKey) {
        this.dataSource = dataSource;
        this.jdbcTemplate = jdbcTemplate;
        this.marketDataProcessingService = marketDataProcessingService;
        this.channel = sanitizeChannel(channel);
        this.safetyTickIntervalMillis = Math.max(5_000L, safetyTickIntervalMillis);
        this.listenPollIntervalMillis = Math.max(100L, listenPollIntervalMillis);
        this.listenReconnectDelayMillis = Math.max(500L, listenReconnectDelayMillis);
        this.advisoryLockKey = advisoryLockKey;
        this.listenExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "compactor-listen-loop");
            t.setDaemon(true);
            return t;
        });
        this.safetyTickExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "compactor-safety-tick");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        listenExecutor.submit(this::listenLoop);
        safetyTickExecutor.scheduleWithFixedDelay(
                this::tryRunIfRequestedSafely,
                30_000L,
                safetyTickIntervalMillis,
                TimeUnit.MILLISECONDS
        );
        log.info("CompactorDaemon started (channel={}, safetyTickMs={}, listenPollMs={}, advisoryLockKey={})",
                channel,
                safetyTickIntervalMillis,
                listenPollIntervalMillis,
                advisoryLockKey);
    }

    @Override
    public synchronized void stop() {
        running = false;
        boolean listenTerminated = shutdownExecutor(listenExecutor, "compactor-listen-loop");
        boolean safetyTickTerminated = shutdownExecutor(safetyTickExecutor, "compactor-safety-tick");
        boolean shouldRequeueClaimedRequest = claimedCompactionInProgress.get() || !safetyTickTerminated;
        ensureCompactionControlConsistent(shouldRequeueClaimedRequest);
        log.info("CompactorDaemon stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private void listenLoop() {
        while (running) {
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("listen " + channel);
                PGConnection pgConnection = tryUnwrapPgConnection(connection);
                log.info("LISTEN {} registered (postgresNotificationsSupported={})", channel, pgConnection != null);

                while (running) {
                    statement.execute("select 1");
                    int notificationCount = drainNotifications(pgConnection);
                    if (notificationCount > 0) {
                        log.info("Received {} notifications on channel {}. Triggering compaction check.",
                                notificationCount,
                                channel);
                        tryRunIfRequestedSafely();
                    }
                    sleepQuietly(listenPollIntervalMillis);
                }
            } catch (SQLTimeoutException e) {
                if (!running) {
                    return;
                }
                log.warn("Compactor connection acquire timed out; retrying in {} ms: {}",
                        listenReconnectDelayMillis,
                        e.toString());
                sleepQuietly(listenReconnectDelayMillis);
            } catch (Exception e) {
                if (!running) {
                    return;
                }
                log.warn("Compactor listen loop error; retrying in {} ms: {}",
                        listenReconnectDelayMillis,
                        e.toString());
                sleepQuietly(listenReconnectDelayMillis);
            }
        }
    }

    private void tryRunIfRequestedSafely() {
        try {
            tryRunIfRequested();
        } catch (Exception e) {
            log.warn("Compactor tryRunIfRequested failed: {}", e.toString(), e);
        }
    }

    private void tryRunIfRequested() {
        if (!running) {
            return;
        }

        Boolean claimed = jdbcTemplate.query("""
                update compaction_control
                set requested = false
                where id = 1 and requested = true
                returning true
                """, rs -> rs.next() ? Boolean.TRUE : Boolean.FALSE);
        if (claimed == null || !claimed) {
            return;
        }

        claimedCompactionInProgress.set(true);
        Connection advisoryConnection = null;
        boolean lockAcquired = false;
        try {
            advisoryConnection = DataSourceUtils.getConnection(dataSource);
            lockAcquired = tryAcquireAdvisoryLock(advisoryConnection);
            if (!lockAcquired) {
                log.info("Compaction requested but advisory lock is held. Re-queueing request.");
                jdbcTemplate.update("update compaction_control set requested = true where id = 1");
                return;
            }

            Instant startedAt = Instant.now();
            log.info("Compaction run started");
            MarketSnapshotPersistenceService.SnapshotCompactionResult result = marketDataProcessingService.compactSnapshots();
            long elapsedMillis = Duration.between(startedAt, Instant.now()).toMillis();
            String msg = "deleted=" + result.deletedCount()
                    + " scanned=" + result.scannedCount()
                    + " kept=" + result.keptCount()
                    + " elapsedMs=" + elapsedMillis;
            jdbcTemplate.update("""
                    update compaction_control
                    set last_run_at = now(),
                        last_run_ok = true,
                        last_run_msg = ?
                    where id = 1
                    """, msg);
            log.info("Compaction run finished in {} ms: scanned={}, kept={}, deleted={}",
                    elapsedMillis,
                    result.scannedCount(),
                    result.keptCount(),
                    result.deletedCount());
        } catch (Exception e) {
            if (!lockAcquired) {
                jdbcTemplate.update("update compaction_control set requested = true where id = 1");
                log.warn("Failed before acquiring advisory lock; request was re-queued: {}", e.toString(), e);
                return;
            }
            jdbcTemplate.update("""
                    update compaction_control
                    set last_run_at = now(),
                        last_run_ok = false,
                        last_run_msg = ?
                    where id = 1
                    """, e.toString());
            log.warn("Compaction run failed: {}", e.toString(), e);
        } finally {
            if (advisoryConnection != null) {
                try {
                    if (lockAcquired) {
                        boolean unlocked = unlockAdvisoryLock(advisoryConnection);
                        if (!unlocked) {
                            log.warn("Compactor advisory lock {} was already unlocked when finishing run", advisoryLockKey);
                        }
                    }
                } catch (SQLException e) {
                    log.warn("Failed to release advisory lock {}: {}", advisoryLockKey, e.toString(), e);
                } finally {
                    DataSourceUtils.releaseConnection(advisoryConnection, dataSource);
                }
            }
            claimedCompactionInProgress.set(false);
        }
    }

    private String sanitizeChannel(String configuredChannel) {
        String candidate = configuredChannel == null ? "" : configuredChannel.trim();
        if (candidate.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return candidate;
        }
        log.warn("Invalid compactor channel '{}' configured; falling back to 'compaction'", configuredChannel);
        return "compaction";
    }

    private PGConnection tryUnwrapPgConnection(Connection connection) {
        try {
            return connection.unwrap(PGConnection.class);
        } catch (Exception e) {
            return null;
        }
    }

    private int drainNotifications(PGConnection pgConnection) {
        if (pgConnection == null) {
            return 0;
        }
        try {
            PGNotification[] notifications = pgConnection.getNotifications();
            return notifications == null ? 0 : notifications.length;
        } catch (SQLException e) {
            log.debug("Failed to read PG notifications", e);
        }
        return 0;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean tryAcquireAdvisoryLock(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select pg_try_advisory_lock(?)")) {
            statement.setLong(1, advisoryLockKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getBoolean(1);
            }
        }
    }

    private boolean unlockAdvisoryLock(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select pg_advisory_unlock(?)")) {
            statement.setLong(1, advisoryLockKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getBoolean(1);
            }
        }
    }

    private boolean shutdownExecutor(ExecutorService executor, String name) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Executor {} did not terminate after shutdown; forcing shutdownNow()", name);
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Executor {} did not terminate after forced shutdown", name);
                    return false;
                }
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
            log.warn("Interrupted while shutting down executor {}; forced shutdownNow()", name);
            return false;
        }
    }

    private void ensureCompactionControlConsistent(boolean shouldRequeueClaimedRequest) {
        try {
            jdbcTemplate.update("""
                    insert into compaction_control (id, requested)
                    values (1, false)
                    on conflict (id) do nothing
                    """);
            Boolean requested = jdbcTemplate.queryForObject(
                    "select requested from compaction_control where id = 1",
                    Boolean.class
            );
            if (shouldRequeueClaimedRequest && Boolean.FALSE.equals(requested)) {
                int updated = jdbcTemplate.update("update compaction_control set requested = true where id = 1 and requested = false");
                if (updated > 0) {
                    log.info("Re-queued claimed compaction request during shutdown consistency check");
                }
            }
            log.debug("Compaction control consistency check finished");
        } catch (Exception e) {
            log.warn("Failed to verify compaction control consistency during shutdown: {}", e.toString(), e);
        }
    }
}
