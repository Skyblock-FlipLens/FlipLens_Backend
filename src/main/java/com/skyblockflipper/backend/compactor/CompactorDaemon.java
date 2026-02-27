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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.SQLTimeoutException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Profile("compactor")
@Slf4j
public class CompactorDaemon implements SmartLifecycle {

    private static final long CONTROL_ROW_RETRY_INITIAL_BACKOFF_MILLIS = 1_000L;
    private static final long CONTROL_ROW_RETRY_MAX_BACKOFF_MILLIS = 60_000L;

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final MarketDataProcessingService marketDataProcessingService;
    private final String channel;
    private final long safetyTickIntervalMillis;
    private final long listenPollIntervalMillis;
    private final long listenReconnectDelayMillis;
    private final long advisoryLockKey;
    private final ObjectMapper objectMapper;
    private volatile HttpClient apiReadinessHttpClient;
    private volatile long apiReadinessHttpClientConnectTimeoutMillis = -1L;

    private ExecutorService listenExecutor;
    private ScheduledExecutorService safetyTickExecutor;
    private ScheduledExecutorService adaptiveDecisionExecutor;
    private final AtomicBoolean claimedCompactionInProgress = new AtomicBoolean(false);
    private volatile boolean running;
    private volatile boolean controlRowReady;
    private volatile long nextControlRowEnsureAttemptEpochMillis;
    private volatile long controlRowEnsureBackoffMillis = CONTROL_ROW_RETRY_INITIAL_BACKOFF_MILLIS;

    @Value("${config.compaction.scheduler.enabled:true}")
    private boolean adaptiveSchedulerEnabled = true;
    @Value("${config.compaction.scheduler.decision-interval-seconds:30}")
    private long decisionIntervalSeconds = 30L;
    @Value("${config.compaction.scheduler.min-interval-seconds:30}")
    private long minIntervalSeconds = 30L;
    @Value("${config.compaction.scheduler.fallback-interval-seconds:60}")
    private long fallbackIntervalSeconds = 60L;
    @Value("${config.compaction.scheduler.api-readiness.enabled:true}")
    private boolean apiReadinessEnabled = true;
    @Value("${config.compaction.scheduler.api-readiness.url:http://api:1881/actuator/compactionReadiness}")
    private String apiReadinessUrl = "http://api:1881/actuator/compactionReadiness";
    @Value("${config.compaction.scheduler.api-readiness.allowed-hosts:api,skyblockflipper-api,localhost,127.0.0.1,::1}")
    private String apiReadinessAllowedHosts = "api,skyblockflipper-api,localhost,127.0.0.1,::1";
    @Value("${config.compaction.scheduler.api-readiness.connect-timeout-ms:500}")
    private long apiReadinessConnectTimeoutMs = 500L;
    @Value("${config.compaction.scheduler.api-readiness.read-timeout-ms:1000}")
    private long apiReadinessReadTimeoutMs = 1000L;
    @Value("${config.compaction.scheduler.db-readiness.enabled:true}")
    private boolean dbReadinessEnabled = true;
    @Value("${config.compaction.scheduler.db-readiness.max-active-sessions:2}")
    private long dbReadinessMaxActiveSessions = 2L;
    @Value("${config.compaction.scheduler.db-readiness.max-waiting-sessions:1}")
    private long dbReadinessMaxWaitingSessions = 1L;
    @Value("${config.compaction.scheduler.db-readiness.max-lock-waits:0}")
    private long dbReadinessMaxLockWaits = 0L;

    public CompactorDaemon(DataSource dataSource,
                           JdbcTemplate jdbcTemplate,
                           MarketDataProcessingService marketDataProcessingService,
                           ObjectMapper objectMapper,
                           @Value("${config.compactor.channel:compaction}") String channel,
                           @Value("${config.compactor.safety-tick-interval-ms:60000}") long safetyTickIntervalMillis,
                           @Value("${config.compactor.listen-poll-interval-ms:500}") long listenPollIntervalMillis,
                           @Value("${config.compactor.listen-reconnect-delay-ms:2000}") long listenReconnectDelayMillis,
                           @Value("${config.compactor.advisory-lock-key:912345678}") long advisoryLockKey) {
        this.dataSource = dataSource;
        this.jdbcTemplate = jdbcTemplate;
        this.marketDataProcessingService = marketDataProcessingService;
        this.objectMapper = objectMapper;
        this.channel = sanitizeChannel(channel);
        this.safetyTickIntervalMillis = Math.max(30_000L, safetyTickIntervalMillis);
        this.listenPollIntervalMillis = Math.max(100L, listenPollIntervalMillis);
        this.listenReconnectDelayMillis = Math.max(500L, listenReconnectDelayMillis);
        this.advisoryLockKey = advisoryLockKey;
        this.listenExecutor = createListenExecutor();
        this.safetyTickExecutor = createSafetyTickExecutor();
        this.adaptiveDecisionExecutor = createAdaptiveDecisionExecutor();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        ensureExecutorsReady();
        controlRowReady = false;
        nextControlRowEnsureAttemptEpochMillis = 0L;
        controlRowEnsureBackoffMillis = CONTROL_ROW_RETRY_INITIAL_BACKOFF_MILLIS;
        running = true;
        if (!ensureControlRowReady("startup")) {
            log.warn("Compactor startup deferred control-row creation; background workers will skip work until schema is ready.");
        }
        try {
            submitCompactorTasks();
        } catch (RejectedExecutionException e) {
            running = false;
            log.warn("Failed to start compactor executors; recreating executors and retrying start once: {}", e.toString());
            ensureExecutorsReady(true);
            running = true;
            submitCompactorTasks();
        }
        log.info("CompactorDaemon started (channel={}, safetyTickMs={}, listenPollMs={}, advisoryLockKey={})",
                channel,
                safetyTickIntervalMillis,
                listenPollIntervalMillis,
                advisoryLockKey);
    }

    @Override
    public synchronized void stop() {
        running = false;
        shutdownExecutor(listenExecutor, "compactor-listen-loop");
        boolean safetyTickTerminated = shutdownExecutor(safetyTickExecutor, "compactor-safety-tick");
        shutdownExecutor(adaptiveDecisionExecutor, "compactor-adaptive-decision");
        listenExecutor = null;
        safetyTickExecutor = null;
        adaptiveDecisionExecutor = null;
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
                    int notificationCount;
                    if (pgConnection != null) {
                        notificationCount = drainNotifications(pgConnection, listenPollIntervalMillis);
                    } else {
                        statement.execute("select 1");
                        notificationCount = 0;
                        sleepQuietly(listenPollIntervalMillis);
                    }
                    if (notificationCount > 0) {
                        log.info("Received {} notifications on channel {}. Triggering compaction check.",
                                notificationCount,
                                channel);
                        tryRunIfRequestedSafely();
                    }
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
        if (!ensureControlRowReady("try_run_if_requested")) {
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

    private void runAdaptiveDecisionSafely() {
        try {
            runAdaptiveDecision();
        } catch (Exception e) {
            log.warn("Adaptive compaction decision loop failed: {}", e.toString(), e);
        }
    }

    private void runAdaptiveDecision() {
        if (!running || !adaptiveSchedulerEnabled) {
            return;
        }
        if (!ensureControlRowReady("adaptive_decision")) {
            return;
        }

        CompactionControlState controlState = readCompactionControlState();
        if (controlState == null) {
            return;
        }
        if (controlState.requested()) {
            log.debug("Adaptive decision skipped: compaction request already pending.");
            return;
        }

        Instant now = Instant.now();
        Duration sinceLastRun = controlState.lastRunAt() == null ? null : Duration.between(controlState.lastRunAt(), now);
        long sinceLastRunSeconds = sinceLastRun == null ? -1L : sinceLastRun.toSeconds();
        boolean fallbackDue = controlState.lastRunAt() == null
                || sinceLastRunSeconds >= Math.max(1L, fallbackIntervalSeconds);
        if (!fallbackDue && sinceLastRunSeconds >= 0L && sinceLastRunSeconds < Math.max(1L, minIntervalSeconds)) {
            log.debug("Adaptive decision skipped: min interval not reached yet (sinceLastRunSeconds={}).", sinceLastRunSeconds);
            return;
        }

        DbReadiness dbReadiness = probeDbReadiness();
        if (!fallbackDue && !dbReadiness.ready()) {
            log.info("Adaptive decision skipped: db not ready (active={}, waiting={}, lockWaits={}, reason={}).",
                    dbReadiness.activeSessions(),
                    dbReadiness.waitingSessions(),
                    dbReadiness.lockWaits(),
                    dbReadiness.reason());
            return;
        }

        ApiReadiness apiReadiness = probeApiReadiness();
        boolean apiAllows = apiReadiness.state() == ApiReadinessState.READY
                || apiReadiness.state() == ApiReadinessState.UNKNOWN && dbReadiness.strongReady();
        if (!fallbackDue && !apiAllows) {
            log.info("Adaptive decision skipped: api readiness denied compaction (state={}, reason={}).",
                    apiReadiness.state(),
                    apiReadiness.reason());
            return;
        }

        String reason = fallbackDue
                ? "fallback"
                : apiReadiness.state() == ApiReadinessState.UNKNOWN ? "low_load_db_only" : "low_load";
        requestCompaction(reason, dbReadiness, apiReadiness);
    }

    private void ensureControlRowExists() {
        jdbcTemplate.update("""
                insert into compaction_control (id, requested)
                values (1, false)
                on conflict (id) do nothing
                """);
    }

    private boolean ensureControlRowReady(String trigger) {
        if (controlRowReady) {
            return true;
        }
        long now = System.currentTimeMillis();
        if (now < nextControlRowEnsureAttemptEpochMillis) {
            return false;
        }
        synchronized (this) {
            if (controlRowReady) {
                return true;
            }
            now = System.currentTimeMillis();
            if (now < nextControlRowEnsureAttemptEpochMillis) {
                return false;
            }
            try {
                ensureControlRowExists();
                controlRowReady = true;
                nextControlRowEnsureAttemptEpochMillis = 0L;
                controlRowEnsureBackoffMillis = CONTROL_ROW_RETRY_INITIAL_BACKOFF_MILLIS;
                log.info("Compaction control row is ready (trigger={})", trigger);
                return true;
            } catch (Exception e) {
                long backoffMillis = Math.max(CONTROL_ROW_RETRY_INITIAL_BACKOFF_MILLIS, controlRowEnsureBackoffMillis);
                nextControlRowEnsureAttemptEpochMillis = now + backoffMillis;
                controlRowEnsureBackoffMillis = Math.min(CONTROL_ROW_RETRY_MAX_BACKOFF_MILLIS, backoffMillis * 2L);
                log.warn("Compaction control row unavailable (trigger={}); postponing work and retrying in {} ms: {}",
                        trigger,
                        backoffMillis,
                        e.toString());
                return false;
            }
        }
    }

    private CompactionControlState readCompactionControlState() {
        return jdbcTemplate.query("""
                select requested, last_run_at
                from compaction_control
                where id = 1
                """, rs -> {
            if (!rs.next()) {
                return null;
            }
            Timestamp lastRunTimestamp = rs.getTimestamp("last_run_at");
            Instant lastRunAt = lastRunTimestamp == null ? null : lastRunTimestamp.toInstant();
            return new CompactionControlState(rs.getBoolean("requested"), lastRunAt);
        });
    }

    private DbReadiness probeDbReadiness() {
        if (!dbReadinessEnabled) {
            return new DbReadiness(true, true, -1L, -1L, -1L, "db_readiness_disabled");
        }
        return jdbcTemplate.query("""
                select
                  count(*) filter (where state = 'active') as active,
                  count(*) filter (
                    where wait_event_type is not null
                      and wait_event_type not in ('ClientRead', 'ClientWrite')
                  ) as waiting,
                  count(*) filter (where wait_event_type = 'Lock') as lock_waits
                from pg_stat_activity
                where datname = current_database()
                  and pid <> pg_backend_pid()
                """, rs -> {
            if (!rs.next()) {
                return new DbReadiness(false, false, -1L, -1L, -1L, "db_readiness_no_data");
            }
            long active = rs.getLong("active");
            long waiting = rs.getLong("waiting");
            long lockWaits = rs.getLong("lock_waits");
            boolean ready = active <= Math.max(0L, dbReadinessMaxActiveSessions)
                    && waiting <= Math.max(0L, dbReadinessMaxWaitingSessions)
                    && lockWaits <= Math.max(0L, dbReadinessMaxLockWaits);
            boolean strong = lockWaits == 0L && waiting == 0L && active <= 1L;
            String reason = ready ? "db_ready" : "db_busy";
            return new DbReadiness(ready, strong, active, waiting, lockWaits, reason);
        });
    }

    private ApiReadiness probeApiReadiness() {
        if (!apiReadinessEnabled) {
            return new ApiReadiness(ApiReadinessState.UNKNOWN, "api_readiness_disabled");
        }
        if (apiReadinessUrl == null || apiReadinessUrl.isBlank()) {
            return new ApiReadiness(ApiReadinessState.UNKNOWN, "api_readiness_url_missing");
        }
        URI apiReadinessUri = parseAndValidateApiReadinessUri(apiReadinessUrl);
        if (apiReadinessUri == null) {
            return new ApiReadiness(ApiReadinessState.UNKNOWN, "api_readiness_url_blocked");
        }
        try {
            long readTimeoutMillis = Math.max(100L, apiReadinessReadTimeoutMs);
            HttpRequest request = HttpRequest.newBuilder(apiReadinessUri)
                    .GET()
                    .timeout(Duration.ofMillis(readTimeoutMillis))
                    .header("Accept", "application/json")
                    .build();
            HttpResponse<String> response = getApiReadinessHttpClient()
                    .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                return new ApiReadiness(ApiReadinessState.UNKNOWN, "api_http_" + response.statusCode());
            }
            JsonNode node = objectMapper.readTree(response.body());
            boolean ok = node.path("ok").asBoolean(false);
            String reason = node.path("reason").asString(ok ? "low_load" : "not_ready");
            return ok
                    ? new ApiReadiness(ApiReadinessState.READY, reason)
                    : new ApiReadiness(ApiReadinessState.NOT_READY, reason);
        } catch (Exception e) {
            return new ApiReadiness(ApiReadinessState.UNKNOWN, "api_unreachable");
        }
    }

    private void requestCompaction(String reason, DbReadiness dbReadiness, ApiReadiness apiReadiness) {
        int updated = jdbcTemplate.update("""
                update compaction_control
                set requested = true,
                    requested_at = now(),
                    requested_by = ?
                where id = 1 and requested = false
                """, "compactor:" + reason);
        if (updated <= 0) {
            log.debug("Adaptive decision skipped: request already pending.");
            return;
        }
        jdbcTemplate.execute("notify " + channel + ", 'run'");
        log.info("Adaptive compaction requested (reason={}, dbActive={}, dbWaiting={}, dbLockWaits={}, apiState={}, apiReason={})",
                reason,
                dbReadiness.activeSessions(),
                dbReadiness.waitingSessions(),
                dbReadiness.lockWaits(),
                apiReadiness.state(),
                apiReadiness.reason());
    }

    private URI parseAndValidateApiReadinessUri(String rawUrl) {
        URI uri;
        try {
            uri = URI.create(rawUrl.trim());
        } catch (Exception e) {
            return null;
        }

        String scheme = uri.getScheme();
        if (scheme == null) {
            return null;
        }
        String schemeLower = scheme.toLowerCase(Locale.ROOT);
        if (!"http".equals(schemeLower) && !"https".equals(schemeLower)) {
            return null;
        }

        if (uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            return null;
        }

        String host = uri.getHost();
        if (host == null || host.isBlank() || !isAllowedReadinessHost(host)) {
            return null;
        }
        return uri;
    }

    private boolean isAllowedReadinessHost(String host) {
        String normalizedHost = host.trim().toLowerCase(Locale.ROOT);
        if (normalizedHost.isBlank()) {
            return false;
        }
        String[] configuredHosts = apiReadinessAllowedHosts.split(",");
        for (String configuredHost : configuredHosts) {
            String normalizedAllowed = configuredHost.trim().toLowerCase(Locale.ROOT);
            if (!normalizedAllowed.isBlank() && normalizedAllowed.equals(normalizedHost)) {
                return true;
            }
        }
        return false;
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

    private int drainNotifications(PGConnection pgConnection, long timeoutMillis) {
        if (pgConnection == null) {
            return 0;
        }
        try {
            int waitMillis = (int) Math.max(0L, Math.min(Integer.MAX_VALUE, timeoutMillis));
            PGNotification[] notifications = pgConnection.getNotifications(waitMillis);
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
        if (executor == null) {
            return true;
        }
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

    private void ensureExecutorsReady() {
        ensureExecutorsReady(false);
    }

    private void ensureExecutorsReady(boolean forceRecreate) {
        if (forceRecreate || listenExecutor == null || listenExecutor.isShutdown() || listenExecutor.isTerminated()) {
            listenExecutor = createListenExecutor();
        }
        if (forceRecreate || safetyTickExecutor == null || safetyTickExecutor.isShutdown() || safetyTickExecutor.isTerminated()) {
            safetyTickExecutor = createSafetyTickExecutor();
        }
        if (forceRecreate || adaptiveDecisionExecutor == null || adaptiveDecisionExecutor.isShutdown() || adaptiveDecisionExecutor.isTerminated()) {
            adaptiveDecisionExecutor = createAdaptiveDecisionExecutor();
        }
    }

    private void submitCompactorTasks() {
        listenExecutor.submit(this::listenLoop);
        safetyTickExecutor.scheduleWithFixedDelay(
                this::tryRunIfRequestedSafely,
                30_000L,
                safetyTickIntervalMillis,
                TimeUnit.MILLISECONDS
        );
        if (adaptiveSchedulerEnabled) {
            long decisionIntervalMillis = Math.max(10_000L, decisionIntervalSeconds * 1_000L);
            adaptiveDecisionExecutor.scheduleWithFixedDelay(
                    this::runAdaptiveDecisionSafely,
                    decisionIntervalMillis,
                    decisionIntervalMillis,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private ExecutorService createListenExecutor() {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "compactor-listen-loop");
            t.setDaemon(true);
            return t;
        });
    }

    private ScheduledExecutorService createSafetyTickExecutor() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "compactor-safety-tick");
            t.setDaemon(true);
            return t;
        });
    }

    private ScheduledExecutorService createAdaptiveDecisionExecutor() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "compactor-adaptive-decision");
            t.setDaemon(true);
            return t;
        });
    }

    private HttpClient getApiReadinessHttpClient() {
        long connectTimeoutMillis = Math.max(100L, apiReadinessConnectTimeoutMs);
        HttpClient existing = apiReadinessHttpClient;
        if (existing != null && connectTimeoutMillis == apiReadinessHttpClientConnectTimeoutMillis) {
            return existing;
        }
        return createApiReadinessHttpClient(connectTimeoutMillis);
    }

    private synchronized HttpClient createApiReadinessHttpClient(long connectTimeoutMillis) {
        if (apiReadinessHttpClient != null && connectTimeoutMillis == apiReadinessHttpClientConnectTimeoutMillis) {
            return apiReadinessHttpClient;
        }
        apiReadinessHttpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMillis))
                .build();
        apiReadinessHttpClientConnectTimeoutMillis = connectTimeoutMillis;
        return apiReadinessHttpClient;
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

    private record CompactionControlState(boolean requested, Instant lastRunAt) {
    }

    private record DbReadiness(boolean ready,
                               boolean strongReady,
                               long activeSessions,
                               long waitingSessions,
                               long lockWaits,
                               String reason) {
    }

    private record ApiReadiness(ApiReadinessState state, String reason) {
    }

    private enum ApiReadinessState {
        READY,
        NOT_READY,
        UNKNOWN
    }
}
