package com.skyblockflipper.backend.compactor.diagnostics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Component
@Profile("compactor")
@RequiredArgsConstructor
@Slf4j
public class CompactorDiagnosticsService implements SmartLifecycle {

    private static final String SQL_TOP_RUNNING = """
            SELECT pid, state, wait_event_type, wait_event, now()-query_start AS running_for, left(query,300) AS query
            FROM pg_stat_activity
            WHERE datname = current_database() AND state <> 'idle'
            ORDER BY running_for DESC
            LIMIT 20
            """;

    private static final String SQL_LOCKS = """
            SELECT l.locktype, l.mode, l.granted, a.pid, now()-a.query_start AS running_for, left(a.query,200) AS query
            FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
            WHERE a.datname = current_database()
            ORDER BY l.granted, running_for DESC
            """;

    private static final String SQL_VACUUM = """
            SELECT relname, n_live_tup, n_dead_tup, last_autovacuum, last_vacuum, autovacuum_count, vacuum_count
            FROM pg_stat_user_tables
            ORDER BY n_dead_tup DESC
            LIMIT 20
            """;

    private static final String SQL_CACHE_HIT = """
            SELECT blks_read, blks_hit,
                   round(100.0*blks_hit/nullif(blks_hit+blks_read,0),2) AS hit_ratio_pct
            FROM pg_stat_database
            WHERE datname = current_database()
            """;

    private static final String SQL_TOP_STATEMENTS = """
            SELECT calls, round(mean_exec_time::numeric,2) AS mean_ms, round(total_exec_time::numeric,2) AS total_ms, rows, left(query,200) AS query
            FROM pg_stat_statements
            ORDER BY total_exec_time DESC
            LIMIT 20
            """;

    private final DataSource dataSource;
    private final CompactorDiagnosticsProperties properties;
    private final ObjectMapper objectMapper;
    private final AtomicReference<CompactorDiagnosticsDto.Snapshot> lastSnapshot = new AtomicReference<>();

    private volatile boolean running;
    private volatile HttpClient apiClient;
    private ScheduledExecutorService scheduler;

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (!properties.isEnabled()) {
            log.info("Compactor diagnostics disabled");
            return;
        }
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "compactor-diagnostics");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMillis = Math.max(1_000L, properties.getInterval().toMillis());
        scheduler.scheduleWithFixedDelay(this::runDiagnosticsSafely, 0L, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("Compactor diagnostics started (intervalMs={}, dbEnabled={})",
                intervalMillis,
                properties.getDb().isEnabled());
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        log.info("Compactor diagnostics stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 10;
    }

    public CompactorDiagnosticsDto.Snapshot getLastSnapshot() {
        return lastSnapshot.get();
    }

    private void runDiagnosticsSafely() {
        if (!running || !properties.isEnabled()) {
            return;
        }
        try {
            CompactorDiagnosticsDto.Snapshot snapshot = collectSnapshot();
            lastSnapshot.set(snapshot);
            log.info("diagnostics_summary={}", objectMapper.writeValueAsString(snapshot));
        } catch (Exception e) {
            log.warn("Compactor diagnostics probe failed: {}", e, e);
        }
    }

    CompactorDiagnosticsDto.Snapshot collectSnapshot() {
        Instant now = Instant.now();
        List<String> errors = new ArrayList<>();

        CompactorDiagnosticsDto.ApiHealth apiHealth = probeApiHealth(errors);

        Map<String, Long> dbWaitSummary = new LinkedHashMap<>();
        List<CompactorDiagnosticsDto.LongRunningQuery> topLongQueries = List.of();
        List<CompactorDiagnosticsDto.StatementStat> topStatements = List.of();
        boolean pgStatStatementsAvailable = false;
        List<CompactorDiagnosticsDto.VacuumTableStat> vacuumHotTables = List.of();
        Double cacheHitRatio = null;

        if (properties.getDb().isEnabled()) {
            try (Connection connection = dataSource.getConnection()) {
                connection.setReadOnly(true);
                String previousStatementTimeout = readCurrentStatementTimeout(connection);
                applyStatementTimeout(connection);
                try {
                    topLongQueries = readTopLongQueries(connection, dbWaitSummary, errors);
                    readLocks(connection, dbWaitSummary, errors);
                    vacuumHotTables = readVacuumStats(connection, errors);
                    cacheHitRatio = readCacheHitRatio(connection, errors);
                    TopStatementsResult topStatementsResult = readTopStatements(connection, errors);
                    topStatements = topStatementsResult.statements();
                    pgStatStatementsAvailable = topStatementsResult.available();
                } finally {
                    restoreStatementTimeout(connection, previousStatementTimeout, errors);
                }
            } catch (Exception e) {
                errors.add("db_probe_failed:" + summarize(e));
            }
        }

        return new CompactorDiagnosticsDto.Snapshot(
                now,
                apiHealth,
                dbWaitSummary,
                topLongQueries,
                topStatements,
                pgStatStatementsAvailable,
                vacuumHotTables,
                cacheHitRatio,
                errors
        );
    }

    private CompactorDiagnosticsDto.ApiHealth probeApiHealth(List<String> errors) {
        String baseUrl = properties.getApi().getBaseUrl();
        if (baseUrl == null || baseUrl.isBlank()) {
            errors.add("api_probe_failed:missing_base_url");
            return new CompactorDiagnosticsDto.ApiHealth("DOWN", null, Map.of(), "missing_base_url");
        }
        try {
            URI uri = URI.create(trimTrailingSlash(baseUrl) + "/actuator/health");
            Duration readTimeout = safeDuration(properties.getApi().getReadTimeout(), Duration.ofSeconds(2));
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .GET()
                    .timeout(readTimeout)
                    .header("Accept", "application/json")
                    .build();
            HttpResponse<String> response = getApiClient()
                    .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            JsonNode node = objectMapper.readTree(response.body());
            String status = node.path("status").asText("UNKNOWN");
            Map<String, Object> details = node.has("details")
                    ? objectMapper.convertValue(node.get("details"),
                        objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class))
                    : Collections.emptyMap();
            boolean healthy = response.statusCode() >= 200
                    && response.statusCode() < 300
                    && "UP".equalsIgnoreCase(status);
            return new CompactorDiagnosticsDto.ApiHealth(
                    healthy ? "UP" : "DOWN",
                    response.statusCode(),
                    details,
                    healthy ? null : "health_status_" + status
            );
        } catch (Exception e) {
            errors.add("api_probe_failed:" + summarize(e));
            return new CompactorDiagnosticsDto.ApiHealth("DOWN", null, Map.of(), summarize(e));
        }
    }

    private void applyStatementTimeout(Connection connection) throws SQLException {
        long timeoutMillis = Math.max(100L, properties.getDb().getStatementTimeout().toMillis());
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET statement_timeout TO '" + timeoutMillis + "ms'");
        }
    }

    private String readCurrentStatementTimeout(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW statement_timeout")) {
            return resultSet.next() ? resultSet.getString(1) : null;
        }
    }

    private void restoreStatementTimeout(Connection connection, String previousStatementTimeout, List<String> errors) {
        try (Statement statement = connection.createStatement()) {
            if (previousStatementTimeout == null || previousStatementTimeout.isBlank()) {
                statement.execute("RESET statement_timeout");
            } else {
                String escaped = previousStatementTimeout.replace("'", "''");
                statement.execute("SET statement_timeout TO '" + escaped + "'");
            }
        } catch (Exception e) {
            errors.add("db_restore_statement_timeout_failed:" + summarize(e));
        }
    }

    private List<CompactorDiagnosticsDto.LongRunningQuery> readTopLongQueries(Connection connection,
                                                                              Map<String, Long> waitSummary,
                                                                              List<String> errors) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_TOP_RUNNING)) {
            List<CompactorDiagnosticsDto.LongRunningQuery> result = new ArrayList<>();
            while (resultSet.next()) {
                String waitEventType = resultSet.getString("wait_event_type");
                String waitEvent = resultSet.getString("wait_event");
                String waitKey = (waitEvent != null && !waitEvent.isBlank())
                        ? waitEvent
                        : (waitEventType != null && !waitEventType.isBlank() ? waitEventType : "none");
                waitSummary.merge(waitKey, 1L, Long::sum);

                result.add(new CompactorDiagnosticsDto.LongRunningQuery(
                        resultSet.getLong("pid"),
                        resultSet.getString("state"),
                        waitEventType,
                        waitEvent,
                        resultSet.getString("running_for"),
                        resultSet.getString("query")
                ));
            }
            return limit5(result);
        } catch (Exception e) {
            errors.add("db_top_running_failed:" + summarize(e));
            return List.of();
        }
    }

    private void readLocks(Connection connection, Map<String, Long> waitSummary, List<String> errors) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_LOCKS)) {
            while (resultSet.next()) {
                boolean granted = resultSet.getBoolean("granted");
                String key = granted ? "lock_granted" : "lock_waiting";
                waitSummary.merge(key, 1L, Long::sum);
            }
        } catch (Exception e) {
            errors.add("db_locks_failed:" + summarize(e));
        }
    }

    private List<CompactorDiagnosticsDto.VacuumTableStat> readVacuumStats(Connection connection, List<String> errors) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_VACUUM)) {
            List<CompactorDiagnosticsDto.VacuumTableStat> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(new CompactorDiagnosticsDto.VacuumTableStat(
                        resultSet.getString("relname"),
                        resultSet.getLong("n_live_tup"),
                        resultSet.getLong("n_dead_tup"),
                        resultSet.getString("last_autovacuum"),
                        resultSet.getString("last_vacuum"),
                        resultSet.getLong("autovacuum_count"),
                        resultSet.getLong("vacuum_count")
                ));
            }
            return limit5(result);
        } catch (Exception e) {
            errors.add("db_vacuum_failed:" + summarize(e));
            return List.of();
        }
    }

    private Double readCacheHitRatio(Connection connection, List<String> errors) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_CACHE_HIT)) {
            if (!resultSet.next()) {
                return null;
            }
            double ratio = resultSet.getDouble("hit_ratio_pct");
            if (resultSet.wasNull() || !Double.isFinite(ratio)) {
                return null;
            }
            return ratio;
        } catch (Exception e) {
            errors.add("db_cache_hit_failed:" + summarize(e));
            return null;
        }
    }

    private TopStatementsResult readTopStatements(Connection connection, List<String> errors) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_TOP_STATEMENTS)) {
            List<CompactorDiagnosticsDto.StatementStat> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(new CompactorDiagnosticsDto.StatementStat(
                        resultSet.getLong("calls"),
                        readNullableDouble(resultSet, "mean_ms"),
                        readNullableDouble(resultSet, "total_ms"),
                        resultSet.getLong("rows"),
                        resultSet.getString("query")
                ));
            }
            return new TopStatementsResult(true, limit5(result));
        } catch (SQLException e) {
            if (isPgStatStatementsMissing(e)) {
                log.info("pg_stat_statements not available");
                return new TopStatementsResult(false, List.of());
            }
            errors.add("db_top_statements_failed:" + summarize(e));
            return new TopStatementsResult(false, List.of());
        } catch (Exception e) {
            errors.add("db_top_statements_failed:" + summarize(e));
            return new TopStatementsResult(false, List.of());
        }
    }

    private boolean isPgStatStatementsMissing(SQLException exception) {
        if (exception.getSQLState() != null && "42P01".equals(exception.getSQLState())) {
            return true;
        }
        String message = exception.getMessage();
        return message != null && message.toLowerCase().contains("pg_stat_statements");
    }

    private Double readNullableDouble(ResultSet resultSet, String column) throws SQLException {
        double value = resultSet.getDouble(column);
        return resultSet.wasNull() ? null : value;
    }

    private <T> List<T> limit5(List<T> input) {
        if (input.size() <= 5) {
            return input;
        }
        return input.subList(0, 5);
    }

    private HttpClient getApiClient() {
        HttpClient existing = apiClient;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (apiClient != null) {
                return apiClient;
            }
            Duration connectTimeout = safeDuration(properties.getApi().getConnectTimeout(), Duration.ofSeconds(2));
            apiClient = HttpClient.newBuilder()
                    .connectTimeout(connectTimeout)
                    .build();
            return apiClient;
        }
    }

    private Duration safeDuration(Duration candidate, Duration fallback) {
        if (candidate == null || candidate.isNegative() || candidate.isZero()) {
            return fallback;
        }
        return candidate;
    }

    private String trimTrailingSlash(String value) {
        String result = value.trim();
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private String summarize(Exception e) {
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            return e.getClass().getSimpleName();
        }
        return message.length() > 160 ? message.substring(0, 160) : message;
    }

    private record TopStatementsResult(boolean available, List<CompactorDiagnosticsDto.StatementStat> statements) {
    }
}
