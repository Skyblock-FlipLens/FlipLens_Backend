package com.skyblockflipper.backend.compactor.diagnostics;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class CompactorDiagnosticsDto {

    private CompactorDiagnosticsDto() {
    }

    public record Snapshot(Instant timestampUtc,
                           ApiHealth apiHealth,
                           Map<String, Long> dbWaitSummary,
                           List<LongRunningQuery> topLongQueries,
                           List<StatementStat> topStatements,
                           boolean pgStatStatementsAvailable,
                           List<VacuumTableStat> vacuumHotTables,
                           Double cacheHitRatio,
                           List<String> errors) {
    }

    public record ApiHealth(String status,
                            Integer httpStatus,
                            Map<String, Object> details,
                            String error) {
    }

    public record LongRunningQuery(long pid,
                                   String state,
                                   String waitEventType,
                                   String waitEvent,
                                   String runningFor,
                                   String query) {
    }

    public record StatementStat(long calls,
                                Double meanMs,
                                Double totalMs,
                                long rows,
                                String query) {
    }

    public record VacuumTableStat(String relname,
                                  long liveTuples,
                                  long deadTuples,
                                  String lastAutovacuum,
                                  String lastVacuum,
                                  long autovacuumCount,
                                  long vacuumCount) {
    }
}
