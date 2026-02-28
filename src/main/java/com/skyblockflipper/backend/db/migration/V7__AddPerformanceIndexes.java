package com.skyblockflipper.backend.db.migration;

import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class V7__AddPerformanceIndexes extends BaseJavaMigration {

    @Override
    public boolean canExecuteInTransaction() {
        return false;
    }

    @Override
    public void migrate(Context context) throws Exception {
        var connection = context.getConnection();
        boolean previousAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(true);
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET lock_timeout TO '5s'");
            statement.execute("SET statement_timeout TO '30min'");
            createIndex(statement, "idx_flip_snapshot_ts", """
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_flip_snapshot_ts
                    ON public.flip (snapshot_timestamp_epoch_millis)
                    """);
            createIndex(statement, "idx_flip_step_flip_id", """
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_flip_step_flip_id
                    ON public.flip_step (flip_id)
                    """);
            createIndex(statement, "idx_flip_constraints_flip_id", """
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_flip_constraints_flip_id
                    ON public.flip_constraints (flip_id)
                    """);
        }
        finally {
            log.warn("Migration failed - cleaning up...");
            Statement statement = connection.createStatement();
            statement.execute("RESET lock_timeout");
            statement.execute("RESET statement_timeout");
            connection.setAutoCommit(previousAutoCommit);
            statement.close();
            log.warn("Migration failed - cleaning completed!");
        }
    }

    private void createIndex(Statement statement, String indexName, String sql) throws SQLException {
        try {
            statement.execute(sql);
            log.info("Ensured index {}", indexName);
        } catch (SQLException e) {
            if (isMissingFlipId(e)) {
                throw new SQLException("Missing required column flip_id while creating " + indexName, e);
            }
            throw e;
        }
    }

    private boolean isMissingFlipId(SQLException exception) {
        String message = exception.getMessage();
        return message != null
                && message.toLowerCase().contains("flip_id")
                && message.toLowerCase().contains("does not exist");
    }
}
