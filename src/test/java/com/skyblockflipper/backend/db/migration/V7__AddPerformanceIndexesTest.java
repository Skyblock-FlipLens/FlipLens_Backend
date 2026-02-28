package com.skyblockflipper.backend.db.migration;

import org.flywaydb.core.api.migration.Context;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class V7__AddPerformanceIndexesTest {

    @Test
    void migrationIsNonTransactional() {
        assertFalse(new V7__AddPerformanceIndexes().canExecuteInTransaction());
    }

    @Test
    void migrationCreatesAllTargetIndexes() throws Exception {
        Context context = mock(Context.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);

        when(context.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);

        new V7__AddPerformanceIndexes().migrate(context);

        verify(connection).setAutoCommit(true);
        verify(statement).execute("SET lock_timeout TO '5s'");
        verify(statement).execute("SET statement_timeout TO '30min'");
        verify(statement).execute("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_flip_snapshot_ts
                    ON public.flip (snapshot_timestamp_epoch_millis)
                    """);
        verify(statement).execute("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_flip_step_flip_id
                    ON public.flip_step (flip_id)
                    """);
        verify(statement).execute("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_flip_constraints_flip_id
                    ON public.flip_constraints (flip_id)
                    """);
    }
}
