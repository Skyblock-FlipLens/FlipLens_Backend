package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.AhItemSnapshotEntity;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

@Repository
public class AhItemSnapshotBatchRepositoryImpl implements AhItemSnapshotBatchRepository {

    private static final String INSERT_IGNORE_SQL = """
            insert into ah_item_snapshot (
                snapshot_ts,
                item_key,
                bin_lowest,
                bin_lowest5_mean,
                bin_p50,
                bin_p95,
                bin_count,
                bid_p50,
                ending_soon_count,
                created_at_epoch_millis
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (snapshot_ts, item_key) do nothing
            """;

    private final JdbcTemplate jdbcTemplate;

    public AhItemSnapshotBatchRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public int[] insertIgnoreBatch(List<AhItemSnapshotEntity> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return new int[0];
        }
        return jdbcTemplate.batchUpdate(INSERT_IGNORE_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                AhItemSnapshotEntity snapshot = snapshots.get(i);
                ps.setLong(1, snapshot.getSnapshotTs());
                ps.setString(2, snapshot.getItemKey());
                ps.setObject(3, snapshot.getBinLowest(), Types.BIGINT);
                ps.setObject(4, snapshot.getBinLowest5Mean(), Types.BIGINT);
                ps.setObject(5, snapshot.getBinP50(), Types.BIGINT);
                ps.setObject(6, snapshot.getBinP95(), Types.BIGINT);
                ps.setInt(7, snapshot.getBinCount());
                ps.setObject(8, snapshot.getBidP50(), Types.BIGINT);
                ps.setInt(9, snapshot.getEndingSoonCount());
                ps.setLong(10, snapshot.getCreatedAtEpochMillis());
            }

            @Override
            public int getBatchSize() {
                return snapshots.size();
            }
        });
    }
}
