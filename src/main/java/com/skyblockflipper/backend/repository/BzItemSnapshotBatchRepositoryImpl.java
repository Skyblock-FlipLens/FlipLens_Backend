package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

@Repository
public class BzItemSnapshotBatchRepositoryImpl implements BzItemSnapshotBatchRepository {

    private static final String INSERT_IGNORE_SQL = """
            insert into bz_item_snapshot (
                snapshot_ts,
                product_id,
                buy_price,
                sell_price,
                buy_volume,
                sell_volume,
                created_at_epoch_millis
            ) values (?, ?, ?, ?, ?, ?, ?)
            on conflict (snapshot_ts, product_id) do nothing
            """;

    private final JdbcTemplate jdbcTemplate;

    public BzItemSnapshotBatchRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public int[] insertIgnoreBatch(List<BzItemSnapshotEntity> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return new int[0];
        }
        return jdbcTemplate.batchUpdate(INSERT_IGNORE_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                BzItemSnapshotEntity snapshot = snapshots.get(i);
                ps.setLong(1, snapshot.getSnapshotTs());
                ps.setString(2, snapshot.getProductId());
                ps.setObject(3, snapshot.getBuyPrice(), Types.DOUBLE);
                ps.setObject(4, snapshot.getSellPrice(), Types.DOUBLE);
                ps.setObject(5, snapshot.getBuyVolume(), Types.BIGINT);
                ps.setObject(6, snapshot.getSellVolume(), Types.BIGINT);
                ps.setLong(7, snapshot.getCreatedAtEpochMillis());
            }

            @Override
            public int getBatchSize() {
                return snapshots.size();
            }
        });
    }
}
