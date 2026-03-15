package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.market.BzItemSnapshotEntity;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;
import java.util.function.Consumer;
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

    private static final String SCAN_BUCKET_SQL = """
            select
                snapshot_ts,
                product_id,
                buy_price,
                sell_price,
                buy_volume,
                sell_volume,
                created_at_epoch_millis
            from bz_item_snapshot
            where snapshot_ts >= ?
              and snapshot_ts < ?
            order by product_id asc, snapshot_ts asc
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

    @Override
    public void scanBucketRows(long fromInclusive, long toExclusive, int fetchSize, Consumer<BzItemSnapshotEntity> consumer) {
        Objects.requireNonNull(consumer, "consumer must not be null");
        jdbcTemplate.query(connection -> {
            PreparedStatement ps = connection.prepareStatement(SCAN_BUCKET_SQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setLong(1, fromInclusive);
            ps.setLong(2, toExclusive);
            ps.setFetchSize(Math.max(1, fetchSize));
            return ps;
        }, (RowCallbackHandler) rs -> consumer.accept(mapRow(rs)));
    }

    private BzItemSnapshotEntity mapRow(ResultSet rs) throws SQLException {
        return new BzItemSnapshotEntity(
                rs.getLong("snapshot_ts"),
                rs.getString("product_id"),
                rs.getObject("buy_price", Double.class),
                rs.getObject("sell_price", Double.class),
                rs.getObject("buy_volume", Long.class),
                rs.getObject("sell_volume", Long.class)
        );
    }
}
