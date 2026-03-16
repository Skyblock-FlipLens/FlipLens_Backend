package com.skyblockflipper.backend.service.market.partitioning;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Locale;

@Service
public class AggregateRollupService {

    private final JdbcTemplate jdbcTemplate;

    public AggregateRollupService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void rollupDailyForTable(String parentTableName, LocalDate dayUtc) {
        if (parentTableName == null || dayUtc == null) {
            return;
        }
        String normalized = parentTableName.trim().toLowerCase(Locale.ROOT);
        if ("bz_item_snapshot".equals(normalized)) {
            rollupBzDay(dayUtc);
            return;
        }
        if ("ah_item_snapshot".equals(normalized)) {
            rollupAhDay(dayUtc);
        }
    }

    private void rollupBzDay(LocalDate dayUtc) {
        long fromEpochMillis = UtcDayBucket.startEpochMillis(dayUtc);
        long toEpochMillis = UtcDayBucket.endEpochMillis(dayUtc);
        long nowEpochMillis = System.currentTimeMillis();

        jdbcTemplate.update("""
                insert into bz_item_snapshot_daily_rollup (
                    bucket_day_utc,
                    product_id,
                    avg_buy_price,
                    avg_sell_price,
                    avg_spread,
                    avg_buy_volume,
                    avg_sell_volume,
                    sample_count,
                    first_snapshot_ts,
                    last_snapshot_ts,
                    created_at_epoch_millis,
                    updated_at_epoch_millis
                )
                select
                    ?::date as bucket_day_utc,
                    s.product_id,
                    avg(s.buy_price) as avg_buy_price,
                    avg(s.sell_price) as avg_sell_price,
                    avg(case
                        when s.buy_price is null or s.sell_price is null then null
                        else s.buy_price - s.sell_price
                    end) as avg_spread,
                    avg(s.buy_volume) as avg_buy_volume,
                    avg(s.sell_volume) as avg_sell_volume,
                    count(*) as sample_count,
                    min(s.snapshot_ts) as first_snapshot_ts,
                    max(s.snapshot_ts) as last_snapshot_ts,
                    ? as created_at_epoch_millis,
                    ? as updated_at_epoch_millis
                from bz_item_snapshot s
                where s.snapshot_ts >= ?
                  and s.snapshot_ts < ?
                group by s.product_id
                on conflict (bucket_day_utc, product_id) do update
                set avg_buy_price = excluded.avg_buy_price,
                    avg_sell_price = excluded.avg_sell_price,
                    avg_spread = excluded.avg_spread,
                    avg_buy_volume = excluded.avg_buy_volume,
                    avg_sell_volume = excluded.avg_sell_volume,
                    sample_count = excluded.sample_count,
                    first_snapshot_ts = excluded.first_snapshot_ts,
                    last_snapshot_ts = excluded.last_snapshot_ts,
                    updated_at_epoch_millis = excluded.updated_at_epoch_millis
                """,
                dayUtc.toString(),
                nowEpochMillis,
                nowEpochMillis,
                fromEpochMillis,
                toEpochMillis
        );
    }

    private void rollupAhDay(LocalDate dayUtc) {
        long fromEpochMillis = UtcDayBucket.startEpochMillis(dayUtc);
        long toEpochMillis = UtcDayBucket.endEpochMillis(dayUtc);
        long nowEpochMillis = System.currentTimeMillis();

        jdbcTemplate.update("""
                insert into ah_item_snapshot_daily_rollup (
                    bucket_day_utc,
                    item_key,
                    avg_bin_lowest,
                    avg_bin_lowest5_mean,
                    avg_bin_p50,
                    avg_bin_p95,
                    avg_bid_p50,
                    avg_bin_count,
                    avg_ending_soon_count,
                    sample_count,
                    first_snapshot_ts,
                    last_snapshot_ts,
                    created_at_epoch_millis,
                    updated_at_epoch_millis
                )
                select
                    ?::date as bucket_day_utc,
                    s.item_key,
                    avg(s.bin_lowest) as avg_bin_lowest,
                    avg(s.bin_lowest5_mean) as avg_bin_lowest5_mean,
                    avg(s.bin_p50) as avg_bin_p50,
                    avg(s.bin_p95) as avg_bin_p95,
                    avg(s.bid_p50) as avg_bid_p50,
                    avg(s.bin_count) as avg_bin_count,
                    avg(s.ending_soon_count) as avg_ending_soon_count,
                    count(*) as sample_count,
                    min(s.snapshot_ts) as first_snapshot_ts,
                    max(s.snapshot_ts) as last_snapshot_ts,
                    ? as created_at_epoch_millis,
                    ? as updated_at_epoch_millis
                from ah_item_snapshot s
                where s.snapshot_ts >= ?
                  and s.snapshot_ts < ?
                group by s.item_key
                on conflict (bucket_day_utc, item_key) do update
                set avg_bin_lowest = excluded.avg_bin_lowest,
                    avg_bin_lowest5_mean = excluded.avg_bin_lowest5_mean,
                    avg_bin_p50 = excluded.avg_bin_p50,
                    avg_bin_p95 = excluded.avg_bin_p95,
                    avg_bid_p50 = excluded.avg_bid_p50,
                    avg_bin_count = excluded.avg_bin_count,
                    avg_ending_soon_count = excluded.avg_ending_soon_count,
                    sample_count = excluded.sample_count,
                    first_snapshot_ts = excluded.first_snapshot_ts,
                    last_snapshot_ts = excluded.last_snapshot_ts,
                    updated_at_epoch_millis = excluded.updated_at_epoch_millis
                """,
                dayUtc.toString(),
                nowEpochMillis,
                nowEpochMillis,
                fromEpochMillis,
                toEpochMillis
        );
    }
}
