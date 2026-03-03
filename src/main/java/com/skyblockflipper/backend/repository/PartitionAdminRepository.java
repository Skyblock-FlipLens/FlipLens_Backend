package com.skyblockflipper.backend.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Locale;

@Repository
public class PartitionAdminRepository {

    private final JdbcTemplate jdbcTemplate;

    public PartitionAdminRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean isTablePartitioned(String schemaName, String parentTableName) {
        String schema = sanitizeIdentifier(schemaName);
        String parent = sanitizeIdentifier(parentTableName);
        Boolean partitioned = jdbcTemplate.queryForObject("""
                select exists (
                    select 1
                    from pg_partitioned_table p
                    join pg_class c on c.oid = p.partrelid
                    join pg_namespace n on n.oid = c.relnamespace
                    where n.nspname = ?
                      and c.relname = ?
                )
                """, Boolean.class, schema, parent);
        return Boolean.TRUE.equals(partitioned);
    }

    public List<String> listChildPartitions(String schemaName, String parentTableName) {
        String schema = sanitizeIdentifier(schemaName);
        String parent = sanitizeIdentifier(parentTableName);
        return jdbcTemplate.query("""
                select c.relname
                from pg_inherits i
                join pg_class c on c.oid = i.inhrelid
                join pg_class p on p.oid = i.inhparent
                join pg_namespace n on n.oid = p.relnamespace
                where n.nspname = ?
                  and p.relname = ?
                order by c.relname asc
                """,
                (rs, rowNum) -> rs.getString(1),
                schema,
                parent
        );
    }

    public void ensureDailyRangePartition(String schemaName,
                                          String parentTableName,
                                          String partitionTableName,
                                          long fromInclusiveEpochMillis,
                                          long toExclusiveEpochMillis) {
        String schema = sanitizeIdentifier(schemaName);
        String parent = sanitizeIdentifier(parentTableName);
        String partition = sanitizeIdentifier(partitionTableName);
        jdbcTemplate.execute("create table if not exists "
                + q(schema) + "." + q(partition)
                + " partition of " + q(schema) + "." + q(parent)
                + " for values from (" + fromInclusiveEpochMillis + ") to (" + toExclusiveEpochMillis + ")");
    }

    public void dropTableIfExists(String schemaName, String tableName) {
        String schema = sanitizeIdentifier(schemaName);
        String table = sanitizeIdentifier(tableName);
        jdbcTemplate.execute("drop table if exists " + q(schema) + "." + q(table));
    }

    private String sanitizeIdentifier(String identifier) {
        if (identifier == null) {
            throw new IllegalArgumentException("identifier must not be null");
        }
        String trimmed = identifier.trim();
        if (!trimmed.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + identifier);
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    private String q(String identifier) {
        return "\"" + identifier + "\"";
    }
}
