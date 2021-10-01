package io.github.tuannh982.mux.config;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class ShardConfig {
    @ToString
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class JdbcConfig {
        private final String driverClass;
        private final String jdbcTemplate;
    }

    @ToString
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class Range {
        private final long from;
        private final long to;
    }

    @ToString
    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TableConfig {
        private final String schema;
        private final String table;
        private final String column;
    }

    private final long version;
    private final int physNodeCount;
    private final JdbcConfig[] physNodeJdbcConfigs;
    private final Range[] physNodeShardRanges;
    private final TableConfig[] tableShardConfigs;
}
