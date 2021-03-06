package io.github.tuannh982.mux.shard.analyzer;

import io.github.tuannh982.mux.commons.tuple.Tuple2;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.statements.invocation.PreparedStatementMethodInvocation;

import java.sql.SQLException;
import java.util.Map;

public interface Analyzer {
    Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyze(
            String schema,
            String sql,
            PreparedStatementMethodInvocation preparedMethodInvocation,
            ShardOps shardOps
    ) throws SQLException;
}
