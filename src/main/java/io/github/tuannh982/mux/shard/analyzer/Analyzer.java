package io.github.tuannh982.mux.shard.analyzer;

import io.github.tuannh982.mux.commons.tuple.Tuple2;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.statements.history.PreparedStatementMethodInvocation;

import java.util.Map;

public interface Analyzer {
    Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyze(String sql, boolean isPrepared, PreparedStatementMethodInvocation values, ShardOps shardOps);
}
