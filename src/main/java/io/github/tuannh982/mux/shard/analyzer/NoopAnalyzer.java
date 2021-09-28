package io.github.tuannh982.mux.shard.analyzer;

import io.github.tuannh982.mux.commons.tuple.Tuple2;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.statements.history.PreparedStatementMethodInvocationState;

import java.util.HashMap;
import java.util.Map;

public class NoopAnalyzer implements Analyzer {
    @Override
    public Map<Integer, Tuple2<String, PreparedStatementMethodInvocationState>> analyze(String sql, boolean isPrepared, PreparedStatementMethodInvocationState values, ShardOps shardOps) {
        Map<Integer, Tuple2<String, PreparedStatementMethodInvocationState>> ret = new HashMap<>();
        ret.put(0, Tuple2.of(sql, values));
        return ret;
    }
}
