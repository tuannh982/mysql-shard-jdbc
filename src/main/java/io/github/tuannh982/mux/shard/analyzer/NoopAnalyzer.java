package io.github.tuannh982.mux.shard.analyzer;

import io.github.tuannh982.mux.commons.tuple.Tuple2;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.statements.invocation.PreparedStatementMethodInvocation;

import java.util.HashMap;
import java.util.Map;

public class NoopAnalyzer implements Analyzer {
    @Override
    public Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyze(
            String schema,
            String sql,
            PreparedStatementMethodInvocation preparedMethodInvocation,
            ShardOps shardOps
    ) {
        Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> ret = new HashMap<>();
        // directly route to the first shard
        ret.put(0, Tuple2.of(sql, preparedMethodInvocation));
        return ret;
    }
}
