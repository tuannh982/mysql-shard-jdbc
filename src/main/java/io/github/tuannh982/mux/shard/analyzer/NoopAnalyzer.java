package io.github.tuannh982.mux.shard.analyzer;

import io.github.tuannh982.mux.shard.shardops.ShardOps;

import java.util.HashMap;
import java.util.Map;

public class NoopAnalyzer implements Analyzer {
    @Override
    public Map<Integer, String> analyze(String sql, boolean isPrepared, Map<?, ?> values, ShardOps shardOps) {
        Map<Integer, String> ret = new HashMap<>();
        ret.put(0, sql);
        return ret;
    }
}
