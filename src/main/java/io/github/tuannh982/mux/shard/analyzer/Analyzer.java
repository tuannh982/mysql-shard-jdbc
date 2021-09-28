package io.github.tuannh982.mux.shard.analyzer;

import io.github.tuannh982.mux.shard.shardops.ShardOps;

import java.util.Map;

public interface Analyzer {
    Map<Integer, String> analyze(String sql, boolean isPrepared, Map<?, ?> values, ShardOps shardOps);
}
