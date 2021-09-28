package io.github.tuannh982.mux.shard.analyzer;

import java.util.HashMap;
import java.util.Map;

public class NoopAnalyzer implements Analyzer {
    @Override
    public Map<Integer, String> analyze(String sql, boolean isPrepared, Map<?, ?> values) {
        Map<Integer, String> ret = new HashMap<>();
        ret.put(0, sql);
        return ret;
    }
}
