package io.github.tuannh982.mux.analyzer;

import java.util.Map;

public interface Analyzer {
    Map<Integer, String> analyze(String sql, boolean isPrepared, Map<?, ?> values);
}
