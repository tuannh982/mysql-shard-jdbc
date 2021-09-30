package io.github.tuannh982.mux.shard.analyzer;

import io.github.tuannh982.mux.shard.analyzer.simplerouting.SimpleRoutingAnalyzer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AnalyzerFactory {
    public static Analyzer defaultAnalyzer() {
        return new SimpleRoutingAnalyzer();
    }

    public static Analyzer noop() {
        return new NoopAnalyzer();
    }
}
