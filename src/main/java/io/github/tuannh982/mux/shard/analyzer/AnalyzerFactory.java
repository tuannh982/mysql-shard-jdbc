package io.github.tuannh982.mux.shard.analyzer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AnalyzerFactory {
    public static Analyzer defaultAnalyzer() {
        // TODO
        return null;
    }

    public static Analyzer noop() {
        return new NoopAnalyzer();
    }
}
