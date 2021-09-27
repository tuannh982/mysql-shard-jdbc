package io.github.tuannh982.mux.config;

public interface ShardConfigStore {
    long version();
    ShardConfig fetchConfig();
}
