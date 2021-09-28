package io.github.tuannh982.mux.shard.shardops;

public interface Hash {
    long hash(Object o);
    long hash(byte[] array);
}
