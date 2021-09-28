package io.github.tuannh982.mux.shard.shardops;

import io.github.tuannh982.mux.config.ShardConfig;
import lombok.Getter;

import java.util.Map;
import java.util.TreeMap;

@Getter
public class ShardOps {
    public static final int DEFAULT_SHARD_NODE = 0;

    private final int physNodeCount;
    private final ShardConfig.Range[] physNodeShardRanges;
    private final Hash hash;
    // for range search
    private final TreeMap<Long, Integer> invertedSortedMapIndex;

    public ShardOps(int physNodeCount, ShardConfig.Range[] physNodeShardRanges, Hash hash) {
        this.physNodeCount = physNodeCount;
        this.physNodeShardRanges = physNodeShardRanges;
        this.hash = hash;
        this.invertedSortedMapIndex = new TreeMap<>();
        for (int i = 0; i < this.physNodeCount; i++) {
            ShardConfig.Range range = this.physNodeShardRanges[i];
            this.invertedSortedMapIndex.put(range.getFrom(), i);
        }
    }

    public int apply(Object shardingKey) {
        long hashValue = hash.hash(shardingKey);
        Map.Entry<Long, Integer> entry = invertedSortedMapIndex.floorEntry(hashValue);
        ShardConfig.Range foundRange = physNodeShardRanges[entry.getValue()];
        if (hashValue >= foundRange.getFrom() && hashValue < foundRange.getTo()) {
            return entry.getValue();
        } else {
            return DEFAULT_SHARD_NODE;
        }
    }
}