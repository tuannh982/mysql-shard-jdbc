package io.github.tuannh982.mux.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ShardConfigStoreFactory {
    public static ShardConfigStore getShardConfig(
            String configServerAddress,
            String configKeyId,
            String username,
            String password
    ) {
        // TODO
        return null;
    }
}
