package io.github.tuannh982.mux.urlparser;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Properties;

@Getter
@AllArgsConstructor
public class ParsedUrl {
    private final String configServerAddress;
    private final String configKeyId;
    private final String database;
    private final Properties properties;
}
