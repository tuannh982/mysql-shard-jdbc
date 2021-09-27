package io.github.tuannh982.mux.urlparser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.regex.Pattern;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {
    public static final String URL_PREFIX = "jdbc:mux:";
    // <example> jdbc:mux://(127.0.0.1:2379,123:4323)[instance1]/testDB?characterEncoding=UTF-8&sessionVariables=sql_mode=ANSI_QUOTES&rewriteBatchedStatements=true
    public static final Pattern URL_PATTERN = Pattern.compile("jdbc:mux:\\/\\/\\((.*)\\)\\[([A-Za-z0-9]+)\\]\\/([A-Za-z0-9]+)(.*)", Pattern.DOTALL);
}
