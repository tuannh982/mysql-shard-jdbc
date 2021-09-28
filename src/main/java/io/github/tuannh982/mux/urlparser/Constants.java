package io.github.tuannh982.mux.urlparser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.regex.Pattern;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {
    public static final String URL_PREFIX = "jdbc:mux:";
    public static final Pattern URL_PATTERN = Pattern.compile("jdbc:mux:\\/\\/\\((.*)\\)\\[([0-9a-zA-Z$_]+)\\]\\/([0-9a-zA-Z$_]+)(.*)", Pattern.DOTALL);
}
