package io.github.tuannh982.mux.urlparser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.regex.Pattern;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {
    public static final String URL_PREFIX = "jdbc:mux:";
    public static final Pattern URL_PATTERN = Pattern.compile("jdbc:mux:\\/\\/\\((.*)\\)\\[([A-Za-z0-9]+)\\]\\/([A-Za-z0-9]+)(.*)", Pattern.DOTALL);
}
