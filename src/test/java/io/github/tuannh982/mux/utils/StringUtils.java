package io.github.tuannh982.mux.utils;

public class StringUtils {
    public static String collapseWhitespace(String s) {
        return s == null ? null : s.replaceAll("^ +| +$|( )+", "$1");
    }
}
