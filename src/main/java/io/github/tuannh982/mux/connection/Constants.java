package io.github.tuannh982.mux.connection;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {
    public static final String OPERATION_NOT_SUPPORTED = "Operation is not supported";
    public static final String SQL_PARSER_EXCEPTION = "SQL parser exception";
    public static final String SQL_PARSER_NO_STATEMENTS_FOUND = "SQL parser exception: no statement found";
    public static final String SQL_PARSER_MULTIPLE_STATEMENTS_NOT_ALLOWED = "SQL parser exception: multiple statement is not allowed";
    public static final String UNINITIALIZED_VARIABLE = "Uninitialized variable: ";
}
