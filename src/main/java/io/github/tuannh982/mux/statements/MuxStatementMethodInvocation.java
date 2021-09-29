package io.github.tuannh982.mux.statements;

import io.github.tuannh982.mux.statements.invocation.MethodInvocation;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum MuxStatementMethodInvocation implements MethodInvocation {
    MAX_FIELD_SIZE(1),
    MAX_ROWS(1),
    ESCAPE_PROCESSING(1),
    QUERY_TIMEOUT(1),
    CURSOR_NAME(1),
    FETCH_DIRECTION(1),
    FETCH_SIZE(1),
    CLOSE_ON_COMPLETION(1),
    POOLABLE(1);

    private final int numberOfArgs;
}
