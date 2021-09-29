package io.github.tuannh982.mux.statements.invocation;

import lombok.Getter;

import java.sql.SQLException;

@Getter
public class MethodInvocationEntry<M extends MethodInvocation> {
    private final M method;
    private final Object[] params;

    public MethodInvocationEntry(M method, Object[] params) throws SQLException {
        this.method = method;
        if (params.length != method.getNumberOfArgs()) {
            throw new SQLException("params length mismatched");
        }
        this.params = params;
    }
}
