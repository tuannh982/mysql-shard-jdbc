package io.github.tuannh982.mux.statements.history;

import lombok.Getter;

import java.sql.SQLException;

@Getter
public class ParamInvocationEntry<M extends MethodInvocation> {
    private final M method;
    private final Object[] params;

    public ParamInvocationEntry(M method, Object[] params) throws SQLException {
        this.method = method;
        if (params.length != method.getNumberOfArgs()) {
            throw new SQLException("params length mismatched");
        }
        this.params = params;
    }
}
