package io.github.tuannh982.mux.statements.history;

import io.github.tuannh982.mux.statements.MuxPreparedStatementMethodInvocation;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class PreparedStatementMethodInvocation {
    private final Map<Integer, MethodInvocationEntry<MuxPreparedStatementMethodInvocation>> state;
    private final Map<Integer, byte[]> brState;

    public PreparedStatementMethodInvocation() {
        this.state = new HashMap<>();
        this.brState = new HashMap<>();
    }

    public void clear() {
        this.state.clear();
        this.brState.clear();
    }
}
