package io.github.tuannh982.mux.statements.history;

import io.github.tuannh982.mux.statements.MuxPreparedStatementMethodInvocation;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class PreparedStatementMethodInvocationState {
    private final Map<Integer, MethodInvocationEntry<MuxPreparedStatementMethodInvocation>> state;
    private final Map<Integer, byte[]> stateAsByteArray;

    public PreparedStatementMethodInvocationState() {
        this.state = new HashMap<>();
        this.stateAsByteArray = new HashMap<>();
    }

    public void clear() {
        this.state.clear();
        this.stateAsByteArray.clear();
    }
}
