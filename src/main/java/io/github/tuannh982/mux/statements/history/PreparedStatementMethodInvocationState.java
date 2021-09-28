package io.github.tuannh982.mux.statements.history;

import io.github.tuannh982.mux.statements.MuxPreparedStatementMethodInvocation;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class PreparedStatementMethodInvocationState {
    private final Map<Integer, MethodInvocationEntry<MuxPreparedStatementMethodInvocation>> state;

    public PreparedStatementMethodInvocationState() {
        this.state = new HashMap<>();
    }

    public void clear() {
        this.state.clear();
    }

    public Map<Integer, byte[]> convertToByteArrays() {
        // TODO
        return null;
    }
}
