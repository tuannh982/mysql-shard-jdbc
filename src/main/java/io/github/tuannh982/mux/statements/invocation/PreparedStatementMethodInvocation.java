package io.github.tuannh982.mux.statements.invocation;

import io.github.tuannh982.mux.statements.MuxPreparedStatementMethodInvocation;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class PreparedStatementMethodInvocation {
    private final Map<Integer, MethodInvocationEntry<MuxPreparedStatementMethodInvocation>> state;
    private final Map<Integer, byte[]> valueMap;

    public PreparedStatementMethodInvocation() {
        this.state = new HashMap<>();
        this.valueMap = new HashMap<>();
    }

    public void clear() {
        this.state.clear();
        this.valueMap.clear();
    }
}
