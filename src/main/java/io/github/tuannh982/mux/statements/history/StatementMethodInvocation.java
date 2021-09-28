package io.github.tuannh982.mux.statements.history;

import io.github.tuannh982.mux.statements.MuxStatementMethodInvocation;
import lombok.Getter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class StatementMethodInvocation {
    private final Map<MuxStatementMethodInvocation, Object[]> state;

    public StatementMethodInvocation() {
        this.state = new HashMap<>();
    }

    public void clear() {
        this.state.clear();
    }

    public List<MethodInvocationEntry<MuxStatementMethodInvocation>> playbackList() throws SQLException {
        List<MethodInvocationEntry<MuxStatementMethodInvocation>> stateAsList = new ArrayList<>();
        for (Map.Entry<MuxStatementMethodInvocation, Object[]> entry : state.entrySet()) {
            stateAsList.add(new MethodInvocationEntry<>(entry.getKey(), entry.getValue()));
        }
        return stateAsList;
    }
}
