package io.github.tuannh982.mux.statements.history;

import lombok.Getter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class ParamInvocationHistory<M extends MethodInvocation> {
    private final Map<M, Object[]> state;
    private final List<ParamInvocationEntry<M>> history;

    public ParamInvocationHistory() {
        this.state = new HashMap<>();
        this.history = new ArrayList<>();
    }

    public void clear() {
        this.state.clear();
        this.history.clear();
    }

    public List<ParamInvocationEntry<M>> extractStateAsList() throws SQLException {
        List<ParamInvocationEntry<M>> stateAsList = new ArrayList<>();
        for (Map.Entry<M, Object[]> entry : state.entrySet()) {
            stateAsList.add(new ParamInvocationEntry<>(entry.getKey(), entry.getValue()));
        }
        return stateAsList;
    }
}
