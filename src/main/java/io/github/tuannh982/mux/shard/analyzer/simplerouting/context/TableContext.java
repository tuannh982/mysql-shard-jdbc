package io.github.tuannh982.mux.shard.analyzer.simplerouting.context;

import lombok.AllArgsConstructor;
import lombok.Getter;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.List;

@Getter
public class TableContext {
    private final Table table;
    private final List<ParameterExpression> parameterExpressions;

    public TableContext(Table table) {
        this.table = table;
        this.parameterExpressions = new ArrayList<>();
    }
}
