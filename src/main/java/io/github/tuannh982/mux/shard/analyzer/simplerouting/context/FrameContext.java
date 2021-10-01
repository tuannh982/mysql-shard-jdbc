package io.github.tuannh982.mux.shard.analyzer.simplerouting.context;

import io.github.tuannh982.mux.shard.analyzer.simplerouting.SQLExceptionRTE;
import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.*;

import static io.github.tuannh982.mux.shard.analyzer.ErrorMessages.*;

public class FrameContext {
    @Getter
    private final int currentStackDepth;
    @Getter
    private final StatementType statementType;
    private final Map<String, TableContext> tableContextMap;
    private final Map<String, TableContext> tableAliasContextMap;
    private boolean neg = false;

    public FrameContext(int currentStackDepth, StatementType statementType) {
        this.currentStackDepth = currentStackDepth;
        this.statementType = statementType;
        this.tableContextMap = new HashMap<>();
        this.tableAliasContextMap = new HashMap<>();
    }

    public void flip() {
        neg = !neg;
    }

    public void putTableContext(Table table, String schema, TableContext tableContext) {
        String fullTableName = TableContextUtils.getFullTableName(table, schema, false);
        String tableAlias = TableContextUtils.getTableAlias(table);
        boolean a = tableContextMap.containsKey(fullTableName);
        boolean b = tableAliasContextMap.containsKey(tableAlias);
        if (a == !b) {
            throw new SQLExceptionRTE(SQL_PARSER_AMBIGUOUS_TABLE_CONTEXT);
        } else {
            tableContextMap.put(fullTableName, tableContext);
            tableAliasContextMap.put(tableAlias, tableContext);
        }
    }

    public TableContext getTableContext(Table table, String schema) {
        String fullTableName = TableContextUtils.getFullTableName(table, schema, false);
        if (tableContextMap.containsKey(fullTableName)) {
            return tableContextMap.get(fullTableName);
        }
        String tableAlias = TableContextUtils.getTableAlias(table);
        if (tableAliasContextMap.containsKey(tableAlias)) {
            tableAliasContextMap.get(tableAlias);
        }
        return null;
    }

    public void addExpressionToTableContext(Column column, String schema, ExpressionType expressionType, Expression... expressions) {
        Table table = column.getTable();
        TableContext tableContext = getTableContext(table, schema);
        List<Parameter> parameters = new ArrayList<>(expressions.length);
        for (Expression expression : expressions) {
            parameters.add(new Parameter(expression));
        }
        ParameterExpression parameterExpression = new ParameterExpression(
                expressionType,
                neg,
                column,
                parameters.toArray(new Parameter[0])
        );
        tableContext.getParameterExpressions().add(parameterExpression);
    }

    public List<TableContext> collectTables() {
        List<TableContext> ret = new ArrayList<>();
        for (Map.Entry<String, TableContext> entry : tableContextMap.entrySet()) {
            TableContext tableCtx = entry.getValue();
            ret.add(tableCtx);
        }
        return ret;
    }
}
