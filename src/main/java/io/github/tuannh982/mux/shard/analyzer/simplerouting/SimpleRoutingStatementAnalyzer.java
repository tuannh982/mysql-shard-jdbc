package io.github.tuannh982.mux.shard.analyzer.simplerouting;

import io.github.tuannh982.mux.config.ShardConfig;
import io.github.tuannh982.mux.shard.analyzer.simplerouting.context.*;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import lombok.AccessLevel;
import lombok.Getter;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.sql.SQLException;
import java.util.*;

import static io.github.tuannh982.mux.ErrorMessages.OPERATION_NOT_SUPPORTED;

@Getter
public class SimpleRoutingStatementAnalyzer {
    private final Deque<FrameContext> stack;
    private final List<TableContext> tableContexts;
    private final String schema;
    private final Statement statement;
    private final ShardOps shardOps;
    @Getter(AccessLevel.NONE)
    private final SimpleRoutingStatementVisitor visitor;

    public SimpleRoutingStatementAnalyzer(String schema, Statement statement, ShardOps shardOps) {
        this.stack = new LinkedList<>();
        this.tableContexts = new ArrayList<>();
        this.schema = schema;
        this.statement = statement;
        this.shardOps = shardOps;
        this.visitor = new SimpleRoutingStatementVisitor(this);
    }

    public boolean containsJoin() {
        return visitor.containsJoin();
    }

    public boolean containsSubQuery() {
        return visitor.containsSubQuery();
    }

    public boolean usingValues() {
        return visitor.usingValues();
    }

    public void analyze() throws SQLException {
        try {
            statement.accept(visitor);
        } catch (SQLExceptionRTE e) {
            throw e.getInner();
        }
    }

    public int stackDepth() {
        return stack.size();
    }

    public void fillingParameters(Map<Integer, byte[]> valueMap) { /* must be called after analyze() */
        for (TableContext tableContext : tableContexts) {
            for (ParameterExpression parameterExpression : tableContext.getParameterExpressions()) {
                for (Parameter parameter : parameterExpression.getParameters()) {
                    if (parameter.isPreparedParameter()) {
                        int parameterIndex = parameter.getIndex();
                        parameter.updateValue(valueMap.get(parameterIndex));
                    }
                }
            }
        }
    }

    private boolean isShardedColumn(Column column) {
        Table table = column.getTable();
        ShardConfig.TableConfig search = new ShardConfig.TableConfig(
                TableContextUtils.extractSchema(table, getSchema()).toLowerCase(Locale.ROOT),
                table.getName().toLowerCase(Locale.ROOT),
                column.getName(false).toLowerCase(Locale.ROOT)
        );
        return shardOps.getTableShardConfigs().contains(search);
    }

    @SuppressWarnings("java:S3776")
    public Set<Integer> extractInvolvedShards() throws SQLException { /* must be called after analyze() */
        Set<Integer> ret = new HashSet<>();
        for (TableContext tableContext : tableContexts) {
            for (ParameterExpression parameterExpression : tableContext.getParameterExpressions()) {
                Column column = parameterExpression.getColumn();
                if (isShardedColumn(column)) {
                    if (
                            (parameterExpression.getExpressionType() == ExpressionType.COMPARISON_EQUAL && !parameterExpression.isNot()) ||
                            parameterExpression.getExpressionType() == ExpressionType.ASSIGNMENT
                    ) {
                        Parameter[] parameters = parameterExpression.getParameters();
                        if (parameters.length == 1) {
                            ret.add(shardOps.apply(parameters[0].getValue()));
                        } else {
                            // TODO support later
                            throw new SQLException(OPERATION_NOT_SUPPORTED);
                        }
                    } else {
                        throw new SQLException(OPERATION_NOT_SUPPORTED);
                    }
                }
            }
        }
        return ret;
    }
}
