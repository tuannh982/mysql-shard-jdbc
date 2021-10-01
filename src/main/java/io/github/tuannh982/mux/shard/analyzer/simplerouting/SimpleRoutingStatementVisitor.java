package io.github.tuannh982.mux.shard.analyzer.simplerouting;

import io.github.tuannh982.mux.shard.analyzer.simplerouting.context.ExpressionType;
import io.github.tuannh982.mux.shard.analyzer.simplerouting.context.FrameContext;
import io.github.tuannh982.mux.shard.analyzer.simplerouting.context.StatementType;
import io.github.tuannh982.mux.shard.analyzer.simplerouting.context.TableContext;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterSession;
import net.sf.jsqlparser.statement.alter.AlterSystemStatement;
import net.sf.jsqlparser.statement.alter.RenameTableStatement;
import net.sf.jsqlparser.statement.alter.sequence.AlterSequence;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.schema.CreateSchema;
import net.sf.jsqlparser.statement.create.sequence.CreateSequence;
import net.sf.jsqlparser.statement.create.synonym.CreateSynonym;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.grant.Grant;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.util.List;

import static io.github.tuannh982.mux.ErrorMessages.OPERATION_NOT_SUPPORTED;
import static io.github.tuannh982.mux.shard.analyzer.ErrorMessages.SQL_PARSER_EMPTY_STACK_FRAME;

@SuppressWarnings("DuplicatedCode")
public class SimpleRoutingStatementVisitor implements
        SelectVisitor, FromItemVisitor, ExpressionVisitor,
        ItemsListVisitor, SelectItemVisitor, StatementVisitor
{
    private final SimpleRoutingStatementAnalyzer analyzer;
    private boolean containsJoin = false;
    private boolean containsSubQuery = false;
    private boolean usingValue = false;

    public boolean containsJoin() {
        return containsJoin;
    }

    public boolean containsSubQuery() {
        return containsSubQuery;
    }

    public boolean usingValue() {
        return usingValue;
    }

    public SimpleRoutingStatementVisitor(SimpleRoutingStatementAnalyzer analyzer) {
        this.analyzer = analyzer;
    }

    private void visitBinaryExpression(BinaryExpression expression) {
        Expression leftExpr = expression.getLeftExpression();
        if (leftExpr instanceof Column) {
            Column column = (Column) leftExpr;
            FrameContext ctx = analyzer.getStack().peek();
            if (ctx == null) {
                throw new SQLExceptionRTE(SQL_PARSER_EMPTY_STACK_FRAME);
            }
            TableContext tableCtx = ctx.getTableContext(column.getTable(), analyzer.getSchema());
            if (tableCtx != null) {
                if (expression instanceof EqualsTo) {
                    ctx.addExpressionToTableContext(column, analyzer.getSchema(), ExpressionType.COMPARISON_EQUAL, expression.getRightExpression());
                } else if (
                        expression instanceof SimilarToExpression       ||
                        expression instanceof LikeExpression            ||
                        expression instanceof RegExpMySQLOperator       ||
                        expression instanceof RegExpMatchOperator
                ) {
                    ctx.addExpressionToTableContext(column, analyzer.getSchema(), ExpressionType.SIMILARITY, expression.getRightExpression());
                }
            } else {
                expression.getLeftExpression().accept(this);
                expression.getRightExpression().accept(this);
            }
        } else {
            expression.getLeftExpression().accept(this);
            expression.getRightExpression().accept(this);
        }
    }

    private void visitColumnsAssignmentExpression(List<Column> columns, ExpressionList expressionList) {
        List<Expression> valueList = expressionList.getExpressions();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            FrameContext ctx = analyzer.getStack().peek();
            if (ctx == null) {
                throw new SQLExceptionRTE(SQL_PARSER_EMPTY_STACK_FRAME);
            }
            ctx.addExpressionToTableContext(column, analyzer.getSchema(), ExpressionType.ASSIGNMENT, valueList.get(i));
        }
    }

    //-----

    @Override
    public void visit(BitwiseRightShift bitwiseRightShift) {
        visitBinaryExpression(bitwiseRightShift);
    }

    @Override
    public void visit(BitwiseLeftShift bitwiseLeftShift) {
        visitBinaryExpression(bitwiseLeftShift);
    }

    @Override
    public void visit(NullValue nullValue) {
        /* ignored */
    }

    @Override
    public void visit(Function function) {
        ExpressionList parameters = function.getParameters();
        if (parameters != null) {
            visit(parameters);
        }
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        signedExpression.getExpression().accept(this);
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        /* ignored */
        /*
         * will be handled later
         */
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        // will not be supported
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        /* ignored */
    }

    @Override
    public void visit(LongValue longValue) {
        /* ignored */
    }

    @Override
    public void visit(HexValue hexValue) {
        /* ignored */
    }

    @Override
    public void visit(DateValue dateValue) {
        /* ignored */
    }

    @Override
    public void visit(TimeValue timeValue) {
        /* ignored */
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        /* ignored */
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
        /* ignored */
    }

    @Override
    public void visit(Addition addition) {
        visitBinaryExpression(addition);
    }

    @Override
    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(IntegerDivision integerDivision) {
        visitBinaryExpression(integerDivision);
    }

    @Override
    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    @Override
    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(XorExpression xorExpression) {
        visitBinaryExpression(xorExpression);
    }

    @Override
    public void visit(Between between) {
        Expression leftExpr = between.getLeftExpression();
        if (leftExpr instanceof Column) {
            FrameContext ctx = analyzer.getStack().peek();
            if (ctx == null) {
                throw new SQLExceptionRTE(SQL_PARSER_EMPTY_STACK_FRAME);
            }
            Column column = (Column) leftExpr;
            TableContext tableCtx = ctx.getTableContext(column.getTable(), analyzer.getSchema());
            if (tableCtx != null) {
                ctx.addExpressionToTableContext(
                        column, analyzer.getSchema(), ExpressionType.BETWEEN,
                        between.getBetweenExpressionStart(), between.getBetweenExpressionEnd()
                );
            } else {
                between.getLeftExpression().accept(this);
                between.getBetweenExpressionStart().accept(this);
                between.getBetweenExpressionEnd().accept(this);
            }
        } else {
            between.getLeftExpression().accept(this);
            between.getBetweenExpressionStart().accept(this);
            between.getBetweenExpressionEnd().accept(this);
        }
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    @SuppressWarnings({"java:S3776"})
    @Override
    public void visit(InExpression inExpression) {
        Expression leftExpr = inExpression.getLeftExpression();
        ItemsList rItemList = inExpression.getRightItemsList();
        FrameContext ctx = analyzer.getStack().peek();
        if (ctx == null) {
            throw new SQLExceptionRTE(SQL_PARSER_EMPTY_STACK_FRAME);
        }
        if (inExpression.isNot() && leftExpr instanceof Column && rItemList instanceof ExpressionList) {
            // normal in expression
            // ... x in (1,2,..)
            Column column = (Column) leftExpr;
            List<Expression> exprList = ((ExpressionList) rItemList).getExpressions();
            TableContext tableCtx = ctx.getTableContext(column.getTable(), analyzer.getSchema());
            if (tableCtx != null) {
                for (Expression expression : exprList) {
                    ctx.addExpressionToTableContext(column, analyzer.getSchema(), ExpressionType.COMPARISON_EQUAL, expression);
                }
            } else {
                if (inExpression.getLeftExpression() != null) {
                    inExpression.getLeftExpression().accept(this);
                }
                if (inExpression.getRightExpression() != null) {
                    inExpression.getRightExpression().accept(this);
                } else if (inExpression.getRightItemsList() != null) {
                    inExpression.getRightItemsList().accept(this);
                } else {
                    inExpression.getMultiExpressionList().accept(this);
                }
            }
        } else {
            if (inExpression.getLeftExpression() != null) {
                inExpression.getLeftExpression().accept(this);
            }

            if (inExpression.getRightExpression() != null) {
                inExpression.getRightExpression().accept(this);
            } else if (inExpression.getRightItemsList() != null) {
                inExpression.getRightItemsList().accept(this);
            } else {
                inExpression.getMultiExpressionList().accept(this);
            }
        }
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        /* ignored */
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        /* ignored */
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        /* ignored */
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(Column column) {
        /* ignored */
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        if (caseExpression.getSwitchExpression() != null) {
            caseExpression.getSwitchExpression().accept(this);
        }
        if (caseExpression.getWhenClauses() != null) {
            for (WhenClause when : caseExpression.getWhenClauses()) {
                when.accept(this);
            }
        }
        if (caseExpression.getElseExpression() != null) {
            caseExpression.getElseExpression().accept(this);
        }
    }

    @Override
    public void visit(WhenClause whenClause) {
        if (whenClause.getWhenExpression() != null) {
            whenClause.getWhenExpression().accept(this);
        }
        if (whenClause.getThenExpression() != null) {
            whenClause.getThenExpression().accept(this);
        }
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(Concat concat) {
        visitBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        visitBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression castExpression) {
        castExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Modulo modulo) {
        visitBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression analyticExpression) {
        /* ignored */
    }

    @Override
    public void visit(ExtractExpression extractExpression) {
        /* ignored */
    }

    @Override
    public void visit(IntervalExpression intervalExpression) {
        /* ignored */
    }

    @Override
    public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {
        if (oracleHierarchicalExpression.getStartExpression() != null) {
            oracleHierarchicalExpression.getStartExpression().accept(this);
        }
        if (oracleHierarchicalExpression.getConnectExpression() != null) {
            oracleHierarchicalExpression.getConnectExpression().accept(this);
        }
    }

    @Override
    public void visit(RegExpMatchOperator regExpMatchOperator) {
        visitBinaryExpression(regExpMatchOperator);
    }

    @Override
    public void visit(JsonExpression jsonExpression) {
        /* ignored */
    }

    @Override
    public void visit(JsonOperator jsonOperator) {
        /* ignored */
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        visitBinaryExpression(regExpMySQLOperator);
    }

    @Override
    public void visit(UserVariable userVariable) {
        /* ignored */
    }

    @Override
    public void visit(NumericBind numericBind) {
        /* ignored */
    }

    @Override
    public void visit(KeepExpression keepExpression) {
        /* ignored */
    }

    @Override
    public void visit(MySQLGroupConcat mySQLGroupConcat) {
        /* ignored */
    }

    @Override
    public void visit(ValueListExpression valueListExpression) {
        valueListExpression.getExpressionList().accept(this);
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        for (Expression expression : rowConstructor.getExprList().getExpressions()) {
            expression.accept(this);
        }
    }

    @Override
    public void visit(RowGetExpression rowGetExpression) {
        rowGetExpression.getExpression().accept(this);
    }

    @Override
    public void visit(OracleHint oracleHint) {
        /* ignored */
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        /* ignored */
    }

    @Override
    public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {
        /* ignored */
    }

    @Override
    public void visit(NotExpression notExpression) {
        FrameContext ctx = analyzer.getStack().peek();
        if (ctx == null) {
            throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
        }
        ctx.flip();
        notExpression.getExpression().accept(this);
        ctx.flip();
    }

    @Override
    public void visit(NextValExpression nextValExpression) {
        /* ignored */
    }

    @Override
    public void visit(CollateExpression collateExpression) {
        collateExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(SimilarToExpression similarToExpression) {
        visitBinaryExpression(similarToExpression);
    }

    @Override
    public void visit(ArrayExpression arrayExpression) {
        arrayExpression.getObjExpression().accept(this);
        if (arrayExpression.getStartIndexExpression() != null) {
            arrayExpression.getIndexExpression().accept(this);
        }
        if (arrayExpression.getStartIndexExpression() != null) {
            arrayExpression.getStartIndexExpression().accept(this);
        }
        if (arrayExpression.getStopIndexExpression() != null) {
            arrayExpression.getStopIndexExpression().accept(this);
        }
    }

    @Override
    public void visit(ArrayConstructor arrayConstructor) {
        for (Expression expression : arrayConstructor.getExpressions()) {
            expression.accept(this);
        }
    }

    @Override
    public void visit(VariableAssignment variableAssignment) {
        variableAssignment.getVariable().accept(this);
        variableAssignment.getExpression().accept(this);
    }

    @Override
    public void visit(XMLSerializeExpr xmlSerializeExpr) {
        /* ignored */
    }

    @Override
    public void visit(TimezoneExpression timezoneExpression) {
        timezoneExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(JsonAggregateFunction jsonAggregateFunction) {
        Expression expr = jsonAggregateFunction.getExpression();
        if (expr != null) {
            expr.accept(this);
        }
        expr = jsonAggregateFunction.getFilterExpression();
        if (expr != null) {
            expr.accept(this);
        }
    }

    @Override
    public void visit(JsonFunction jsonFunction) {
        for (JsonFunctionExpression expression : jsonFunction.getExpressions()) {
            expression.getExpression().accept(this);
        }
    }

    @Override
    public void visit(ConnectByRootOperator connectByRootOperator) {
        connectByRootOperator.getColumn().accept(this);
    }

    @Override
    public void visit(OracleNamedFunctionParameter oracleNamedFunctionParameter) {
        oracleNamedFunctionParameter.getExpression().accept(this);
    }

    @Override
    public void visit(ExpressionList expressionList) {
        for (Expression expression : expressionList.getExpressions()) {
            expression.accept(this);
        }
    }

    @Override
    public void visit(NamedExpressionList namedExpressionList) {
        for (Expression expression : namedExpressionList.getExpressions()) {
            expression.accept(this);
        }
    }

    @Override
    public void visit(MultiExpressionList multiExpressionList) {
        for (ExpressionList expressionList : multiExpressionList.getExpressionLists()) {
            expressionList.accept(this);
        }
    }

    @Override
    public void visit(SavepointStatement savepointStatement) {
        /* ignored */
    }

    @Override
    public void visit(RollbackStatement rollbackStatement) {
        /* ignored */
    }

    @Override
    public void visit(Comment comment) {
        Table table = null;
        if (comment.getTable() != null) {
            table = comment.getTable();
        }
        if (comment.getColumn() != null) {
            table = comment.getColumn().getTable();
        }
        if (table != null) {
            analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.COMMENT));
            visit(table);
            FrameContext ctx = analyzer.getStack().pop();
            analyzer.getTableContexts().addAll(ctx.collectTables());
        }
    }

    @Override
    public void visit(Commit commit) {

    }

    @Override
    public void visit(Delete delete) {
        analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.DELETE));
        visit(delete.getTable());
        if (delete.getJoins() != null) {
            if (!containsJoin && !delete.getJoins().isEmpty()) {
                containsJoin = true;
            }
            for (Join join : delete.getJoins()) {
                join.getRightItem().accept(this);
            }
        }
        if (delete.getWhere() != null) {
            delete.getWhere().accept(this);
        }
        FrameContext ctx = analyzer.getStack().pop();
        analyzer.getTableContexts().addAll(ctx.collectTables());
    }

    @SuppressWarnings("java:S3776")
    @Override
    public void visit(Update update) {
        analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.UPDATE));
        visit(update.getTable());
        if (update.getStartJoins() != null) {
            if (!containsJoin && !update.getStartJoins().isEmpty()) {
                containsJoin = true;
            }
            for (Join join : update.getStartJoins()) {
                join.getRightItem().accept(this);
            }
        }
        if (update.getExpressions() != null) {
            for (Expression expression : update.getExpressions()) {
                expression.accept(this);
            }
        }
        if (update.getFromItem() != null) {
            update.getFromItem().accept(this);
        }
        if (update.getJoins() != null) {
            if (!containsJoin && !update.getJoins().isEmpty()) {
                containsJoin = true;
            }
            for (Join join : update.getJoins()) {
                join.getRightItem().accept(this);
            }
        }
        if (update.getWhere() != null) {
            update.getWhere().accept(this);
        }
        FrameContext ctx = analyzer.getStack().pop();
        analyzer.getTableContexts().addAll(ctx.collectTables());
    }

    @Override
    public void visit(Insert insert) {
        analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.INSERT));
        visit(insert.getTable());
        if (insert.getSelect() != null) {
            visit(insert.getSelect());
        }
        List<Column> cols = insert.getColumns();
        if (cols != null) {
            ItemsList items = insert.getItemsList();
            if (items instanceof ExpressionList) {
                if (!usingValue) {
                    usingValue = true;
                }
                visitColumnsAssignmentExpression(cols, (ExpressionList) items);
            } else if (items instanceof MultiExpressionList) {
                // not support multi expression list
                throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
            }
        }
        FrameContext ctx = analyzer.getStack().pop();
        analyzer.getTableContexts().addAll(ctx.collectTables());
    }

    @Override
    public void visit(Replace replace) {
        analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.REPLACE));
        visit(replace.getTable());
        if (replace.getExpressions() != null) {
            for (Expression expression : replace.getExpressions()) {
                expression.accept(this);
            }
        }
        if (replace.getItemsList() != null) {
            replace.getItemsList().accept(this);
        }
        List<Column> cols = replace.getColumns();
        if (cols != null) {
            ItemsList items = replace.getItemsList();
            if (items instanceof ExpressionList) {
                if (!usingValue) {
                    usingValue = true;
                }
                visitColumnsAssignmentExpression(cols, (ExpressionList) items);
            } else if (items instanceof MultiExpressionList) {
                // not support multi expression list
                throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
            }
        }
        FrameContext ctx = analyzer.getStack().pop();
        analyzer.getTableContexts().addAll(ctx.collectTables());
    }

    @Override
    public void visit(Drop drop) {
        /* ignored: no need to handle since it's always forward to all shards */
    }

    @Override
    public void visit(Truncate truncate) {
        /* ignored: no need to handle since it's always forward to all shards */
    }

    @Override
    public void visit(CreateIndex createIndex) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(CreateSchema createSchema) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(CreateTable createTable) {
        /* ignored: no need to handle since it's always forward to all shards */
    }

    @Override
    public void visit(CreateView createView) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(AlterView alterView) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(Alter alter) {
        /* ignored: no need to handle since it's always forward to all shards */
    }

    @Override
    public void visit(Statements statements) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(Execute execute) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(SetStatement setStatement) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(ResetStatement resetStatement) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(ShowColumnsStatement showColumnsStatement) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(ShowTablesStatement showTablesStatement) {
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(Merge merge) {
        analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.MERGE));
        visit(merge.getTable());
        if (merge.getUsingTable() != null) {
            merge.getUsingTable().accept(this);
        } else if (merge.getUsingSelect() != null) {
            merge.getUsingSelect().accept((ExpressionVisitor) this);
        }
        FrameContext ctx = analyzer.getStack().pop();
        analyzer.getTableContexts().addAll(ctx.collectTables());
    }

    @Override
    public void visit(Select select) {
        analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.SELECT));
        if (select.getWithItemsList() != null) {
            for (WithItem withItem : select.getWithItemsList()) {
                withItem.accept(this);
            }
        }
        select.getSelectBody().accept(this);
        FrameContext ctx = analyzer.getStack().pop();
        analyzer.getTableContexts().addAll(ctx.collectTables());
    }

    @Override
    public void visit(Upsert upsert) {
        analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.UPSERT));
        visit(upsert.getTable());
        if (upsert.getSelect() != null) {
            visit(upsert.getSelect());
        }
        List<Column> cols = upsert.getColumns();
        if (cols != null) {
            ItemsList items = upsert.getItemsList();
            if (items instanceof ExpressionList) {
                if (!usingValue) {
                    usingValue = true;
                }
                visitColumnsAssignmentExpression(cols, (ExpressionList) items);
            } else if (items instanceof MultiExpressionList) {
                // not support multi expression list
                throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
            }
        }
        FrameContext ctx = analyzer.getStack().pop();
        analyzer.getTableContexts().addAll(ctx.collectTables());
    }

    @Override
    public void visit(UseStatement useStatement) {
        /* ignored: no need to handle since it's always forward to all shards */
    }

    @Override
    public void visit(Block block) {
        if (block.getStatements() != null) {
            visit(block.getStatements());
        }
    }

    @Override
    public void visit(DescribeStatement describeStatement) {
        describeStatement.getTable().accept(this);
    }

    @Override
    public void visit(ExplainStatement explainStatement) {
        explainStatement.getStatement().accept(this);
    }

    @Override
    public void visit(ShowStatement showStatement) {
        /* ignored */
    }

    @Override
    public void visit(DeclareStatement declareStatement) {
        /* ignored */
    }

    @Override
    public void visit(Grant grant) {
        /* ignored */
    }

    @Override
    public void visit(CreateSequence createSequence) {
        /* ignored */
    }

    @Override
    public void visit(AlterSequence alterSequence) {
        /* ignored */
    }

    @Override
    public void visit(CreateFunctionalStatement createFunctionalStatement) {
        /* ignored */
    }

    @Override
    public void visit(CreateSynonym createSynonym) {
        /* ignored */
    }

    @Override
    public void visit(AlterSession alterSession) {
        /* ignored */
    }

    @Override
    public void visit(IfElseStatement ifElseStatement) {
        ifElseStatement.getIfStatement().accept(this);
        if (ifElseStatement.getElseStatement() != null) {
            ifElseStatement.getElseStatement().accept(this);
        }
    }

    @Override
    public void visit(RenameTableStatement renameTableStatement) {
        // will not support table rename
        throw new SQLExceptionRTE(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void visit(PurgeStatement purgeStatement) {
        if (purgeStatement.getPurgeObjectType() == PurgeObjectType.TABLE) {
            ((Table)purgeStatement.getObject()).accept(this);
        }
    }

    @Override
    public void visit(AlterSystemStatement alterSystemStatement) {
        /* ignored */
    }

    @Override
    public void visit(Table table) {
        FrameContext ctx = analyzer.getStack().peek();
        if (ctx == null) {
            throw new SQLExceptionRTE(SQL_PARSER_EMPTY_STACK_FRAME);
        }
        TableContext tableContext = new TableContext(table);
        ctx.putTableContext(table, analyzer.getSchema(), tableContext);
    }

    @Override
    public void visit(SubSelect subSelect) {
        analyzer.getStack().push(new FrameContext(analyzer.stackDepth(), StatementType.SELECT));
        if (!containsSubQuery) {
            containsSubQuery = true;
        }
        if (subSelect.getWithItemsList() != null) {
            for (WithItem withItem : subSelect.getWithItemsList()) {
                withItem.accept(this);
            }
        }
        subSelect.getSelectBody().accept(this);
        FrameContext ctx = analyzer.getStack().pop();
        analyzer.getTableContexts().addAll(ctx.collectTables());
    }

    @Override
    public void visit(SubJoin subJoin) {
        if (!containsJoin && !subJoin.getJoinList().isEmpty()) {
            containsJoin = true;
        }
        subJoin.getLeft().accept(this);
        for (Join join : subJoin.getJoinList()) {
            join.getRightItem().accept(this);
        }
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        if (!containsSubQuery) {
            containsSubQuery = true;
        }
        lateralSubSelect.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(ValuesList valuesList) {
        /* ignored */
    }

    @Override
    public void visit(TableFunction tableFunction) {
        /* ignored */
    }

    @Override
    public void visit(ParenthesisFromItem parenthesisFromItem) {
        parenthesisFromItem.getFromItem().accept(this);
    }

    @Override
    public void visit(AllColumns allColumns) {
        /* ignored */
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        /* ignored */
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(this);
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if (plainSelect.getSelectItems() != null) {
            for (SelectItem item : plainSelect.getSelectItems()) {
                item.accept(this);
            }
        }
        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }
        if (plainSelect.getJoins() != null) {
            if (!containsJoin && !plainSelect.getJoins().isEmpty()) {
                containsJoin = true;
            }
            for (Join join : plainSelect.getJoins()) {
                join.getRightItem().accept(this);
            }
        }
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }
        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(this);
        }
        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(this);
        }
    }

    @Override
    public void visit(SetOperationList setOperationList) {
        for (SelectBody selectBody : setOperationList.getSelects()) {
            selectBody.accept(this);
        }
    }

    @Override
    public void visit(WithItem withItem) {
        visit(withItem.getSubSelect());
    }

    @Override
    public void visit(ValuesStatement valuesStatement) {
        valuesStatement.getExpressions().accept(this);
    }
}
