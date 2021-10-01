package io.github.tuannh982.mux.shard.analyzer.simplerouting;

import io.github.tuannh982.mux.commons.tuple.Tuple2;
import io.github.tuannh982.mux.shard.analyzer.Analyzer;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.statements.invocation.PreparedStatementMethodInvocation;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.github.tuannh982.mux.ErrorMessages.*;
import static io.github.tuannh982.mux.shard.analyzer.ErrorMessages.*;

public class SimpleRoutingAnalyzer implements Analyzer {
    @Override
    public Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyze(
            String schema,
            String sql,
            PreparedStatementMethodInvocation preparedMethodInvocation,
            ShardOps shardOps
    ) throws SQLException {
        Statement statement = parse(sql);
        return handleStatement(schema, sql, statement, preparedMethodInvocation, shardOps);
    }

    private Statement parse(String sql) throws SQLException {
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(sql);
            if (statements.getStatements().isEmpty()) {
                throw new SQLException(SQL_PARSER_NO_STATEMENTS_FOUND);
            } else if (statements.getStatements().size() > 1) {
                throw new SQLException(SQL_PARSER_MULTIPLE_STATEMENTS_NOT_ALLOWED);
            } else {
                return statements.getStatements().get(0);
            }
        } catch (JSQLParserException e) {
            SQLException te = new SQLException(SQL_PARSER_EXCEPTION);
            te.setStackTrace(e.getStackTrace());
            throw te;
        }
    }

    @SuppressWarnings("java:S3776")
    private Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> handleStatement(
            String schema,
            String originalSql,
            Statement statement,
            PreparedStatementMethodInvocation preparedMethodInvocation,
            ShardOps shardOps
    ) throws SQLException {
        if (
                statement instanceof Delete         ||
                statement instanceof Update         ||
                statement instanceof Select         ||
                statement instanceof Merge          ||
                statement instanceof Insert         ||
                statement instanceof Upsert         ||
                statement instanceof Replace
        ) {
            SimpleRoutingStatementAnalyzer statementAnalyzer = new SimpleRoutingStatementAnalyzer(
                    schema,
                    statement,
                    shardOps
            );
            if (preparedMethodInvocation != null) { // is prepared statement
                statementAnalyzer.fillingParameters(preparedMethodInvocation.getValueMap());
            }
            statementAnalyzer.analyze();
            Set<Integer> involvedShards = statementAnalyzer.extractInvolvedShards();
            if (
                    statement instanceof Delete         ||
                    statement instanceof Update         ||
                    statement instanceof Select         ||
                    statement instanceof Merge
            ) {
                /* not contains any JOIN or sub SELECT (sub SELECT might be equals to JOIN in some cases) */
                if (!statementAnalyzer.containsJoin() && !statementAnalyzer.containsSubQuery()) {
                    if (involvedShards.isEmpty()) { /* forward sql to all shards */
                        return forwardToAllShards(originalSql, preparedMethodInvocation, shardOps);
                    } else if (involvedShards.size() == 1) { /* this sql will be executed on 1 shard */
                        return forwardToOneShard(originalSql, preparedMethodInvocation, involvedShards.iterator().next());
                    } else {
                        throw new SQLException("Cross shard statement detected, statement = \n" + originalSql);
                    }
                } else { /* this sql will be executed on at most 1 shard*/
                    if (involvedShards.size() == 1) { /* this sql will be executed on 1 shard */
                        return forwardToOneShard(originalSql, preparedMethodInvocation, involvedShards.iterator().next());
                    } else {
                        throw new SQLException("Cross shard statement detected, statement = \n" + originalSql);
                    }
                }
            } else {
                if (statementAnalyzer.usingValues()) { /* using VALUES */
                    if (involvedShards.isEmpty()) { /* forward sql to all shards */
                        return forwardToAllShards(originalSql, preparedMethodInvocation, shardOps);
                    } else if (involvedShards.size() == 1) { /* this sql will be executed on 1 shard */
                        return forwardToOneShard(originalSql, preparedMethodInvocation, involvedShards.iterator().next());
                    } else {
                        throw new SQLException("Cross shard statement detected, statement = \n" + originalSql);
                    }
                } else {
                    throw new SQLException(OPERATION_NOT_SUPPORTED);
                }
            }
        } else if (
                statement instanceof CreateTable    ||
                statement instanceof Alter          ||
                statement instanceof Drop           ||
                statement instanceof Truncate       ||
                statement instanceof UseStatement
        ) { /* this sql will be executed on all shards */
            return forwardToAllShards(originalSql, preparedMethodInvocation, shardOps);
        } else {
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        }
    }

    private Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> forwardToOneShard(
            String originalSql,
            PreparedStatementMethodInvocation preparedMethodInvocation,
            int shardIndex
    ) {
        Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> ret = new HashMap<>();
        ret.put(shardIndex, Tuple2.of(originalSql, preparedMethodInvocation));
        return ret;
    }

    private Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> forwardToAllShards(
            String originalSql,
            PreparedStatementMethodInvocation preparedMethodInvocation,
            ShardOps shardOps
    ) {
        Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> ret = new HashMap<>();
        for (int i = 0; i < shardOps.getPhysNodeCount(); i++) {
            ret.put(i, Tuple2.of(originalSql, preparedMethodInvocation));
        }
        return ret;
    }
}
