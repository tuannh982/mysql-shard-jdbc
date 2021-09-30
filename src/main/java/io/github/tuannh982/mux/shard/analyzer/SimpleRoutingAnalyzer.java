package io.github.tuannh982.mux.shard.analyzer;

import io.github.tuannh982.mux.commons.tuple.Tuple2;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.statements.invocation.PreparedStatementMethodInvocation;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;

import java.sql.SQLException;
import java.util.Map;

import static io.github.tuannh982.mux.connection.Constants.*;

public class SimpleRoutingAnalyzer implements Analyzer {
    @Override
    public Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyze(
            String sql,
            PreparedStatementMethodInvocation preparedMethodInvocation,
            ShardOps shardOps
    ) throws SQLException {
        Statement statement = parse(sql);
        return null;
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
}
