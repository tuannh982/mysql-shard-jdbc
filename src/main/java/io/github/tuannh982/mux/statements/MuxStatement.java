package io.github.tuannh982.mux.statements;

import io.github.tuannh982.mux.commons.tuple.Tuple2;
import io.github.tuannh982.mux.connection.Constants;
import io.github.tuannh982.mux.connection.MuxConnection;
import io.github.tuannh982.mux.shard.analyzer.Analyzer;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.statements.invocation.MethodInvocationEntry;
import io.github.tuannh982.mux.statements.invocation.PreparedStatementMethodInvocation;
import io.github.tuannh982.mux.statements.invocation.StatementMethodInvocation;
import io.github.tuannh982.mux.statements.resultset.MuxResultSet;

import java.sql.*;
import java.util.*;

import static io.github.tuannh982.mux.connection.Constants.OPERATION_NOT_SUPPORTED;
import static io.github.tuannh982.mux.connection.Constants.UNINITIALIZED_VARIABLE;
import static io.github.tuannh982.mux.statements.MuxStatementMethodInvocation.*;

@SuppressWarnings("DuplicatedCode")
public class MuxStatement implements Statement {
    private final ConstructorType statementConstructorType;
    protected MuxConnection connection;
    protected Analyzer analyzer;
    protected ShardOps shardOps;
    //------------------------------------------------------------------------------------------------------------------
    private final StatementMethodInvocation methodInvocationState = new StatementMethodInvocation();
    //------------------------------------------------------------------------------------------------------------------
    protected int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    protected int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    protected int resultSetHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    //------------------------------------------------------------------------------------------------------------------
    protected Map<Integer, Statement> statements;
    protected ResultSet resultSet = null;
    protected int updateCount = 0;
    //------------------------------------------------------------------------------------------------------------------
    private volatile boolean isClosed = false;

    private enum ConstructorType {
        STATEMENT_EMPTY,
        STATEMENT_II,
        STATEMENT_III
    }

    protected void init(MuxConnection connection) {
        this.connection = connection;
        this.analyzer = connection.getInternal().getAnalyzer();
        this.shardOps = connection.getInternal().getShardOps();
    }

    protected MuxStatement() {
        statementConstructorType = null;
    }

    public MuxStatement(MuxConnection connection) {
        init(connection);
        this.statementConstructorType = ConstructorType.STATEMENT_EMPTY;
    }

    public MuxStatement(MuxConnection connection, int resultSetType, int resultSetConcurrency) {
        init(connection);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.statementConstructorType = ConstructorType.STATEMENT_II;
    }

    public MuxStatement(MuxConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        init(connection);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.statementConstructorType = ConstructorType.STATEMENT_III;
    }

    //-------------------------
    private void statementPreparation(Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult) throws SQLException {
        switch (statementConstructorType) {
            case STATEMENT_EMPTY:
                statements = connection.getInternal().createStatement(analyzedResult);
                break;
            case STATEMENT_II:
                statements = connection.getInternal().createStatement(analyzedResult, resultSetType, resultSetConcurrency);
                break;
            case STATEMENT_III:
                statements = connection.getInternal().createStatement(analyzedResult, resultSetType, resultSetConcurrency, resultSetHoldability);
                break;
            default:
                throw new SQLException("Unexpected type " + statementConstructorType);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            List<ResultSet> resultSets = new ArrayList<>(analyzedResult.size());
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                resultSets.add(statement.executeQuery(analyzedResult.get(index).getA0()));
            }
            resultSet = new MuxResultSet(this, resultSets);
            return resultSet;
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            int affected = 0;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                affected += statement.executeUpdate(analyzedResult.get(index).getA0());
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            int affected = 0;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                affected += statement.executeUpdate(analyzedResult.get(index).getA0(), autoGeneratedKeys);
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            int affected = 0;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                affected += statement.executeUpdate(analyzedResult.get(index).getA0(), columnIndexes);
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            int affected = 0;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                affected += statement.executeUpdate(analyzedResult.get(index).getA0(), columnNames);
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            boolean ret = false;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                ret |= statement.execute(analyzedResult.get(index).getA0());
            }
            return ret;
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            boolean ret = false;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                ret |= statement.execute(analyzedResult.get(index).getA0(), autoGeneratedKeys);
            }
            return ret;
        }
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            boolean ret = false;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                ret |= statement.execute(analyzedResult.get(index).getA0(), columnIndexes);
            }
            return ret;
        }
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult = analyzer.analyze(sql, null, shardOps);
            statementPreparation(analyzedResult);
            playback();
            boolean ret = false;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                Statement statement = entry.getValue();
                ret |= statement.execute(analyzedResult.get(index).getA0(), columnNames);
            }
            return ret;
        }
    }
    //-------------------------
    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // TODO support later
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        // TODO support later
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        List<ResultSet> resultSets = new ArrayList<>();
        for (Statement statement : statements.values()) {
            resultSets.add(statement.getGeneratedKeys());
        }
        return new MuxResultSet(this, resultSets);
    }
    //-------------------------

    @SuppressWarnings("java:S3776")
    protected void playback() throws SQLException {
        List<MethodInvocationEntry<MuxStatementMethodInvocation>> invokeList = methodInvocationState.playbackList();
        Collection<Statement> statementList = statements.values();
        for (MethodInvocationEntry<MuxStatementMethodInvocation> invokeCommand : invokeList) {
            Object[] v = invokeCommand.getParams();
            switch (invokeCommand.getMethod()) {
                case MAX_FIELD_SIZE:
                    for (Statement statement : statementList) {
                        statement.setMaxFieldSize((Integer) v[0]);
                    }
                    break;
                case MAX_ROWS:
                    for (Statement statement : statementList) {
                        statement.setMaxRows((Integer) v[0]);
                    }
                    break;
                case ESCAPE_PROCESSING:
                    for (Statement statement : statementList) {
                        statement.setEscapeProcessing((Boolean) v[0]);
                    }
                    break;
                case QUERY_TIMEOUT:
                    for (Statement statement : statementList) {
                        statement.setQueryTimeout((Integer) v[0]);
                    }
                    break;
                case CURSOR_NAME:
                    for (Statement statement : statementList) {
                        statement.setCursorName((String) v[0]);
                    }
                    break;
                case FETCH_DIRECTION:
                    for (Statement statement : statementList) {
                        //noinspection MagicConstant
                        statement.setFetchDirection((Integer) v[0]);
                    }
                    break;
                case FETCH_SIZE:
                    for (Statement statement : statementList) {
                        statement.setFetchSize((Integer) v[0]);
                    }
                    break;
                case CLOSE_ON_COMPLETION:
                    for (Statement statement : statementList) {
                        statement.closeOnCompletion();
                    }
                    break;
                case POOLABLE:
                    for (Statement statement : statementList) {
                        statement.setPoolable((Boolean) v[0]);
                    }
                    break;
                default:
                    throw new SQLException("Unexpected command " + invokeCommand.getMethod());
            }
        }
    }

    @Override
    public void close() throws SQLException {
        synchronized (this) {
            for (Statement statement : statements.values()) {
                statement.close();
            }
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void cancel() throws SQLException {
        synchronized (this) {
            for (Statement statement : statements.values()) {
                statement.cancel();
            }
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        MuxStatementMethodInvocation method = MAX_FIELD_SIZE;
        Object[] ret = methodInvocationState.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "MaxFieldSize");
        }
        return (int) ret[0];
    }

    @Override
    public void setMaxFieldSize(int max) {
        methodInvocationState.getState().put(MAX_FIELD_SIZE, new Object[] {max});
    }

    @Override
    public int getMaxRows() throws SQLException {
        MuxStatementMethodInvocation method = MAX_ROWS;
        Object[] ret = methodInvocationState.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "MaxRows");
        }
        return (int) ret[0];
    }

    @Override
    public void setMaxRows(int max) {
        methodInvocationState.getState().put(MAX_ROWS, new Object[] {max});
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        methodInvocationState.getState().put(ESCAPE_PROCESSING, new Object[] {enable});
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        MuxStatementMethodInvocation method = QUERY_TIMEOUT;
        Object[] ret = methodInvocationState.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "QueryTimeout");
        }
        return (int) ret[0];
    }

    @Override
    public void setQueryTimeout(int seconds) {
        methodInvocationState.getState().put(QUERY_TIMEOUT, new Object[] {seconds});
    }

    @Override
    public void setCursorName(String name) {
        methodInvocationState.getState().put(CURSOR_NAME, new Object[] {name});
    }

    @Override
    public void setFetchDirection(int direction) {
        methodInvocationState.getState().put(FETCH_DIRECTION, new Object[] {direction});
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getFetchDirection() throws SQLException {
        MuxStatementMethodInvocation method = FETCH_DIRECTION;
        Object[] ret = methodInvocationState.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "FetchDirection");
        }
        return (int) ret[0];
    }

    @Override
    public void setFetchSize(int rows) {
        methodInvocationState.getState().put(FETCH_SIZE, new Object[] {rows});
    }

    @Override
    public int getFetchSize() throws SQLException {
        MuxStatementMethodInvocation method = FETCH_SIZE;
        Object[] ret = methodInvocationState.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "FetchSize");
        }
        return (int) ret[0];
    }

    @Override
    public void closeOnCompletion() {
        methodInvocationState.getState().put(CLOSE_ON_COMPLETION, new Object[] {true});
    }

    @Override
    public boolean isCloseOnCompletion() {
        MuxStatementMethodInvocation method = CLOSE_ON_COMPLETION;
        Object[] ret = methodInvocationState.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            return false;
        }
        return (boolean) ret[0];
    }

    @Override
    public void setPoolable(boolean poolable) {
        methodInvocationState.getState().put(POOLABLE, new Object[] {poolable});
    }

    @Override
    public boolean isPoolable() throws SQLException {
        MuxStatementMethodInvocation method = POOLABLE;
        Object[] ret = methodInvocationState.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "Poolable");
        }
        return (boolean) ret[0];
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public int getResultSetConcurrency() {
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() {
        return resultSetType;
    }

    @Override
    public int getResultSetHoldability() {
        return resultSetHoldability;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        // TODO support later
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void clearBatch() throws SQLException {
        // will not be supported
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        // will not be supported
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        SQLWarning first = null;
        SQLWarning last = null;
        for (Statement statement : statements.values()) {
            SQLWarning warning = statement.getWarnings();
            if (first == null) {
                first = warning;
            } else {
                last.setNextWarning(warning);
            }
            last = warning;
        }
        return first;
    }

    @Override
    public void clearWarnings() throws SQLException {
        for (Statement statement : statements.values()) {
            statement.clearWarnings();
        }
    }

    // Wrapper class methods

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (this.isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException("The receiver is not a wrapper for " + iface.getName());
            }
        } catch (Exception e) {
            throw new SQLException("The receiver is not a wrapper and does not implement the interface");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
