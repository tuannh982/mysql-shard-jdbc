package io.github.tuannh982.mux.statements;

import io.github.tuannh982.mux.connection.Constants;
import io.github.tuannh982.mux.connection.MuxConnection;
import io.github.tuannh982.mux.shard.analyzer.Analyzer;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.statements.history.ParamInvocationEntry;
import io.github.tuannh982.mux.statements.history.ParamInvocationHistory;
import io.github.tuannh982.mux.statements.history.ParamInvocationPlayback;
import io.github.tuannh982.mux.statements.resultset.MuxResultSet;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.tuannh982.mux.connection.Constants.UNINITIALIZED_VARIABLE;
import static io.github.tuannh982.mux.statements.MuxStatementConstructorType.*;
import static io.github.tuannh982.mux.statements.MuxStatementInvocationMethod.*;

@SuppressWarnings("DuplicatedCode")
public class MuxStatement implements Statement, ParamInvocationPlayback {
    protected MuxStatementConstructorType constructorType;
    protected MuxConnection connection;
    protected Analyzer analyzer;
    protected ShardOps shardOps;
    private final ParamInvocationHistory<MuxStatementInvocationMethod> paramInvocationHistory = new ParamInvocationHistory<>();
    protected int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    protected int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    protected int resultSetHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    protected List<? extends Statement> statements = new ArrayList<>();
    protected ResultSet resultSet = null;
    protected int updateCount = 0;
    private volatile boolean isClosed = false;

    public MuxStatement(MuxConnection connection) {
        this.connection = connection;
        this.analyzer = connection.getInternal().getAnalyzer();
        this.shardOps = connection.getInternal().getShardOps();
        this.constructorType = STATEMENT_EMPTY;
    }

    public MuxStatement(MuxConnection connection, int resultSetType, int resultSetConcurrency) {
        this.connection = connection;
        this.analyzer = connection.getInternal().getAnalyzer();
        this.shardOps = connection.getInternal().getShardOps();
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.constructorType = STATEMENT_II;
    }

    public MuxStatement(MuxConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        this.connection = connection;
        this.analyzer = connection.getInternal().getAnalyzer();
        this.shardOps = connection.getInternal().getShardOps();
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.constructorType = STATEMENT_III;
    }

    //-------------------------
    private void prepareStatements(Integer[] selectedShards) throws SQLException {
        switch (constructorType) {
            case STATEMENT_EMPTY:
                statements = connection.getInternal().createStatement(selectedShards);
                break;
            case STATEMENT_II:
                statements = connection.getInternal().createStatement(selectedShards, resultSetType, resultSetConcurrency);
                break;
            case STATEMENT_III:
                statements = connection.getInternal().createStatement(selectedShards, resultSetType, resultSetConcurrency, resultSetHoldability);
                break;
            default:
                throw new SQLException("Unexpected type " + constructorType);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            List<ResultSet> resultSets = new ArrayList<>(sqls.size());
            for (int i = 0; i < shardIndexes.length; i++) {
                statements.get(i).executeQuery(sqls.get(shardIndexes[i]));
            }
            resultSet = new MuxResultSet(this, resultSets);
            return resultSet;
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            int affected = 0;
            for (int i = 0; i < shardIndexes.length; i++) {
                affected += statements.get(i).executeUpdate(sqls.get(shardIndexes[i]));
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            int affected = 0;
            for (int i = 0; i < shardIndexes.length; i++) {
                affected += statements.get(i).executeUpdate(sqls.get(shardIndexes[i]), autoGeneratedKeys);
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            int affected = 0;
            for (int i = 0; i < shardIndexes.length; i++) {
                affected += statements.get(i).executeUpdate(sqls.get(shardIndexes[i]), columnIndexes);
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            int affected = 0;
            for (int i = 0; i < shardIndexes.length; i++) {
                affected += statements.get(i).executeUpdate(sqls.get(shardIndexes[i]), columnNames);
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            boolean ret = false;
            for (int i = 0; i < shardIndexes.length; i++) {
                ret |= statements.get(i).execute(sqls.get(shardIndexes[i]));
            }
            return ret;
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            boolean ret = false;
            for (int i = 0; i < shardIndexes.length; i++) {
                ret |= statements.get(i).execute(sqls.get(shardIndexes[i]), autoGeneratedKeys);
            }
            return ret;
        }
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            boolean ret = false;
            for (int i = 0; i < shardIndexes.length; i++) {
                ret |= statements.get(i).execute(sqls.get(shardIndexes[i]), columnIndexes);
            }
            return ret;
        }
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        synchronized (this) {
            Map<Integer, String> sqls = analyzer.analyze(sql, false, null, shardOps);
            Integer[] shardIndexes = sqls.keySet().toArray(new Integer[0]);
            prepareStatements(shardIndexes);
            playback();
            boolean ret = false;
            for (int i = 0; i < shardIndexes.length; i++) {
                ret |= statements.get(i).execute(sqls.get(shardIndexes[i]), columnNames);
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
        return false; // TODO
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false; // TODO
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null; // TODO
    }
    //-------------------------

    @SuppressWarnings("java:S3776")
    @Override
    public void playback() throws SQLException {
        List<ParamInvocationEntry<MuxStatementInvocationMethod>> invokeList = paramInvocationHistory.extractStateAsList();
        for (ParamInvocationEntry<MuxStatementInvocationMethod> invokeCommand : invokeList) {
            Object[] v = invokeCommand.getParams();
            switch (invokeCommand.getMethod()) {
                case MAX_FIELD_SIZE:
                    for (Statement statement : statements) {
                        statement.setMaxFieldSize((Integer) v[0]);
                    }
                    break;
                case MAX_ROWS:
                    for (Statement statement : statements) {
                        statement.setMaxRows((Integer) v[0]);
                    }
                    break;
                case ESCAPE_PROCESSING:
                    for (Statement statement : statements) {
                        statement.setEscapeProcessing((Boolean) v[0]);
                    }
                    break;
                case QUERY_TIMEOUT:
                    for (Statement statement : statements) {
                        statement.setQueryTimeout((Integer) v[0]);
                    }
                    break;
                case CURSOR_NAME:
                    for (Statement statement : statements) {
                        statement.setCursorName((String) v[0]);
                    }
                    break;
                case FETCH_DIRECTION:
                    for (Statement statement : statements) {
                        //noinspection MagicConstant
                        statement.setFetchDirection((Integer) v[0]);
                    }
                    break;
                case FETCH_SIZE:
                    for (Statement statement : statements) {
                        statement.setFetchSize((Integer) v[0]);
                    }
                    break;
                case CLOSE_ON_COMPLETION:
                    for (Statement statement : statements) {
                        statement.closeOnCompletion();
                    }
                    break;
                case POOLABLE:
                    for (Statement statement : statements) {
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
            for (Statement statement : statements) {
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
            for (Statement statement : statements) {
                statement.cancel();
            }
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        MuxStatementInvocationMethod method = MAX_FIELD_SIZE;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "MaxFieldSize");
        }
        return (int) ret[0];
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        MuxStatementInvocationMethod method = MAX_FIELD_SIZE;
        paramInvocationHistory.getState().put(method, new Object[] {max});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {max}));
    }

    @Override
    public int getMaxRows() throws SQLException {
        MuxStatementInvocationMethod method = MAX_ROWS;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "MaxRows");
        }
        return (int) ret[0];
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        MuxStatementInvocationMethod method = MAX_ROWS;
        paramInvocationHistory.getState().put(method, new Object[] {max});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {max}));
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        MuxStatementInvocationMethod method = ESCAPE_PROCESSING;
        paramInvocationHistory.getState().put(method, new Object[] {enable});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {enable}));
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        MuxStatementInvocationMethod method = QUERY_TIMEOUT;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "QueryTimeout");
        }
        return (int) ret[0];
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        MuxStatementInvocationMethod method = QUERY_TIMEOUT;
        paramInvocationHistory.getState().put(method, new Object[] {seconds});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {seconds}));
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        MuxStatementInvocationMethod method = CURSOR_NAME;
        paramInvocationHistory.getState().put(method, new Object[] {name});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {name}));
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        MuxStatementInvocationMethod method = FETCH_DIRECTION;
        paramInvocationHistory.getState().put(method, new Object[] {direction});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {direction}));
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getFetchDirection() throws SQLException {
        MuxStatementInvocationMethod method = FETCH_DIRECTION;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "FetchDirection");
        }
        return (int) ret[0];
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        MuxStatementInvocationMethod method = FETCH_SIZE;
        paramInvocationHistory.getState().put(method, new Object[] {rows});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {rows}));
    }

    @Override
    public int getFetchSize() throws SQLException {
        MuxStatementInvocationMethod method = FETCH_SIZE;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "FetchSize");
        }
        return (int) ret[0];
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        MuxStatementInvocationMethod method = CLOSE_ON_COMPLETION;
        paramInvocationHistory.getState().put(method, new Object[] {true});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {true}));
    }

    @Override
    public boolean isCloseOnCompletion() {
        MuxStatementInvocationMethod method = CLOSE_ON_COMPLETION;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            return false;
        }
        return (boolean) ret[0];
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        MuxStatementInvocationMethod method = POOLABLE;
        paramInvocationHistory.getState().put(method, new Object[] {poolable});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {poolable}));
    }

    @Override
    public boolean isPoolable() throws SQLException {
        MuxStatementInvocationMethod method = POOLABLE;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "Poolable");
        }
        return (boolean) ret[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
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
    public int getResultSetHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        // will not be supported
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
        return null; // TODO
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO
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
