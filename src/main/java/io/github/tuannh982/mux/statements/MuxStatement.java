package io.github.tuannh982.mux.statements;

import io.github.tuannh982.mux.connection.Constants;
import io.github.tuannh982.mux.connection.MuxConnection;
import io.github.tuannh982.mux.statements.history.ParamInvocationEntry;
import io.github.tuannh982.mux.statements.history.ParamInvocationHistory;
import io.github.tuannh982.mux.statements.history.ParamInvocationPlayback;

import java.sql.*;
import java.util.List;

import static io.github.tuannh982.mux.connection.Constants.UNINITIALIZED_VARIABLE;
import static io.github.tuannh982.mux.statements.StatementInvocationMethod.*;

public class MuxStatement implements Statement, ParamInvocationPlayback {
    protected final MuxConnection connection;
    private final ParamInvocationHistory<StatementInvocationMethod> paramInvocationHistory = new ParamInvocationHistory<>();
    protected int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    protected int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    protected int resultSetHoldability = -1;
    protected List<? extends Statement> statements;
    protected ResultSet resultSet;
    protected int updateCount;

    //-------------------------
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }
    //-------------------------

    @Override
    public void playback() throws SQLException {
        List<ParamInvocationEntry<StatementInvocationMethod>> invokeList = paramInvocationHistory.extractStateAsList();
        for (ParamInvocationEntry<StatementInvocationMethod> invokeCommand : invokeList) {
            switch (invokeCommand.getMethod()) {
                case MAX_FIELD_SIZE:
                    // TODO
                    break;
                case MAX_ROWS:
                    // TODO
                    break;
                case ESCAPE_PROCESSING:
                    // TODO
                    break;
                case QUERY_TIMEOUT:
                    // TODO
                    break;
                case CURSOR_NAME:
                    // TODO
                    break;
                case FETCH_DIRECTION:
                    // TODO
                    break;
                case FETCH_SIZE:
                    // TODO
                    break;
                case CLOSE_ON_COMPLETION:
                    // TODO
                    break;
                case POOLABLE:
                    // TODO
                    break;
                default:
                    throw new SQLException("Unexpected command " + invokeCommand.getMethod());
            }
        }
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        StatementInvocationMethod method = MAX_FIELD_SIZE;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "MaxFieldSize");
        }
        return (int) ret[0];
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        StatementInvocationMethod method = MAX_FIELD_SIZE;
        paramInvocationHistory.getState().put(method, new Object[] {max});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {max}));
    }

    @Override
    public int getMaxRows() throws SQLException {
        StatementInvocationMethod method = MAX_ROWS;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "MaxRows");
        }
        return (int) ret[0];
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        StatementInvocationMethod method = MAX_ROWS;
        paramInvocationHistory.getState().put(method, new Object[] {max});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {max}));
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        StatementInvocationMethod method = ESCAPE_PROCESSING;
        paramInvocationHistory.getState().put(method, new Object[] {enable});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {enable}));
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        StatementInvocationMethod method = QUERY_TIMEOUT;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "QueryTimeout");
        }
        return (int) ret[0];
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        StatementInvocationMethod method = QUERY_TIMEOUT;
        paramInvocationHistory.getState().put(method, new Object[] {seconds});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {seconds}));
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        StatementInvocationMethod method = CURSOR_NAME;
        paramInvocationHistory.getState().put(method, new Object[] {name});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {name}));
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        StatementInvocationMethod method = FETCH_DIRECTION;
        paramInvocationHistory.getState().put(method, new Object[] {direction});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {direction}));
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getFetchDirection() throws SQLException {
        StatementInvocationMethod method = FETCH_DIRECTION;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "FetchDirection");
        }
        return (int) ret[0];
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        StatementInvocationMethod method = FETCH_SIZE;
        paramInvocationHistory.getState().put(method, new Object[] {rows});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {rows}));
    }

    @Override
    public int getFetchSize() throws SQLException {
        StatementInvocationMethod method = FETCH_SIZE;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "FetchSize");
        }
        return (int) ret[0];
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        StatementInvocationMethod method = CLOSE_ON_COMPLETION;
        paramInvocationHistory.getState().put(method, new Object[] {true});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {true}));
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        StatementInvocationMethod method = CLOSE_ON_COMPLETION;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            return false;
        }
        return (boolean) ret[0];
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        StatementInvocationMethod method = POOLABLE;
        paramInvocationHistory.getState().put(method, new Object[] {poolable});
        paramInvocationHistory.getHistory().add(new ParamInvocationEntry<>(method, new Object[] {poolable}));
    }

    @Override
    public boolean isPoolable() throws SQLException {
        StatementInvocationMethod method = POOLABLE;
        Object[] ret = paramInvocationHistory.getState().get(method);
        if (ret == null || ret.length != method.getNumberOfArgs()) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "Poolable");
        }
        return (boolean) ret[0];
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        // TODO support later
        // might be not supported
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void clearBatch() throws SQLException {
        // TODO support later
        // might be not supported
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        // TODO support later
        // might be not supported
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
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
