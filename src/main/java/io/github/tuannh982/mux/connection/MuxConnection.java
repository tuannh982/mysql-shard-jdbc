package io.github.tuannh982.mux.connection;

import io.github.tuannh982.mux.statements.MuxStatement;
import io.github.tuannh982.mux.urlparser.ParsedUrl;
import lombok.Getter;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

public class MuxConnection implements Connection {
    @Getter
    private final MuxConnectionInternal internal;
    private final ReentrantLock lock;

    private MuxConnection(MuxConnectionInternal internal, ReentrantLock lock) {
        this.internal = internal;
        this.lock = lock;
    }

    public static MuxConnection newConnection(ParsedUrl parsedUrl) throws SQLException {
        ReentrantLock lock = new ReentrantLock();
        MuxConnectionInternal internal = new MuxConnectionInternal(parsedUrl, lock);
        return new MuxConnection(internal, lock);
    }

    //-----createStatement-----
    @Override
    public Statement createStatement() throws SQLException {
        return new MuxStatement(this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new MuxStatement(this, resultSetType, resultSetConcurrency);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new MuxStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }
    //-------------------------

    //-----prepareStatement----
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        // TODO
        return null;
    }
    //-------------------------

    //-----prepareCall---------
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        // will not be supported
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        // will not be supported
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        // will not be supported
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }
    //-------------------------

    @Override
    public String nativeSQL(String sql) throws SQLException {
        // TODO support later
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    //-------------------------
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        internal.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return internal.getAutoCommit();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        internal.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return internal.getHoldability();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        internal.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return internal.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        internal.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return internal.getCatalog();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        internal.setTypeMap(map);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return internal.getTypeMap();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        internal.setTransactionIsolation(level);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getTransactionIsolation() throws SQLException {
        return internal.getTransactionIsolation();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        internal.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return internal.getNetworkTimeout();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        internal.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        internal.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return internal.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return internal.getClientInfo();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        internal.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return internal.getSchema();
    }
    //-------------------------

    //-------------------------
    @Override
    public void commit() throws SQLException {
        internal.commit();
    }

    @Override
    public void rollback() throws SQLException {
        internal.rollback();
    }
    //-------------------------

    //-------------------------
    @Override
    public void close() throws SQLException {
        internal.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return internal.isClosed();
    }
    //-------------------------

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null; // TODO
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null; // TODO
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        // README will not support savepoint
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public Savepoint setSavepoint(String s) throws SQLException {
        // README will not support savepoint
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        // README will not support savepoint
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        // README will not support savepoint
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public Clob createClob() throws SQLException {
        // TODO support later
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public Blob createBlob() throws SQLException {
        // TODO support later
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public NClob createNClob() throws SQLException {
        // TODO support later
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO support later
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        // TODO support later
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        // TODO support later
        throw new SQLException(Constants.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        return false; // TODO
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        internal.abort(executor);
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
