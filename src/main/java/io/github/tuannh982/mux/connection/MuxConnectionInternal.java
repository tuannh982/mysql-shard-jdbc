package io.github.tuannh982.mux.connection;

import io.github.tuannh982.mux.config.ShardConfig;
import io.github.tuannh982.mux.config.ShardConfigStore;
import io.github.tuannh982.mux.config.ShardConfigStoreFactory;
import io.github.tuannh982.mux.shard.analyzer.Analyzer;
import io.github.tuannh982.mux.shard.analyzer.AnalyzerFactory;
import io.github.tuannh982.mux.shard.shardops.Murmur3Hash;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import io.github.tuannh982.mux.urlparser.ParsedUrl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import static io.github.tuannh982.mux.connection.Constants.*;

@Slf4j
public class MuxConnectionInternal {
    private final ReentrantLock lock;
    private final EnumMap<ConnectionProperties, Object> connectionProperties;
    private final ShardConfigStore shardConfigStore;
    private final ShardConfig shardConfig;
    private final Connection[] connections;
    private volatile boolean isClosed = false;
    // transaction
    private String transactionIdentifier = null;
    private enum TransactionState {
        INITIALIZED,
        STARTED,
        ENDED,
        PREPARED,
    }
    private TransactionState transactionStatus = TransactionState.INITIALIZED;
    //
    @Getter
    private final Analyzer analyzer;
    @Getter
    private final ShardOps shardOps;

    public MuxConnectionInternal(ParsedUrl parsedUrl, ReentrantLock lock) throws SQLException {
        this.lock = lock;
        this.connectionProperties = new EnumMap<>(ConnectionProperties.class);
        final String username = parsedUrl.getProperties().getProperty("user");
        final String password = parsedUrl.getProperties().getProperty("password");
        this.shardConfigStore = ShardConfigStoreFactory.getShardConfig(
                parsedUrl.getConfigServerAddress(),
                parsedUrl.getConfigKeyId(),
                username,
                password
        );
        this.shardConfig = this.shardConfigStore.fetchConfig();
        this.connections = new Connection[shardConfig.getPhysNodeCount()];
        for (int i = 0; i < shardConfig.getPhysNodeCount(); i++) {
            ShardConfig.JdbcConfig config = shardConfig.getPhysNodeJdbcConfigs()[i];
            this.connections[i] = DriverManager.getConnection(String.format(config.getJdbcTemplate(), parsedUrl.getDatabase()), parsedUrl.getProperties());
        }
        setAutoCommit(false);
        setDefaultConnectionPropertyValues();
        this.analyzer = AnalyzerFactory.defaultAnalyzer();
        this.shardOps = new ShardOps(
                shardConfig.getPhysNodeCount(),
                shardConfig.getPhysNodeShardRanges(),
                new Murmur3Hash()
        );
        log.debug("---------CONNECTION INFO--------");
        log.debug("version=" + shardConfig.getVersion());
        log.debug("config servers=" + parsedUrl.getConfigServerAddress());
        log.debug("config instance key ID=" + parsedUrl.getConfigKeyId());
        log.debug("phys. node count=" + shardConfig.getPhysNodeCount());
        log.debug("phys. node info=" + Arrays.toString(shardConfig.getPhysNodeJdbcConfigs()));
        log.debug("phys. node shard range=" + Arrays.toString(shardConfig.getPhysNodeShardRanges()));
        log.debug("username=" + username);
        log.debug("database=" + parsedUrl.getDatabase());
        log.debug("properties=" + parsedUrl.getProperties().toString());
        log.debug("table shard config=" + Arrays.toString(shardConfig.getTableShardConfigs()));
        log.debug("--------------------------------");
    }

    private void setDefaultConnectionPropertyValues() throws SQLException {
        if (connections.length != 0) {
            Connection connection = connections[0];
            this.connectionProperties.put(ConnectionProperties.AUTO_COMMIT, connection.getAutoCommit());
            this.connectionProperties.put(ConnectionProperties.HOLDABILITY, connection.getHoldability());
            this.connectionProperties.put(ConnectionProperties.READ_ONLY, connection.isReadOnly());
            this.connectionProperties.put(ConnectionProperties.CATALOG, connection.getCatalog());
            this.connectionProperties.put(ConnectionProperties.TYPE_MAP, connection.getTypeMap());
            this.connectionProperties.put(ConnectionProperties.TRANSACTION_ISOLATION, connection.getTransactionIsolation());
            this.connectionProperties.put(ConnectionProperties.NETWORK_TIMEOUT, connection.getNetworkTimeout());
            this.connectionProperties.put(ConnectionProperties.CLIENT_INFO, connection.getClientInfo());
            this.connectionProperties.put(ConnectionProperties.SCHEMA, connection.getSchema());
        }
    }

    private void checkVersion() throws SQLException {
        lock.lock();
        try {
            long newVersion = shardConfigStore.version();
            if (shardConfig.getVersion() != newVersion) {
                throw new SQLException("Version changed");
            }
        } finally {
            lock.unlock();
        }
    }

    public List<Statement> createStatement(Integer[] selectedShards) throws SQLException {
        checkVersion();
        List<Statement> ret = new ArrayList<>();
        if (selectedShards != null) {
            for (int i : selectedShards) {
                ret.add(connections[i].createStatement());
            }
        }
        return ret;
    }

    public List<Statement> createStatement(Integer[] selectedShards, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkVersion();
        List<Statement> ret = new ArrayList<>();
        if (selectedShards != null) {
            for (int i : selectedShards) {
                ret.add(connections[i].createStatement(resultSetType, resultSetConcurrency));
            }
        }
        return ret;
    }

    public List<Statement> createStatement(Integer[] selectedShards, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkVersion();
        List<Statement> ret = new ArrayList<>();
        if (selectedShards != null) {
            for (int i : selectedShards) {
                ret.add(connections[i].createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
            }
        }
        return ret;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        for (Connection connection : connections) {
            connection.setAutoCommit(autoCommit);
        }
        connectionProperties.put(ConnectionProperties.AUTO_COMMIT, autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.AUTO_COMMIT);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "AutoCommit");
        }
        return (boolean) ret;
    }

    public void setHoldability(int holdability) throws SQLException {
        for (Connection connection : connections) {
            connection.setHoldability(holdability);
        }
        connectionProperties.put(ConnectionProperties.HOLDABILITY, holdability);
    }

    public int getHoldability() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.HOLDABILITY);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "Holdability");
        }
        return (int) ret;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        for (Connection connection : connections) {
            connection.setReadOnly(readOnly);
        }
        connectionProperties.put(ConnectionProperties.READ_ONLY, readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.READ_ONLY);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "ReadOnly");
        }
        return (boolean) ret;
    }

    public void setCatalog(String catalog) throws SQLException {
        for (Connection connection : connections) {
            connection.setCatalog(catalog);
        }
        connectionProperties.put(ConnectionProperties.CATALOG, catalog);
    }

    public String getCatalog() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.CATALOG);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "Catalog");
        }
        return (String) ret;
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        for (Connection connection : connections) {
            connection.setTypeMap(map);
        }
        connectionProperties.put(ConnectionProperties.TYPE_MAP, map);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.TYPE_MAP);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "TypeMap");
        }
        return (Map<String, Class<?>>) ret;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        for (Connection connection : connections) {
            connection.setTransactionIsolation(level);
        }
        connectionProperties.put(ConnectionProperties.TRANSACTION_ISOLATION, level);
    }

    public int getTransactionIsolation() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.TRANSACTION_ISOLATION);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "TransactionIsolation");
        }
        return (int) ret;
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        for (Connection connection : connections) {
            connection.setNetworkTimeout(executor, milliseconds);
        }
        connectionProperties.put(ConnectionProperties.NETWORK_TIMEOUT, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.NETWORK_TIMEOUT);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "NetworkTimeout");
        }
        return (int) ret;
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        for (Connection connection : connections) {
            connection.setClientInfo(name, value);
        }
        Object ret = connectionProperties.get(ConnectionProperties.CLIENT_INFO);
        if (ret == null) {
            Properties properties = new Properties();
            properties.setProperty(name, value);
            connectionProperties.put(ConnectionProperties.CLIENT_INFO, properties);
        } else {
            Properties properties = (Properties) ret;
            properties.setProperty(name, value);
        }
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        for (Connection connection : connections) {
            connection.setClientInfo(properties);
        }
        connectionProperties.put(ConnectionProperties.CLIENT_INFO, properties);
    }

    public String getClientInfo(String name) throws SQLException {
        Object props = connectionProperties.get(ConnectionProperties.CLIENT_INFO);
        if (props == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "ClientInfo");
        } else {
            String ret = ((Properties) props).getProperty(name);
            if (ret == null) {
                throw new SQLException(UNINITIALIZED_VARIABLE + "ClientInfo::" + name);
            }
            return ret;
        }
    }

    public Properties getClientInfo() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.CLIENT_INFO);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "ClientInfo");
        }
        return (Properties) ret;
    }

    public void setSchema(String schema) throws SQLException {
        for (Connection connection : connections) {
            connection.setSchema(schema);
        }
        connectionProperties.put(ConnectionProperties.SCHEMA, schema);
    }

    public String getSchema() throws SQLException {
        Object ret = connectionProperties.get(ConnectionProperties.SCHEMA);
        if (ret == null) {
            throw new SQLException(UNINITIALIZED_VARIABLE + "Schema");
        }
        return (String) ret;
    }

    public void commit() throws SQLException {
        lock.lock();
        try {
            for (Connection connection : connections) {
                connection.commit();
            }
        } finally {
            lock.unlock();
        }
    }

    public void rollback() throws SQLException {
        lock.lock();
        try {
            for (Connection connection : connections) {
                connection.rollback();
            }
        } finally {
            lock.unlock();
        }
    }

    public void close() throws SQLException {
        lock.lock();
        try {
            for (Connection connection : connections) {
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            isClosed = true;
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void abort(Executor executor) throws SQLException {
        lock.lock();
        try {
            for (Connection connection : connections) {
                if (!connection.isClosed()) {
                    connection.abort(executor);
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
