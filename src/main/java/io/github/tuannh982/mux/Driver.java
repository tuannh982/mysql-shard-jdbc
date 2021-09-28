package io.github.tuannh982.mux;

import io.github.tuannh982.mux.connection.MuxConnection;
import io.github.tuannh982.mux.urlparser.ParsedUrl;
import io.github.tuannh982.mux.urlparser.ParserUtils;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

@SuppressWarnings("java:S2176")
public class Driver implements java.sql.Driver {
    private static final Driver INSTANCE = new Driver();
    private static volatile boolean registered = false;

    static {
        try {
            load();
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public static synchronized Driver load() throws SQLException {
        if (!registered) {
            registered = true;
            DriverManager.registerDriver(INSTANCE);
        }
        return INSTANCE;
    }

    public static synchronized void unload() throws SQLException {
        if (registered) {
            registered = false;
            DriverManager.deregisterDriver(INSTANCE);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        ParsedUrl parsedUrl = ParserUtils.parse(url, info);
        return MuxConnection.newConnection(parsedUrl);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return ParserUtils.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // TODO
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return Version.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return Version.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Feature not supported");
    }
}
