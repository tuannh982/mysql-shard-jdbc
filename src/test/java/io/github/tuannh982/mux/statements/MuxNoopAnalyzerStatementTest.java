package io.github.tuannh982.mux.statements;

import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import io.github.tuannh982.mux.DriverLoader;
import io.github.tuannh982.mux.config.ShardConfig;
import io.github.tuannh982.mux.config.ShardConfigStore;
import io.github.tuannh982.mux.config.ShardConfigStoreFactory;
import io.github.tuannh982.mux.shard.analyzer.AnalyzerFactory;
import io.github.tuannh982.mux.shard.analyzer.NoopAnalyzer;
import io.github.tuannh982.mux.utils.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.*;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("java:S5786")
public class MuxNoopAnalyzerStatementTest {
    private static MockedStatic<ShardConfigStoreFactory> mockedShardConfigStoreFactory;
    private static MockedStatic<AnalyzerFactory> mockedAnalyzerFactory;
    private static String database;
    private static String username;
    private static String password;

    @RegisterExtension
    public static DockerComposeExtension docker = DockerComposeExtension.builder()
            .file("src/test/resources/docker-compose.yaml")
            .waitingForService("db1", HealthChecks.toHaveAllPortsOpen())
            .waitingForService("db2", HealthChecks.toHaveAllPortsOpen())
            .waitingForService("db3", HealthChecks.toHaveAllPortsOpen())
            .build();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeAll
    public static void setup() {
        DriverLoader.load();
        mockedShardConfigStoreFactory = Mockito.mockStatic(ShardConfigStoreFactory.class);
        mockedShardConfigStoreFactory.when(() -> ShardConfigStoreFactory.getShardConfig(
                any(String.class),
                any(String.class),
                any(String.class),
                any(String.class)
        )).thenReturn(new ShardConfigStore() {
            @Override
            public long version() {
                return 2;
            }

            @Override
            public ShardConfig fetchConfig() {
                return new ShardConfig(
                        2,
                        2,
                        new ShardConfig.JdbcConfig[] {
                                new ShardConfig.JdbcConfig(
                                        "com.mysql.cj.jdbc.Driver",
                                        "jdbc:mysql://127.0.0.1:20306/%s"
                                ),
                                new ShardConfig.JdbcConfig(
                                        "com.mysql.cj.jdbc.Driver",
                                        "jdbc:mysql://127.0.0.1:20307/%s"
                                ),
                                new ShardConfig.JdbcConfig(
                                        "com.mysql.cj.jdbc.Driver",
                                        "jdbc:mysql://127.0.0.1:20308/%s"
                                ),
                        },
                        new ShardConfig.Range[] {
                                new ShardConfig.Range(Long.MIN_VALUE, Long.MIN_VALUE / 2),
                                new ShardConfig.Range(Long.MIN_VALUE / 2, Long.MAX_VALUE / 2),
                                new ShardConfig.Range(Long.MAX_VALUE / 2, Long.MAX_VALUE),
                        },
                        new ShardConfig.TableConfig[] {}
                );
            }
        });
        mockedAnalyzerFactory = Mockito.mockStatic(AnalyzerFactory.class);
        mockedAnalyzerFactory.when(AnalyzerFactory::defaultAnalyzer).thenReturn(new NoopAnalyzer());
        database = "test_database";
        username = "test_user";
        password = "test_password";
    }

    @AfterAll
    public static void cleanup() {
        mockedShardConfigStoreFactory.close();
        mockedAnalyzerFactory.close();
    }

    /**
     * execute(String sql)
     * create table
     */
    @Test
    public void testExecuteCreateTable() throws SQLException {
        String tableName = "contacts";
        String showTableResultPrefix =
                "CREATE TABLE \"contacts\" (\n" +
                        " \"contact_id\" int NOT NULL,\n" +
                        " \"first_name\" varchar(255) NOT NULL,\n" +
                        " \"last_name\" varchar(255) NOT NULL,\n" +
                        " \"email\" varchar(255) NOT NULL,\n" +
                        " \"phone\" varchar(255) NOT NULL,";
        String sql =
                "CREATE TABLE contacts (\n" +
                        "\tcontact_id integer primary key,\n" +
                        "\tfirst_name varchar(255) not null,\n" +
                        "\tlast_name varchar(255) not null,\n" +
                        "\temail varchar(255) not null unique,\n" +
                        "\tphone varchar(255) not null unique\n" +
                        ");";
        String connectionString = String.format("jdbc:mux://(127.0.0.1:12345)[keyId01]/%s?characterEncoding=UTF-8&sessionVariables=sql_mode=ANSI_QUOTES&rewriteBatchedStatements=true", database);
        Connection connection = DriverManager.getConnection(connectionString, username, password);
        Statement statement = connection.createStatement();
        boolean isResultSetReturned = statement.execute(sql);
        assertFalse(isResultSetReturned);
        statement.close();
        connection.close();
        // first database, according to NoopAnalyzer
        connectionString = String.format("jdbc:mysql://127.0.0.1:20306/%s?characterEncoding=UTF-8&sessionVariables=sql_mode=ANSI_QUOTES&rewriteBatchedStatements=true", database);
        connection = DriverManager.getConnection(connectionString, username, password);
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show create table contacts;");
        assertTrue(resultSet.next());
        String readTableName = resultSet.getString(1);
        assertEquals(tableName, readTableName);
        String readDdl = resultSet.getString(2);
        assertTrue(
                StringUtils.collapseWhitespace(readDdl).toLowerCase(Locale.ROOT)
                        .startsWith(StringUtils.collapseWhitespace(showTableResultPrefix).toLowerCase(Locale.ROOT))
        );
    }
}