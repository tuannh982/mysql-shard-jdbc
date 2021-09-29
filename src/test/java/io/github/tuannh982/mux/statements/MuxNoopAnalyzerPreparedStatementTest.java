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
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("java:S5786")
public class MuxNoopAnalyzerPreparedStatementTest {
    private static final Random random = new Random();
    private static MockedStatic<ShardConfigStoreFactory> mockedShardConfigStoreFactory;
    private static MockedStatic<AnalyzerFactory> mockedAnalyzerFactory;
    private static String database;
    private static String username;
    private static String password;

    @RegisterExtension
    public static DockerComposeExtension docker = DockerComposeExtension.builder()
            .file("src/test/resources/docker-compose-only-db.yaml")
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

    private void executeSqlNoReturn(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * executeUpdate()
     * executeQuery()
     * modify table
     * insert data into table
     * update data
     * read data
     */
    @Test
    public void testExecuteModifyTableAndInsertAndUpdate() throws SQLException {
        String tableName = "contacts";
        String connectionString = String.format("jdbc:mux://(127.0.0.1:12345)[keyId01]/%s?characterEncoding=UTF-8&sessionVariables=sql_mode=ANSI_QUOTES&rewriteBatchedStatements=true", database);
        String connectionStringDb1 = String.format("jdbc:mysql://127.0.0.1:20306/%s?characterEncoding=UTF-8&sessionVariables=sql_mode=ANSI_QUOTES&rewriteBatchedStatements=true", database);
        //-----create table---------------------------------------------------------------------------------------------
        create_table: {
            Connection connection = DriverManager.getConnection(connectionString, username, password);
            executeSqlNoReturn(connection, "drop table if exists contacts;");
            executeSqlNoReturn(
                    connection,
                    "CREATE TABLE contacts (\n" +
                            "\tcontact_id integer primary key,\n" +
                            "\tfirst_name varchar(255) not null,\n" +
                            "\tlast_name varchar(255) not null,\n" +
                            "\temail varchar(255) not null unique,\n" +
                            "\tphone varchar(255) not null unique\n" +
                            ");"
            );
            connection.commit();
            connection.close();
        }
        //-----modify table---------------------------------------------------------------------------------------------
        modify_table: {
            Connection connection = DriverManager.getConnection(connectionString, username, password);
            executeSqlNoReturn(connection,"alter table contacts drop column phone, drop column email;");
            executeSqlNoReturn(connection,"alter table contacts add column contact_str varchar(50);");
            connection.commit();
            connection.close();
        }
        //-----generate data for insertion------------------------------------------------------------------------------
        int toBeInsertedCount = random.nextInt(64) + 64;
        Object[][] toBeInsertedObjects = new Object[toBeInsertedCount][];
        for (int i = 0; i < toBeInsertedCount; i++) {
            toBeInsertedObjects[i] = new Object[] {
                    i,
                    RandomStringUtils.random(16),
                    RandomStringUtils.random(16),
                    RandomStringUtils.random(16)
            };
        }
        //-----insert data into table-----------------------------------------------------------------------------------
        insert_data_into_table: {
            Connection connection = DriverManager.getConnection(connectionString, username, password);
            for (Object[] value : toBeInsertedObjects) {
                PreparedStatement statement = connection.prepareStatement(
                        "insert into contacts(contact_id, first_name, last_name, contact_str) values (?, ?, ?, ?);"
                );
                statement.setInt(1, (Integer) value[0]);
                statement.setString(2, (String) value[1]);
                statement.setString(3, (String) value[2]);
                statement.setString(4, (String) value[3]);
                int affected = statement.executeUpdate();
                assertEquals(1, affected);
                statement.close();
            }
            connection.commit(); // commit after insert
            connection.close();
        }
        //-----read data from table-------------------------------------------------------------------------------------
        read_data_from_table: {
            Connection connection = DriverManager.getConnection(connectionStringDb1, username, password);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from contacts;");
            int index = 0;
            while (resultSet.next()) {
                int contactId = resultSet.getInt(1);
                String firstName = resultSet.getString(2);
                String lastName = resultSet.getString(3);
                String contactStr = resultSet.getString(4);
                assertEquals(contactId, toBeInsertedObjects[index][0]);
                assertEquals(firstName, toBeInsertedObjects[index][1]);
                assertEquals(lastName, toBeInsertedObjects[index][2]);
                assertEquals(contactStr, toBeInsertedObjects[index][3]);
                index++;
            }
            assertEquals(index, toBeInsertedObjects.length);
            resultSet.close();
            statement.close();
            connection.close();
        }
        //-----update data and verify-----------------------------------------------------------------------------------
        update_data_and_verify: {
            Connection connection = DriverManager.getConnection(connectionString, username, password);
            PreparedStatement updateStatement = connection.prepareStatement(
                    "update contacts set first_name = ? where contact_id >= ?;"
            );
            final String updatedInfo = RandomStringUtils.random(16);
            updateStatement.setString(1, updatedInfo);
            final int updateFromIdx = (int) toBeInsertedObjects[random.nextInt(toBeInsertedCount)][0];
            updateStatement.setInt(2, updateFromIdx);
            int affected = updateStatement.executeUpdate();
            assertEquals(toBeInsertedCount - updateFromIdx, affected);
            updateStatement.close();
            connection.commit();
            PreparedStatement queryStatement = connection.prepareStatement(
                    "select * from contacts where contact_id >= ?;"
            );
            queryStatement.setInt(1, updateFromIdx);
            int count = 0;
            try (ResultSet resultSet = queryStatement.executeQuery()) {
                while (resultSet.next()) {
                    String firstName = resultSet.getString(2);
                    assertEquals(updatedInfo, firstName);
                    count++;
                }
            }
            assertEquals(toBeInsertedCount - updateFromIdx, count);
            queryStatement.close();
            connection.close();
        }
        //-----cleanup table--------------------------------------------------------------------------------------------
        cleanup_table: {
            Connection connection = DriverManager.getConnection(connectionString, username, password);
            executeSqlNoReturn(connection, "drop table if exists contacts;");
            connection.commit();
            connection.close();
        }
    }

}