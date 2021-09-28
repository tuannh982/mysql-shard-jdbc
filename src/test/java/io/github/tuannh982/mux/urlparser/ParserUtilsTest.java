package io.github.tuannh982.mux.urlparser;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("java:S5786")
public class ParserUtilsTest {
    @Test
    public void testParse0() throws SQLException {
        String url = "jdbc:mux://(127.0.0.1:12345)[keyId01]/test_database?characterEncoding=UTF-8&sessionVariables=sql_mode=ANSI_QUOTES&rewriteBatchedStatements=true";
        ParsedUrl parsedUrl = ParserUtils.parse(url, null);
        assertNotNull(parsedUrl);
        assertEquals("127.0.0.1:12345", parsedUrl.getConfigServerAddress());
        assertEquals("keyId01", parsedUrl.getConfigKeyId());
        assertEquals("test_database", parsedUrl.getDatabase());
    }
}