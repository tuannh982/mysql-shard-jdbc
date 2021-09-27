package io.github.tuannh982.mux.urlparser;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public class ParserUtilsTest {
    @Test
    public void testParse0() throws SQLException {
        String url = "jdbc:mux://(127.0.0.1:2379,123:4323)[instance1]/testDB?characterEncoding=UTF-8&sessionVariables=sql_mode=ANSI_QUOTES&rewriteBatchedStatements=true";
        ParsedUrl parsedUrl = ParserUtils.parse(url, null);
        assertNotNull(parsedUrl);
        assertEquals("127.0.0.1:2379,123:4323", parsedUrl.getConfigServerAddress());
        assertEquals("instance1", parsedUrl.getConfigKeyId());
        assertEquals("testDB", parsedUrl.getDatabase());
    }
}