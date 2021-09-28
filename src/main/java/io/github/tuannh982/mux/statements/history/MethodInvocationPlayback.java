package io.github.tuannh982.mux.statements.history;

import java.sql.SQLException;

public interface MethodInvocationPlayback {
    void playback() throws SQLException;
}
