package io.github.tuannh982.mux.statements.history;

import java.sql.SQLException;

public interface ParamInvocationPlayback {
    void playback() throws SQLException;
}
