package io.github.tuannh982.mux.shard.analyzer.simplerouting;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.sql.SQLException;

@AllArgsConstructor
public class SQLExceptionRTE extends RuntimeException {
    public SQLExceptionRTE(String s) {
        inner = new SQLException(s);
    }

    @Getter
    private final SQLException inner;
}
