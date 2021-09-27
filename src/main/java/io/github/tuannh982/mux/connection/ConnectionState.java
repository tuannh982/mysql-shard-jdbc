package io.github.tuannh982.mux.connection;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConnectionState {
    public enum State {
        BLANK,
        UNCOMMITTED,
        COMMITTED;
    }

    private State state = State.BLANK;
}
