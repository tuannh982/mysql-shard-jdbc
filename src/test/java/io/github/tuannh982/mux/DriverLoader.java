package io.github.tuannh982.mux;

public class DriverLoader {
    public static void load() {
        try {
            Class.forName(io.github.tuannh982.mux.Driver.class.getName());
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }
}
