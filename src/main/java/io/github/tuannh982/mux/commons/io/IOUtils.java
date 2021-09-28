package io.github.tuannh982.mux.commons.io;

import io.github.tuannh982.mux.commons.binary.ByteUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IOUtils {
    private static final int BUFFER_SIZE = 4096;

    public static byte[] streamToBytes(InputStream is, long length) throws IOException {
        byte[] ret = new byte[0];
        int len;
        for (
                byte[] array = new byte[BUFFER_SIZE];
                length > 0L && (len = is.read(array, 0, (int) Math.min(BUFFER_SIZE, length))) > 0;
                length -= len
        ) {
            ret = ByteUtils.concat(ret, array, len);
        }
        return ret;
    }

    public static byte[] streamToBytes(InputStream is) throws IOException {
        byte[] ret = new byte[0];
        byte[] array = new byte[BUFFER_SIZE];
        int len;
        while ((len = is.read(array)) > 0) {
            ret = ByteUtils.concat(ret, array, len);
        }
        return ret;
    }

    public static byte[] readerToBytes(Reader reader, long length) throws IOException {
        byte[] ret = new byte[0];
        int len;
        for(
                char[] buffer = new char[BUFFER_SIZE];
                length > 0L && (len = reader.read(buffer, 0, (int) Math.min(BUFFER_SIZE, length))) >= 0;
                length -= len
        ) {
            byte[] data = (new String(buffer, 0, len)).getBytes(StandardCharsets.UTF_8);
            ret = ByteUtils.concat(ret, data);
        }
        return ret;
    }

    public static byte[] readerToBytes(Reader reader) throws IOException {
        byte[] ret = new byte[0];
        char[] buffer = new char[BUFFER_SIZE];
        int len;
        while ((len = reader.read(buffer)) >= 0) {
            byte[] data = (new String(buffer, 0, len)).getBytes(StandardCharsets.UTF_8);
            ret = ByteUtils.concat(ret, data);
        }
        return ret;
    }
}
