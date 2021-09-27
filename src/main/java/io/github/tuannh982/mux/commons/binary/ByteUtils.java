package io.github.tuannh982.mux.commons.binary;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * NOTE: all operations are follow Big Endian order
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ByteUtils {
    public static short readShort(byte[] arr, int offset) {
        return  (short) (((short) (arr[offset   ] & 0xFF) <<  8) |
                        (arr[offset + 1] & 0xFF));
    }

    public static void writeShort(byte[] arr, int offset, short value) {
        arr[offset    ] = (byte)(value >>  8);
        arr[offset + 1] = (byte)value;
    }

    public static int readInt(byte[] arr, int offset) {
        return  ((arr[offset    ] & 0xFF) << 24) |
                ((arr[offset + 1] & 0xFF) << 16) |
                ((arr[offset + 2] & 0xFF) <<  8) |
                (arr[offset + 3] & 0xFF);
    }

    public static void writeInt(byte[] arr, int offset, int value) {
        arr[offset    ] = (byte)(value >> 24);
        arr[offset + 1] = (byte)(value >> 16);
        arr[offset + 2] = (byte)(value >>  8);
        arr[offset + 3] = (byte)value;
    }

    public static long readLong(byte[] arr, int offset) {
        return  ((long) (arr[offset    ] & 0xFF) << 56) |
                ((long) (arr[offset + 1] & 0xFF) << 48) |
                ((long) (arr[offset + 2] & 0xFF) << 40) |
                ((long) (arr[offset + 3] & 0xFF) << 32) |
                ((long) (arr[offset + 4] & 0xFF) << 24) |
                ((long) (arr[offset + 5] & 0xFF) << 16) |
                ((long) (arr[offset + 6] & 0xFF) <<  8) |
                ((long) arr[offset + 7] & 0xFF);
    }

    public static void writeLong(byte[] arr, int offset, long value) {
        arr[offset    ] = (byte)(value >> 56);
        arr[offset + 1] = (byte)(value >> 48);
        arr[offset + 2] = (byte)(value >> 40);
        arr[offset + 3] = (byte)(value >> 32);
        arr[offset + 4] = (byte)(value >> 24);
        arr[offset + 5] = (byte)(value >> 16);
        arr[offset + 6] = (byte)(value >>  8);
        arr[offset + 7] = (byte)value;
    }

    public static void read(byte[] arr, int offset, byte[] value) {
        System.arraycopy(arr, offset, value, 0, value.length);
    }

    public static void write(byte[] arr, int offset, byte[] value) {
        System.arraycopy(value, 0, arr, offset, value.length);
    }

    public static byte[] concat(byte[] l, byte[] r) {
        byte[] ret = new byte[l.length + r.length];
        System.arraycopy(l, 0, ret, 0, l.length);
        System.arraycopy(r, 0, ret, l.length, r.length);
        return ret;
    }

    public static byte[] slice(byte[] arr, int from, int to) {
        byte[] ret = new byte[to - from];
        System.arraycopy(arr, from, ret, 0, to - from);
        return ret;
    }
}
