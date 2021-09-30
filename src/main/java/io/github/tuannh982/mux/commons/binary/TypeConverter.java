package io.github.tuannh982.mux.commons.binary;

import io.github.tuannh982.mux.commons.io.IOUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import static io.github.tuannh982.mux.connection.Constants.OPERATION_NOT_SUPPORTED;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TypeConverter {
    public static byte[] SQLTypeToBytes(Object o) throws SQLException {
        if (o == null) {
            return null;
        } else if (o instanceof Boolean) {
            Boolean b = (Boolean) o;
            return new byte[] {(byte) (Boolean.TRUE.equals(b) ? 1 : 0)};
        } else if (o instanceof Byte) {
            Byte b = (Byte) o;
            return new byte[] {b};
        } else if (o instanceof Short) {
            Short i1 = (Short) o;
            byte[] bArr = new byte[Short.BYTES];
            ByteUtils.writeShort(bArr, 0, i1);
            return bArr;
        } else if (o instanceof Integer) {
            Integer i1 = (Integer) o;
            byte[] bArr = new byte[Integer.BYTES];
            ByteUtils.writeInt(bArr, 0, i1);
            return bArr;
        } else if (o instanceof Long) {
            Long l = (Long) o;
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, l);
            return bArr;
        } else if (o instanceof Float) {
            Float v = (Float) o;
            int intValue = Float.floatToIntBits(v);
            byte[] bArr = new byte[Integer.BYTES];
            ByteUtils.writeInt(bArr, 0, intValue);
            return bArr;
        } else if (o instanceof Double) {
            Double v = (Double) o;
            long longValue = Double.doubleToLongBits(v);
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, longValue);
            return bArr;
        } else if (o instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) o;
            return bigDecimal.unscaledValue().toByteArray();
        } else if (o instanceof String) {
            String s = (String) o;
            return s.getBytes(StandardCharsets.UTF_8);
        } else if (o instanceof byte[]) {
            return (byte[]) o;
        } else if (o instanceof Date) {
            Date date = (Date) o;
            long ts = date.getTime();
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, ts);
            return bArr;
        } else if (o instanceof Time) {
            Time date = (Time) o;
            long ts = date.getTime();
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, ts);
            return bArr;
        } else if (o instanceof Timestamp) {
            Timestamp date = (Timestamp) o;
            long ts = date.getTime();
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, ts);
            return bArr;
        } else if (o instanceof InputStream) {
            try {
                InputStream inputStream = (InputStream) o;
                return IOUtils.streamToBytes(inputStream);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof Reader) {
            try {
                Reader reader = (Reader) o;
                return IOUtils.readerToBytes(reader);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof Ref) {
            // will not be supported
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        } else if (o instanceof Blob) {
            try {
                Blob blob = (Blob) o;
                return IOUtils.streamToBytes(blob.getBinaryStream(), blob.length());
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof NClob) {
            try {
                NClob clob = (NClob) o;
                return IOUtils.readerToBytes(clob.getCharacterStream(), clob.length());
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof Clob) {
            try {
                Clob clob = (Clob) o;
                return IOUtils.readerToBytes(clob.getCharacterStream(), clob.length());
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof Array) {
            // will not be supported
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        } else if (o instanceof URL) {
            URL url = (URL) o;
            return url.toString().getBytes(StandardCharsets.UTF_8);
        } else if (o instanceof RowId) {
            // will not be supported
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        } else if (o instanceof SQLXML) {
            try {
                SQLXML sqlxml = (SQLXML) o;
                return IOUtils.readerToBytes(sqlxml.getCharacterStream());
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else {
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        }
    }
}
