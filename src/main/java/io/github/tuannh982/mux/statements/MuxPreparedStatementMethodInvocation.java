package io.github.tuannh982.mux.statements;

import io.github.tuannh982.mux.statements.invocation.MethodInvocation;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum MuxPreparedStatementMethodInvocation implements MethodInvocation {
    // skip first argument since first argument is always param index
    SET_NULL_INT_I_INT_I1(1), // setNull(int i, int i1)
    SET_BOOLEAN_INT_I_BOOLEAN_B(1), // setBoolean(int i, boolean b)
    SET_BYTE_INT_I_BYTE_B(1), // setByte(int i, byte b)
    SET_SHORT_INT_I_SHORT_I1(1), // setShort(int i, short i1)
    SET_INT_INT_I_INT_I1(1), // setInt(int i, int i1)
    SET_LONG_INT_I_LONG_L(1), // setLong(int i, long l)
    SET_FLOAT_INT_I_FLOAT_V(1), // setFloat(int i, float v)
    SET_DOUBLE_INT_I_DOUBLE_V(1), // setDouble(int i, double v)
    SET_BIGDECIMAL_INT_I_BIGDECIMAL_BIGDECIMAL(1), // setBigDecimal(int i, BigDecimal bigDecimal)
    SET_STRING_INT_I_STRING_S(1), // setString(int i, String s)
    SET_BYTES_INT_I_BYTEARR_BYTES(1), // setBytes(int i, byte[] bytes)
    SET_DATE_INT_I_DATE_DATE(1), // setDate(int i, Date date)
    SET_TIME_INT_I_TIME_TIME(1), // setTime(int i, Time time)
    SET_TIMESTAMP_INT_I_TIMESTAMP_TIMESTAMP(1), // setTimestamp(int i, Timestamp timestamp)
    SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1(2), // setAsciiStream(int i, InputStream inputStream, int i1)
    SET_UNICODESTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1(2), // setUnicodeStream(int i, InputStream inputStream, int i1)
    SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1(2), // setBinaryStream(int i, InputStream inputStream, int i1)
    SET_OBJECT_INT_I_OBJECT_O_INT_I1(2), // setObject(int i, Object o, int i1)
    SET_OBJECT_INT_I_OBJECT_O(1), // setObject(int i, Object o)
    SET_CHARACTERSTREAM_INT_I_READER_READER_INT_I1(2), // setCharacterStream(int i, Reader reader, int i1)
    SET_REF_INT_I_REF_REF(1), // setRef(int i, Ref ref)
    SET_BLOB_INT_I_BLOB_BLOB(1), // setBlob(int i, Blob blob)
    SET_CLOB_INT_I_CLOB_CLOB(1), // setClob(int i, Clob clob)
    SET_ARRAY_INT_I_ARRAY_ARRAY(1), // setArray(int i, Array array)
    SET_DATE_INT_I_DATE_DATE_CALENDAR_CALENDAR(2), // setDate(int i, Date date, Calendar calendar)
    SET_TIME_INT_I_TIME_TIME_CALENDAR_CALENDAR(2), // setTime(int i, Time time, Calendar calendar)
    SET_TIMESTAMP_INT_I_TIMESTAMP_TIMESTAMP_CALENDAR_CALENDAR(2), // setTimestamp(int i, Timestamp timestamp, Calendar calendar)
    SET_NULL_INT_I_INT_I1_STRING_S(2), // setNull(int i, int i1, String s)
    SET_URL_INT_I_URL_URL(1), // setURL(int i, URL url)
    SET_ROWID_INT_I_ROWID_ROWID(1), // setRowId(int i, RowId rowId)
    SET_NSTRING_INT_I_STRING_S(1), // setNString(int i, String s)
    SET_NCHARACTERSTREAM_INT_I_READER_READER_LONG_L(2), // setNCharacterStream(int i, Reader reader, long l)
    SET_NCLOB_INT_I_NCLOB_NCLOB(1), // setNClob(int i, NClob nClob)
    SET_CLOB_INT_I_READER_READER_LONG_L(2), // setClob(int i, Reader reader, long l)
    SET_BLOB_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L(2), // setBlob(int i, InputStream inputStream, long l)
    SET_NCLOB_INT_I_READER_READER_LONG_L(2), // setNClob(int i, Reader reader, long l)
    SET_SQLXML_INT_I_SQLXML_SQLXML(1), // setSQLXML(int i, SQLXML sqlxml)
    SET_OBJECT_INT_I_OBJECT_O_INT_I1_INT_I2(3), // setObject(int i, Object o, int i1, int i2)
    SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L(2), // setAsciiStream(int i, InputStream inputStream, long l)
    SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L(2), // setBinaryStream(int i, InputStream inputStream, long l)
    SET_CHARACTERSTREAM_INT_I_READER_READER_LONG_L(2), // setCharacterStream(int i, Reader reader, long l)
    SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM(1), // setAsciiStream(int i, InputStream inputStream)
    SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM(1), // setBinaryStream(int i, InputStream inputStream)
    SET_CHARACTERSTREAM_INT_I_READER_READER(1), // setCharacterStream(int i, Reader reader)
    SET_NCHARACTERSTREAM_INT_I_READER_READER(1), // setNCharacterStream(int i, Reader reader)
    SET_CLOB_INT_I_READER_READER(1), // setClob(int i, Reader reader)
    SET_BLOB_INT_I_INPUTSTREAM_INPUTSTREAM(1), // setBlob(int i, InputStream inputStream)
    SET_NCLOB_INT_I_READER_READER(1) // setNClob(int i, Reader reader)
    ;

    private final int numberOfArgs;
}
