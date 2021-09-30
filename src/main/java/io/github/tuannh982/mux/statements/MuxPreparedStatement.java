package io.github.tuannh982.mux.statements;

import io.github.tuannh982.mux.commons.binary.ByteUtils;
import io.github.tuannh982.mux.commons.io.IOUtils;
import io.github.tuannh982.mux.commons.tuple.Tuple2;
import io.github.tuannh982.mux.connection.MuxConnection;
import io.github.tuannh982.mux.statements.invocation.MethodInvocationEntry;
import io.github.tuannh982.mux.statements.invocation.PreparedStatementMethodInvocation;
import io.github.tuannh982.mux.statements.resultset.MuxResultSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static io.github.tuannh982.mux.connection.Constants.OPERATION_NOT_SUPPORTED;
import static io.github.tuannh982.mux.statements.MuxPreparedStatementMethodInvocation.*;

public class MuxPreparedStatement extends MuxStatement implements PreparedStatement {
    private final ConstructorType preparedStatementConstructorType;
    //------------------------------------------------------------------------------------------------------------------
    private final PreparedStatementMethodInvocation methodInvocationState = new PreparedStatementMethodInvocation();
    //------------------------------------------------------------------------------------------------------------------
    protected String sql;
    protected int autoGeneratedKeys;
    protected int[] columnIndexes = new int[0];
    protected String[] columnNames = new String[0];
    //------------------------------------------------------------------------------------------------------------------

    private enum ConstructorType {
        PREP_STATEMENT_S,
        PREP_STATEMENT_S_II,
        PREP_STATEMENT_S_III,
        PREP_STATEMENT_S_AUTO_GENERATED_KEY,
        PREP_STATEMENT_S_COLUMN_IDX,
        PREP_STATEMENT_S_COLUMN_NAMES
    }

    public MuxPreparedStatement(MuxConnection connection, String sql) {
        super();
        super.init(connection);
        this.sql = sql;
        this.preparedStatementConstructorType = ConstructorType.PREP_STATEMENT_S;
    }

    public MuxPreparedStatement(MuxConnection connection, String sql, int resultSetType, int resultSetConcurrency) {
        super();
        super.init(connection);
        this.sql = sql;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.preparedStatementConstructorType = ConstructorType.PREP_STATEMENT_S_II;
    }

    public MuxPreparedStatement(MuxConnection connection, String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super();
        super.init(connection);
        this.sql = sql;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.preparedStatementConstructorType = ConstructorType.PREP_STATEMENT_S_III;
    }

    public MuxPreparedStatement(MuxConnection connection, String sql, int autoGeneratedKeys) {
        super();
        super.init(connection);
        this.sql = sql;
        this.autoGeneratedKeys = autoGeneratedKeys;
        this.preparedStatementConstructorType = ConstructorType.PREP_STATEMENT_S_AUTO_GENERATED_KEY;
    }

    public MuxPreparedStatement(MuxConnection connection, String sql, int[] columnIndexes) {
        super();
        super.init(connection);
        this.sql = sql;
        this.columnIndexes = columnIndexes;
        this.preparedStatementConstructorType = ConstructorType.PREP_STATEMENT_S_COLUMN_IDX;
    }

    public MuxPreparedStatement(MuxConnection connection, String sql, String[] columnNames) {
        super();
        super.init(connection);
        this.sql = sql;
        this.columnNames = columnNames;
        this.preparedStatementConstructorType = ConstructorType.PREP_STATEMENT_S_COLUMN_NAMES;
    }

    //-------------------------
    private void preparedStatementPreparation(Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult) throws SQLException {
        switch (preparedStatementConstructorType) {
            case PREP_STATEMENT_S:
                statements = connection.getInternal().createPreparedStatement(analyzedResult);
                break;
            case PREP_STATEMENT_S_II:
                statements = connection.getInternal().createPreparedStatement(analyzedResult, resultSetType, resultSetConcurrency);
                break;
            case PREP_STATEMENT_S_III:
                statements = connection.getInternal().createPreparedStatement(analyzedResult, resultSetType, resultSetConcurrency, resultSetHoldability);
                break;
            case PREP_STATEMENT_S_AUTO_GENERATED_KEY:
                statements = connection.getInternal().createPreparedStatement(analyzedResult, autoGeneratedKeys);
                break;
            case PREP_STATEMENT_S_COLUMN_IDX:
                statements = connection.getInternal().createPreparedStatement(analyzedResult, columnIndexes);
                break;
            case PREP_STATEMENT_S_COLUMN_NAMES:
                statements = connection.getInternal().createPreparedStatement(analyzedResult, columnNames);
                break;
            default:
                throw new SQLException("Unexpected type " + preparedStatementConstructorType);
        }
    }

    @SuppressWarnings("java:S1479")
    private void playback(PreparedStatementMethodInvocation analyzed, PreparedStatement statement) throws SQLException {
        Map<Integer, MethodInvocationEntry<MuxPreparedStatementMethodInvocation>> state = analyzed.getState();
        for (Map.Entry<Integer, MethodInvocationEntry<MuxPreparedStatementMethodInvocation>> entry : state.entrySet()) {
            int index = entry.getKey();
            MethodInvocationEntry<MuxPreparedStatementMethodInvocation> invokeCommand = entry.getValue();
            Object[] v = invokeCommand.getParams();
            switch (invokeCommand.getMethod()) {
                case SET_NULL_INT_I_INT_I1: /* setNull(int i, int i1) */
                    statement.setNull(index, (Integer) v[0]);
                    break;
                case SET_BOOLEAN_INT_I_BOOLEAN_B: /* setBoolean(int i, boolean b) */
                    statement.setBoolean(index, (Boolean) v[0]);
                    break;
                case SET_BYTE_INT_I_BYTE_B: /* setByte(int i, byte b) */
                    statement.setByte(index, (Byte) v[0]);
                    break;
                case SET_SHORT_INT_I_SHORT_I1: /* setShort(int i, short i1) */
                    statement.setShort(index, (Short) v[0]);
                    break;
                case SET_INT_INT_I_INT_I1: /* setInt(int i, int i1) */
                    statement.setInt(index, (Integer) v[0]);
                    break;
                case SET_LONG_INT_I_LONG_L: /* setLong(int i, long l) */
                    statement.setLong(index, (Long) v[0]);
                    break;
                case SET_FLOAT_INT_I_FLOAT_V: /* setFloat(int i, float v) */
                    statement.setFloat(index, (Float) v[0]);
                    break;
                case SET_DOUBLE_INT_I_DOUBLE_V: /* setDouble(int i, double v) */
                    statement.setDouble(index, (Double) v[0]);
                    break;
                case SET_BIGDECIMAL_INT_I_BIGDECIMAL_BIGDECIMAL: /* setBigDecimal(int i, BigDecimal bigDecimal) */
                    statement.setBigDecimal(index, (BigDecimal) v[0]);
                    break;
                case SET_STRING_INT_I_STRING_S: /* setString(int i, String s) */
                    statement.setString(index, (String) v[0]);
                    break;
                case SET_BYTES_INT_I_BYTEARR_BYTES: /* setBytes(int i, byte[] bytes) */
                    statement.setBytes(index, (byte[]) v[0]);
                    break;
                case SET_DATE_INT_I_DATE_DATE: /* setDate(int i, Date date) */
                    statement.setDate(index, (Date) v[0]);
                    break;
                case SET_TIME_INT_I_TIME_TIME: /* setTime(int i, Time time) */
                    statement.setTime(index, (Time) v[0]);
                    break;
                case SET_TIMESTAMP_INT_I_TIMESTAMP_TIMESTAMP: /* setTimestamp(int i, Timestamp timestamp) */
                    statement.setTimestamp(index, (Timestamp) v[0]);
                    break;
                case SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1: /* setAsciiStream(int i, InputStream inputStream, int i1) */
                    statement.setAsciiStream(index, (InputStream) v[0], (Integer) v[1]);
                    break;
                case SET_UNICODESTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1: /* setUnicodeStream(int i, InputStream inputStream, int i1) */
                    statement.setUnicodeStream(index, (InputStream) v[0], (Integer) v[1]);
                    break;
                case SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1: /* setBinaryStream(int i, InputStream inputStream, int i1) */
                    statement.setBinaryStream(index, (InputStream) v[0], (Integer) v[1]);
                    break;
                case SET_OBJECT_INT_I_OBJECT_O_INT_I1: /* setObject(int i, Object o, int i1) */
                    statement.setObject(index, v[0], (Integer) v[1]);
                    break;
                case SET_OBJECT_INT_I_OBJECT_O: /* setObject(int i, Object o) */
                    statement.setObject(index, v[0]);
                    break;
                case SET_CHARACTERSTREAM_INT_I_READER_READER_INT_I1: /* setCharacterStream(int i, Reader reader, int i1) */
                    statement.setCharacterStream(index, (Reader) v[0], (Integer) v[1]);
                    break;
                case SET_REF_INT_I_REF_REF: /* setRef(int i, Ref ref) */
                    statement.setRef(index, (Ref) v[0]);
                    break;
                case SET_BLOB_INT_I_BLOB_BLOB: /* setBlob(int i, Blob blob) */
                    statement.setBlob(index, (Blob) v[0]);
                    break;
                case SET_CLOB_INT_I_CLOB_CLOB: /* setClob(int i, Clob clob) */
                    statement.setClob(index, (Clob) v[0]);
                    break;
                case SET_ARRAY_INT_I_ARRAY_ARRAY: /* setArray(int i, Array array) */
                    statement.setArray(index, (Array) v[0]);
                    break;
                case SET_DATE_INT_I_DATE_DATE_CALENDAR_CALENDAR: /* setDate(int i, Date date, Calendar calendar) */
                    statement.setDate(index, (Date) v[0], (Calendar) v[1]);
                    break;
                case SET_TIME_INT_I_TIME_TIME_CALENDAR_CALENDAR: /* setTime(int i, Time time, Calendar calendar) */
                    statement.setTime(index, (Time) v[0], (Calendar) v[1]);
                    break;
                case SET_TIMESTAMP_INT_I_TIMESTAMP_TIMESTAMP_CALENDAR_CALENDAR: /* setTimestamp(int i, Timestamp timestamp, Calendar calendar) */
                    statement.setTimestamp(index, (Timestamp) v[0], (Calendar) v[1]);
                    break;
                case SET_NULL_INT_I_INT_I1_STRING_S: /* setNull(int i, int i1, String s) */
                    statement.setNull(index, (Integer) v[0], (String) v[1]);
                    break;
                case SET_URL_INT_I_URL_URL: /* setURL(int i, URL url) */
                    statement.setURL(index, (URL) v[0]);
                    break;
                case SET_ROWID_INT_I_ROWID_ROWID: /* setRowId(int i, RowId rowId) */
                    statement.setRowId(index, (RowId) v[0]);
                    break;
                case SET_NSTRING_INT_I_STRING_S: /* setNString(int i, String s) */
                    statement.setNString(index, (String) v[0]);
                    break;
                case SET_NCHARACTERSTREAM_INT_I_READER_READER_LONG_L: /* setNCharacterStream(int i, Reader reader, long l) */
                    statement.setNCharacterStream(index, (Reader) v[0], (Long) v[1]);
                    break;
                case SET_NCLOB_INT_I_NCLOB_NCLOB: /* setNClob(int i, NClob nClob) */
                    statement.setNClob(index, (NClob) v[0]);
                    break;
                case SET_CLOB_INT_I_READER_READER_LONG_L: /* setClob(int i, Reader reader, long l) */
                    statement.setClob(index, (Reader) v[0], (Long) v[1]);
                    break;
                case SET_BLOB_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L: /* setBlob(int i, InputStream inputStream, long l) */
                    statement.setBlob(index, (InputStream) v[0], (Long) v[1]);
                    break;
                case SET_NCLOB_INT_I_READER_READER_LONG_L: /* setNClob(int i, Reader reader, long l) */
                    statement.setNClob(index, (Reader) v[0], (Long) v[1]);
                    break;
                case SET_SQLXML_INT_I_SQLXML_SQLXML: /* setSQLXML(int i, SQLXML sqlxml) */
                    statement.setSQLXML(index, (SQLXML) v[0]);
                    break;
                case SET_OBJECT_INT_I_OBJECT_O_INT_I1_INT_I2: /* setObject(int i, Object o, int i1, int i2) */
                    statement.setObject(index, v[0], (Integer) v[1], (Integer) v[2]);
                    break;
                case SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L: /* setAsciiStream(int i, InputStream inputStream, long l) */
                    statement.setAsciiStream(index, (InputStream) v[0], (Long) v[1]);
                    break;
                case SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L: /* setBinaryStream(int i, InputStream inputStream, long l) */
                    statement.setBinaryStream(index, (InputStream) v[0], (Long) v[1]);
                    break;
                case SET_CHARACTERSTREAM_INT_I_READER_READER_LONG_L: /* setCharacterStream(int i, Reader reader, long l) */
                    statement.setCharacterStream(index, (Reader) v[0], (Long) v[1]);
                    break;
                case SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM: /* setAsciiStream(int i, InputStream inputStream) */
                    statement.setAsciiStream(index, (InputStream) v[0]);
                    break;
                case SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM: /* setBinaryStream(int i, InputStream inputStream) */
                    statement.setBinaryStream(index, (InputStream) v[0]);
                    break;
                case SET_CHARACTERSTREAM_INT_I_READER_READER: /* setCharacterStream(int i, Reader reader) */
                    statement.setCharacterStream(index, (Reader) v[0]);
                    break;
                case SET_NCHARACTERSTREAM_INT_I_READER_READER: /* setNCharacterStream(int i, Reader reader) */
                    statement.setNCharacterStream(index, (Reader) v[0]);
                    break;
                case SET_CLOB_INT_I_READER_READER: /* setClob(int i, Reader reader) */
                    statement.setClob(index, (Reader) v[0]);
                    break;
                case SET_BLOB_INT_I_INPUTSTREAM_INPUTSTREAM: /* setBlob(int i, InputStream inputStream) */
                    statement.setBlob(index, (InputStream) v[0]);
                    break;
                case SET_NCLOB_INT_I_READER_READER: /* setNClob(int i, Reader reader) */
                    statement.setNClob(index, (Reader) v[0]);
                    break;
                default:
                    throw new SQLException(OPERATION_NOT_SUPPORTED);
            }
        }
    }

    //-------------------------
    @Override
    public ResultSet executeQuery() throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult =
                    analyzer.analyze(schema, sql, methodInvocationState, shardOps);
            preparedStatementPreparation(analyzedResult);
            playback();
            List<ResultSet> resultSets = new ArrayList<>(analyzedResult.size());
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                PreparedStatementMethodInvocation analyzed = analyzedResult.get(index).getA1();
                PreparedStatement statement = (PreparedStatement) entry.getValue();
                playback(analyzed, statement);
                resultSets.add(statement.executeQuery());
            }
            resultSet = new MuxResultSet(this, resultSets);
            return resultSet;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult =
                    analyzer.analyze(schema, sql, methodInvocationState, shardOps);
            preparedStatementPreparation(analyzedResult);
            playback();
            int affected = 0;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                PreparedStatementMethodInvocation analyzed = analyzedResult.get(index).getA1();
                PreparedStatement statement = (PreparedStatement) entry.getValue();
                playback(analyzed, statement);
                affected += statement.executeUpdate();
            }
            updateCount = affected;
            return updateCount;
        }
    }

    @Override
    public boolean execute() throws SQLException {
        synchronized (this) {
            Map<Integer, Tuple2<String, PreparedStatementMethodInvocation>> analyzedResult =
                    analyzer.analyze(schema, sql, methodInvocationState, shardOps);
            preparedStatementPreparation(analyzedResult);
            playback();
            boolean ret = false;
            for (Map.Entry<Integer, Statement> entry : statements.entrySet()) {
                int index = entry.getKey();
                PreparedStatementMethodInvocation analyzed = analyzedResult.get(index).getA1();
                PreparedStatement statement = (PreparedStatement) entry.getValue();
                playback(analyzed, statement);
                ret |= statement.execute();
            }
            return ret;
        }
    }
    //-------------------------
    @Override
    public void setNull(int i, int i1) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_NULL_INT_I_INT_I1, new Object[] {i1}));
        internalSetObject(i, null);
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_BOOLEAN_INT_I_BOOLEAN_B, new Object[] {b}));
        internalSetObject(i, b);
    }

    @Override
    public void setByte(int i, byte b) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_BYTE_INT_I_BYTE_B, new Object[] {b}));
        internalSetObject(i, b);
    }

    @Override
    public void setShort(int i, short i1) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_SHORT_INT_I_SHORT_I1, new Object[] {i1}));
        internalSetObject(i, i1);
    }

    @Override
    public void setInt(int i, int i1) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_INT_INT_I_INT_I1, new Object[] {i1}));
        internalSetObject(i, i1);
    }

    @Override
    public void setLong(int i, long l) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_LONG_INT_I_LONG_L, new Object[] {l}));
        internalSetObject(i, l);
    }

    @Override
    public void setFloat(int i, float v) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_FLOAT_INT_I_FLOAT_V, new Object[] {v}));
        internalSetObject(i, v);
    }

    @Override
    public void setDouble(int i, double v) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_DOUBLE_INT_I_DOUBLE_V, new Object[] {v}));
        internalSetObject(i, v);
    }

    @Override
    public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_BIGDECIMAL_INT_I_BIGDECIMAL_BIGDECIMAL, new Object[] {bigDecimal}));
        internalSetObject(i, bigDecimal);
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_STRING_INT_I_STRING_S, new Object[] {s}));
        internalSetObject(i, s);
    }

    @Override
    public void setBytes(int i, byte[] bytes) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_BYTES_INT_I_BYTEARR_BYTES, new Object[] {bytes}));
        internalSetObject(i, bytes);
    }

    @Override
    public void setDate(int i, Date date) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_DATE_INT_I_DATE_DATE, new Object[] {date}));
        internalSetObject(i, date);
    }

    @Override
    public void setTime(int i, Time time) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_TIME_INT_I_TIME_TIME, new Object[] {time}));
        internalSetObject(i, time);
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_TIMESTAMP_INT_I_TIMESTAMP_TIMESTAMP, new Object[] {timestamp}));
        internalSetObject(i, timestamp);
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1,
                new Object[] {inputStream, i1})
        );
        internalSetInputStream(i, inputStream, i1);
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public void setUnicodeStream(int i, InputStream inputStream, int i1) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_UNICODESTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1,
                new Object[] {inputStream, i1})
        );
        internalSetInputStream(i, inputStream, i1);
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_INT_I1,
                new Object[] {inputStream, i1})
        );
        internalSetInputStream(i, inputStream, i1);
    }

    @Override
    public void setObject(int i, Object o, int i1) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_OBJECT_INT_I_OBJECT_O_INT_I1, new Object[] {o, i1}));
        internalSetObject(i, o);
    }

    @Override
    public void setObject(int i, Object o) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_OBJECT_INT_I_OBJECT_O, new Object[] {o}));
        internalSetObject(i, o);
    }

    @Override
    public void setCharacterStream(int i, Reader reader, int i1) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_CHARACTERSTREAM_INT_I_READER_READER_INT_I1,
                new Object[] {reader, i1})
        );
        internalSetReader(i, reader, i1);
    }

    @Override
    public void setRef(int i, Ref ref) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_REF_INT_I_REF_REF, new Object[] {ref}));
        internalSetObject(i, ref);
    }

    @Override
    public void setBlob(int i, Blob blob) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_BLOB_INT_I_BLOB_BLOB, new Object[] {blob}));
        internalSetObject(i, blob);
    }

    @Override
    public void setClob(int i, Clob clob) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_CLOB_INT_I_CLOB_CLOB, new Object[] {clob}));
        internalSetObject(i, clob);
    }

    @Override
    public void setArray(int i, Array array) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_ARRAY_INT_I_ARRAY_ARRAY, new Object[] {array}));
        internalSetObject(i, array);
    }

    @Override
    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_DATE_INT_I_DATE_DATE_CALENDAR_CALENDAR,
                new Object[] {date, calendar})
        );
        internalSetObject(i, date);
    }

    @Override
    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_TIME_INT_I_TIME_TIME_CALENDAR_CALENDAR,
                new Object[] {time, calendar})
        );
        internalSetObject(i, time);
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_TIMESTAMP_INT_I_TIMESTAMP_TIMESTAMP_CALENDAR_CALENDAR,
                new Object[] {timestamp, calendar})
        );
        internalSetObject(i, timestamp);
    }

    @Override
    public void setNull(int i, int i1, String s) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_NULL_INT_I_INT_I1_STRING_S,
                new Object[] {i1, s})
        );
        internalSetObject(i, null);
    }

    @Override
    public void setURL(int i, URL url) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_URL_INT_I_URL_URL, new Object[] {url}));
        internalSetObject(i, url);
    }

    @Override
    public void setRowId(int i, RowId rowId) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_ROWID_INT_I_ROWID_ROWID, new Object[] {rowId}));
        internalSetObject(i, rowId);
    }

    @Override
    public void setNString(int i, String s) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_NSTRING_INT_I_STRING_S, new Object[] {s}));
        internalSetObject(i, s);
    }

    @Override
    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_NCHARACTERSTREAM_INT_I_READER_READER_LONG_L,
                new Object[] {reader, l})
        );
        internalSetReader(i, reader, l);
    }

    @Override
    public void setNClob(int i, NClob nClob) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_NCLOB_INT_I_NCLOB_NCLOB, new Object[] {nClob}));
        internalSetObject(i, nClob);
    }

    @Override
    public void setClob(int i, Reader reader, long l) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_CLOB_INT_I_READER_READER_LONG_L,
                new Object[] {reader, l})
        );
        internalSetReader(i, reader, l);
    }

    @Override
    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_BLOB_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L,
                new Object[] {inputStream, l})
        );
        internalSetInputStream(i, inputStream, l);
    }

    @Override
    public void setNClob(int i, Reader reader, long l) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_NCLOB_INT_I_READER_READER_LONG_L,
                new Object[] {reader, l})
        );
        internalSetReader(i, reader, l);
    }

    @Override
    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_SQLXML_INT_I_SQLXML_SQLXML, new Object[] {sqlxml}));
        internalSetObject(i, sqlxml);
    }

    @Override
    public void setObject(int i, Object o, int i1, int i2) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_OBJECT_INT_I_OBJECT_O_INT_I1_INT_I2,
                new Object[] {o, i1, i2})
        );
        internalSetObject(i, o);
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L,
                new Object[] {inputStream, l})
        );
        internalSetInputStream(i, inputStream, l);
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM_LONG_L,
                new Object[] {inputStream, l})
        );
        internalSetInputStream(i, inputStream, l);
    }

    @Override
    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(
                SET_CHARACTERSTREAM_INT_I_READER_READER_LONG_L,
                new Object[] {reader, l})
        );
        internalSetReader(i, reader, l);
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_ASCIISTREAM_INT_I_INPUTSTREAM_INPUTSTREAM, new Object[] {inputStream}));
        internalSetObject(i, inputStream);
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_BINARYSTREAM_INT_I_INPUTSTREAM_INPUTSTREAM, new Object[] {inputStream}));
        internalSetObject(i, inputStream);
    }

    @Override
    public void setCharacterStream(int i, Reader reader) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_CHARACTERSTREAM_INT_I_READER_READER, new Object[] {reader}));
        internalSetObject(i, reader);
    }

    @Override
    public void setNCharacterStream(int i, Reader reader) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_NCHARACTERSTREAM_INT_I_READER_READER, new Object[] {reader}));
        internalSetObject(i, reader);
    }

    @Override
    public void setClob(int i, Reader reader) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_CLOB_INT_I_READER_READER, new Object[] {reader}));
        internalSetObject(i, reader);
    }

    @Override
    public void setBlob(int i, InputStream inputStream) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_BLOB_INT_I_INPUTSTREAM_INPUTSTREAM, new Object[] {inputStream}));
        internalSetObject(i, inputStream);
    }

    @Override
    public void setNClob(int i, Reader reader) throws SQLException {
        methodInvocationState.getState().put(i, new MethodInvocationEntry<>(SET_NCLOB_INT_I_READER_READER, new Object[] {reader}));
        internalSetObject(i, reader);
    }

    //-------------------------
    private void internalSetInputStream(int i, InputStream inputStream, long l) throws SQLException {
        if (inputStream == null) {
            methodInvocationState.getValueMap().put(i, null);
        } else {
            try {
                byte[] bArr = IOUtils.streamToBytes(inputStream, l);
                methodInvocationState.getValueMap().put(i, bArr);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    private void internalSetReader(int i, Reader reader, long l) throws SQLException {
        if (reader == null) {
            methodInvocationState.getValueMap().put(i, null);
        } else {
            try {
                byte[] bArr = IOUtils.readerToBytes(reader, l);
                methodInvocationState.getValueMap().put(i, bArr);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    @SuppressWarnings({"java:S1144", "java:S3776"})
    private void internalSetObject(int i, Object o) throws SQLException {
        if (o == null) {
            methodInvocationState.getValueMap().put(i, null);
        } else if (o instanceof Boolean) {
            Boolean b = (Boolean) o;
            methodInvocationState.getValueMap().put(i, new byte[] {(byte) (Boolean.TRUE.equals(b) ? 1 : 0)});
        } else if (o instanceof Byte) {
            Byte b = (Byte) o;
            methodInvocationState.getValueMap().put(i, new byte[] {b});
        } else if (o instanceof Short) {
            Short i1 = (Short) o;
            byte[] bArr = new byte[Short.BYTES];
            ByteUtils.writeShort(bArr, 0, i1);
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof Integer) {
            Integer i1 = (Integer) o;
            byte[] bArr = new byte[Integer.BYTES];
            ByteUtils.writeInt(bArr, 0, i1);
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof Long) {
            Long l = (Long) o;
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, l);
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof Float) {
            Float v = (Float) o;
            int intValue = Float.floatToIntBits(v);
            byte[] bArr = new byte[Integer.BYTES];
            ByteUtils.writeInt(bArr, 0, intValue);
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof Double) {
            Double v = (Double) o;
            long longValue = Double.doubleToLongBits(v);
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, longValue);
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) o;
            byte[] bArr = bigDecimal.unscaledValue().toByteArray();
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof String) {
            String s = (String) o;
            methodInvocationState.getValueMap().put(i, s.getBytes(StandardCharsets.UTF_8));
        } else if (o instanceof byte[]) {
            byte[] bytes = (byte[]) o;
            methodInvocationState.getValueMap().put(i, bytes);
        } else if (o instanceof Date) {
            Date date = (Date) o;
            long ts = date.getTime();
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, ts);
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof Time) {
            Time date = (Time) o;
            long ts = date.getTime();
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, ts);
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof Timestamp) {
            Timestamp date = (Timestamp) o;
            long ts = date.getTime();
            byte[] bArr = new byte[Long.BYTES];
            ByteUtils.writeLong(bArr, 0, ts);
            methodInvocationState.getValueMap().put(i, bArr);
        } else if (o instanceof InputStream) {
            try {
                InputStream inputStream = (InputStream) o;
                byte[] bArr = IOUtils.streamToBytes(inputStream);
                methodInvocationState.getValueMap().put(i, bArr);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof Reader) {
            try {
                Reader reader = (Reader) o;
                byte[] bArr = IOUtils.readerToBytes(reader);
                methodInvocationState.getValueMap().put(i, bArr);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof Ref) {
            // will not be supported
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        } else if (o instanceof Blob) {
            try {
                Blob blob = (Blob) o;
                byte[] bArr = IOUtils.streamToBytes(blob.getBinaryStream(), blob.length());
                methodInvocationState.getValueMap().put(i, bArr);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof NClob) {
            try {
                NClob clob = (NClob) o;
                byte[] bArr = IOUtils.readerToBytes(clob.getCharacterStream(), clob.length());
                methodInvocationState.getValueMap().put(i, bArr);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof Clob) {
            try {
                Clob clob = (Clob) o;
                byte[] bArr = IOUtils.readerToBytes(clob.getCharacterStream(), clob.length());
                methodInvocationState.getValueMap().put(i, bArr);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else if (o instanceof Array) {
            // will not be supported
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        } else if (o instanceof URL) {
            URL url = (URL) o;
            methodInvocationState.getValueMap().put(i, url.toString().getBytes(StandardCharsets.UTF_8));
        } else if (o instanceof RowId) {
            // will not be supported
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        } else if (o instanceof SQLXML) {
            try {
                SQLXML sqlxml = (SQLXML) o;
                byte[] bArr = IOUtils.readerToBytes(sqlxml.getCharacterStream());
                methodInvocationState.getValueMap().put(i, bArr);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } else {
            throw new SQLException(OPERATION_NOT_SUPPORTED);
        }
    }
    //-------------------------
    @Override
    public void clearParameters() {
        methodInvocationState.clear();
    }

    @Override
    public void addBatch() throws SQLException {
        // TODO support later
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSet.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null; // TODO
    }
}
