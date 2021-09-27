package io.github.tuannh982.mux.statements.resultset;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.tuannh982.mux.connection.Constants.OPERATION_NOT_SUPPORTED;

public class MuxResultSet implements ResultSet {
    private final Statement statement;
    private final List<ResultSet> resultSets;
    private int currentIndex;
    private ResultSet currentResultSet;
    private volatile boolean isClosed;
    private volatile boolean isBeginToReadResultSet;
    //
    private int fetchDirection;
    private int fetchSize;
    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetHoldability = HOLD_CURSORS_OVER_COMMIT;
    //
    private final AtomicInteger currentRow;
    private ResultSetMetaData metadata;

    public MuxResultSet(Statement statement, List<ResultSet> resultSets) throws SQLException {
        this.statement = statement;
        if (resultSets.isEmpty()) {
            this.resultSets = resultSets;
            isClosed = true;
            isBeginToReadResultSet = false;
            currentIndex = -1;
            currentRow = new AtomicInteger(-1);
        } else {
            this.resultSets = resultSets;
            isClosed = false;
            isBeginToReadResultSet = true;
            currentIndex = -1;
            currentResultSet = this.resultSets.get(0);
            currentRow = new AtomicInteger(-1);
            updateResultSetData();
            updateResultSetMetadata();
        }
    }

    private void updateResultSetMetadata() throws SQLException {
        List<ResultSetMetaData> metaDataList = new ArrayList<>();
        for (ResultSet rs : resultSets) {
            metaDataList.add(rs.getMetaData());
        }
        metadata = new MuxResultSetMetadata(metaDataList);
    }

    private void updateResultSetData() throws SQLException {
        for (int i = 1; i < resultSets.size(); i++) {
            if (resultSets.get(i - 1).getType() != resultSets.get(i).getType()) {
                throw new SQLException("Different ResultSetType configuration error occurred");
            }
            if (resultSets.get(i - 1).getConcurrency() != resultSets.get(i).getConcurrency()) {
                throw new SQLException("Different ResultSetConcurrency configuration error occurred");
            }
        }
        resultSetType = resultSets.get(0).getType();
        resultSetConcurrency = resultSets.get(0).getConcurrency();
        resultSetHoldability = resultSets.get(0).getHoldability();
    }

    @Override
    public void close() throws SQLException {
        synchronized (this) {
            for (ResultSet rs : resultSets) {
                rs.close();
            }
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public String getString(int i) throws SQLException {
        return currentResultSet.getString(i);
    }

    @Override
    public String getString(String s) throws SQLException {
        return currentResultSet.getString(s);
    }

    @Override
    public boolean getBoolean(int i) throws SQLException {
        return currentResultSet.getBoolean(i);
    }

    @Override
    public boolean getBoolean(String s) throws SQLException {
        return currentResultSet.getBoolean(s);
    }

    @Override
    public byte getByte(int i) throws SQLException {
        return currentResultSet.getByte(i);
    }

    @Override
    public byte getByte(String s) throws SQLException {
        return currentResultSet.getByte(s);
    }

    @Override
    public byte[] getBytes(int i) throws SQLException {
        return currentResultSet.getBytes(i);
    }

    @Override
    public byte[] getBytes(String s) throws SQLException {
        return currentResultSet.getBytes(s);
    }

    @Override
    public short getShort(int i) throws SQLException {
        return currentResultSet.getShort(i);
    }

    @Override
    public short getShort(String s) throws SQLException {
        return currentResultSet.getShort(s);
    }

    @Override
    public int getInt(int i) throws SQLException {
        return currentResultSet.getInt(i);
    }

    @Override
    public int getInt(String s) throws SQLException {
        return currentResultSet.getInt(s);
    }

    @Override
    public long getLong(int i) throws SQLException {
        return currentResultSet.getLong(i);
    }

    @Override
    public long getLong(String s) throws SQLException {
        return currentResultSet.getLong(s);
    }

    @Override
    public float getFloat(int i) throws SQLException {
        return currentResultSet.getFloat(i);
    }

    @Override
    public float getFloat(String s) throws SQLException {
        return currentResultSet.getFloat(s);
    }

    @Override
    public double getDouble(int i) throws SQLException {
        return currentResultSet.getDouble(i);
    }

    @Override
    public double getDouble(String s) throws SQLException {
        return currentResultSet.getDouble(s);
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int i, int i1) throws SQLException {
        return currentResultSet.getBigDecimal(i, i1);
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String s, int i) throws SQLException {
        return currentResultSet.getBigDecimal(s, i);
    }

    @Override
    public BigDecimal getBigDecimal(int i) throws SQLException {
        return currentResultSet.getBigDecimal(i);
    }

    @Override
    public BigDecimal getBigDecimal(String s) throws SQLException {
        return currentResultSet.getBigDecimal(s);
    }

    @Override
    public Date getDate(int i) throws SQLException {
        return currentResultSet.getDate(i);
    }

    @Override
    public Date getDate(String s) throws SQLException {
        return currentResultSet.getDate(s);
    }

    @Override
    public Date getDate(int i, Calendar calendar) throws SQLException {
        return currentResultSet.getDate(i, calendar);
    }

    @Override
    public Date getDate(String s, Calendar calendar) throws SQLException {
        return currentResultSet.getDate(s, calendar);
    }

    @Override
    public Time getTime(int i) throws SQLException {
        return currentResultSet.getTime(i);
    }

    @Override
    public Time getTime(String s) throws SQLException {
        return currentResultSet.getTime(s);
    }

    @Override
    public Time getTime(int i, Calendar calendar) throws SQLException {
        return currentResultSet.getTime(i, calendar);
    }

    @Override
    public Time getTime(String s, Calendar calendar) throws SQLException {
        return currentResultSet.getTime(s, calendar);
    }

    @Override
    public Timestamp getTimestamp(String s) throws SQLException {
        return currentResultSet.getTimestamp(s);
    }

    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        return currentResultSet.getTimestamp(i);
    }

    @Override
    public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
        return currentResultSet.getTimestamp(i, calendar);
    }

    @Override
    public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
        return currentResultSet.getTimestamp(s, calendar);
    }

    @Override
    public InputStream getAsciiStream(int i) throws SQLException {
        return currentResultSet.getAsciiStream(i);
    }

    @Override
    public InputStream getAsciiStream(String s) throws SQLException {
        return currentResultSet.getAsciiStream(s);
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(int i) throws SQLException {
        return currentResultSet.getUnicodeStream(i);
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(String s) throws SQLException {
        return currentResultSet.getUnicodeStream(s);
    }

    @Override
    public InputStream getBinaryStream(int i) throws SQLException {
        return currentResultSet.getBinaryStream(i);
    }

    @Override
    public InputStream getBinaryStream(String s) throws SQLException {
        return currentResultSet.getBinaryStream(s);
    }

    @Override
    public Object getObject(int i) throws SQLException {
        return currentResultSet.getObject(i);
    }

    @Override
    public Object getObject(String s) throws SQLException {
        return currentResultSet.getObject(s);
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        return currentResultSet.getObject(i, map);
    }

    @Override
    public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
        return currentResultSet.getObject(s, map);
    }

    @Override
    public <T> T getObject(int i, Class<T> aClass) throws SQLException {
        return currentResultSet.getObject(i, aClass);
    }

    @Override
    public <T> T getObject(String s, Class<T> aClass) throws SQLException {
        return currentResultSet.getObject(s, aClass);
    }

    @Override
    public Reader getCharacterStream(int i) throws SQLException {
        return currentResultSet.getCharacterStream(i);
    }

    @Override
    public Reader getCharacterStream(String s) throws SQLException {
        return currentResultSet.getCharacterStream(s);
    }

    @Override
    public Ref getRef(int i) throws SQLException {
        return currentResultSet.getRef(i);
    }

    @Override
    public Ref getRef(String s) throws SQLException {
        return currentResultSet.getRef(s);
    }

    @Override
    public Blob getBlob(int i) throws SQLException {
        return currentResultSet.getBlob(i);
    }

    @Override
    public Blob getBlob(String s) throws SQLException {
        return currentResultSet.getBlob(s);
    }

    @Override
    public Clob getClob(int i) throws SQLException {
        return currentResultSet.getClob(i);
    }

    @Override
    public Clob getClob(String s) throws SQLException {
        return currentResultSet.getClob(s);
    }

    @Override
    public Array getArray(int i) throws SQLException {
        return currentResultSet.getArray(i);
    }

    @Override
    public Array getArray(String s) throws SQLException {
        return currentResultSet.getArray(s);
    }

    @Override
    public URL getURL(int i) throws SQLException {
        return currentResultSet.getURL(i);
    }

    @Override
    public URL getURL(String s) throws SQLException {
        return currentResultSet.getURL(s);
    }

    @Override
    public NClob getNClob(int i) throws SQLException {
        return currentResultSet.getNClob(i);
    }

    @Override
    public NClob getNClob(String s) throws SQLException {
        return currentResultSet.getNClob(s);
    }

    @Override
    public SQLXML getSQLXML(int i) throws SQLException {
        return currentResultSet.getSQLXML(i);
    }

    @Override
    public SQLXML getSQLXML(String s) throws SQLException {
        return currentResultSet.getSQLXML(s);
    }

    @Override
    public String getNString(int i) throws SQLException {
        return currentResultSet.getNString(i);
    }

    @Override
    public String getNString(String s) throws SQLException {
        return currentResultSet.getNString(s);
    }

    @Override
    public Reader getNCharacterStream(int i) throws SQLException {
        return currentResultSet.getNCharacterStream(i);
    }

    @Override
    public Reader getNCharacterStream(String s) throws SQLException {
        return currentResultSet.getNCharacterStream(s);
    }

    @Override
    public RowId getRowId(int i) throws SQLException {
        return currentResultSet.getRowId(i);
    }

    @Override
    public RowId getRowId(String s) throws SQLException {
        return currentResultSet.getRowId(s);
    }

    @Override
    public void updateNull(int i) throws SQLException {
        currentResultSet.updateNull(i);
    }

    @Override
    public void updateNull(String s) throws SQLException {
        currentResultSet.updateNull(s);
    }

    @Override
    public void updateBoolean(int i, boolean b) throws SQLException {
        currentResultSet.updateBoolean(i, b);
    }

    @Override
    public void updateBoolean(String s, boolean b) throws SQLException {
        currentResultSet.updateBoolean(s, b);
    }

    @Override
    public void updateByte(int i, byte b) throws SQLException {
        currentResultSet.updateByte(i, b);
    }

    @Override
    public void updateByte(String s, byte b) throws SQLException {
        currentResultSet.updateByte(s, b);
    }

    @Override
    public void updateShort(int i, short i1) throws SQLException {
        currentResultSet.updateShort(i, i1);
    }

    @Override
    public void updateShort(String s, short i) throws SQLException {
        currentResultSet.updateShort(s, i);
    }

    @Override
    public void updateInt(int i, int i1) throws SQLException {
        currentResultSet.updateInt(i, i1);
    }

    @Override
    public void updateInt(String s, int i) throws SQLException {
        currentResultSet.updateInt(s, i);
    }

    @Override
    public void updateLong(int i, long l) throws SQLException {
        currentResultSet.updateLong(i, l);
    }

    @Override
    public void updateLong(String s, long l) throws SQLException {
        currentResultSet.updateLong(s, l);
    }

    @Override
    public void updateFloat(int i, float v) throws SQLException {
        currentResultSet.updateFloat(i, v);
    }

    @Override
    public void updateFloat(String s, float v) throws SQLException {
        currentResultSet.updateFloat(s, v);
    }

    @Override
    public void updateDouble(int i, double v) throws SQLException {
        currentResultSet.updateDouble(i, v);
    }

    @Override
    public void updateDouble(String s, double v) throws SQLException {
        currentResultSet.updateDouble(s, v);
    }

    @Override
    public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        currentResultSet.updateBigDecimal(i, bigDecimal);
    }

    @Override
    public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
        currentResultSet.updateBigDecimal(s, bigDecimal);
    }

    @Override
    public void updateString(int i, String s) throws SQLException {
        currentResultSet.updateString(i, s);
    }

    @Override
    public void updateString(String s, String s1) throws SQLException {
        currentResultSet.updateString(s, s1);
    }

    @Override
    public void updateBytes(int i, byte[] bytes) throws SQLException {
        currentResultSet.updateBytes(i, bytes);
    }

    @Override
    public void updateBytes(String s, byte[] bytes) throws SQLException {
        currentResultSet.updateBytes(s, bytes);
    }

    @Override
    public void updateDate(int i, Date date) throws SQLException {
        currentResultSet.updateDate(i, date);
    }

    @Override
    public void updateDate(String s, Date date) throws SQLException {
        currentResultSet.updateDate(s, date);
    }

    @Override
    public void updateTime(int i, Time time) throws SQLException {
        currentResultSet.updateTime(i, time);
    }

    @Override
    public void updateTime(String s, Time time) throws SQLException {
        currentResultSet.updateTime(s, time);
    }

    @Override
    public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
        currentResultSet.updateTimestamp(i, timestamp);
    }

    @Override
    public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {
        currentResultSet.updateTimestamp(s, timestamp);
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {
        currentResultSet.updateAsciiStream(i, inputStream, i1);
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
        currentResultSet.updateAsciiStream(s, inputStream, i);
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        currentResultSet.updateAsciiStream(i, inputStream, l);
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
        currentResultSet.updateAsciiStream(s, inputStream, l);
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
        currentResultSet.updateAsciiStream(i, inputStream);
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {
        currentResultSet.updateAsciiStream(s, inputStream);
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {
        currentResultSet.updateBinaryStream(i, inputStream, i1);
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
        currentResultSet.updateBinaryStream(s, inputStream, i);
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        currentResultSet.updateBinaryStream(i, inputStream, l);
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
        currentResultSet.updateBinaryStream(s, inputStream, l);
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
        currentResultSet.updateBinaryStream(i, inputStream);
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {
        currentResultSet.updateBinaryStream(s, inputStream);
    }

    @Override
    public void updateCharacterStream(int i, Reader reader, int i1) throws SQLException {
        currentResultSet.updateCharacterStream(i, reader, i1);
    }

    @Override
    public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {
        currentResultSet.updateCharacterStream(s, reader, i);
    }

    @Override
    public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
        currentResultSet.updateCharacterStream(i, reader, l);
    }

    @Override
    public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {
        currentResultSet.updateCharacterStream(s, reader, l);
    }

    @Override
    public void updateCharacterStream(int i, Reader reader) throws SQLException {
        currentResultSet.updateCharacterStream(i, reader);
    }

    @Override
    public void updateCharacterStream(String s, Reader reader) throws SQLException {
        currentResultSet.updateCharacterStream(s, reader);
    }

    @Override
    public void updateObject(int i, Object o, int i1) throws SQLException {
        currentResultSet.updateObject(i, o, i1);
    }

    @Override
    public void updateObject(int i, Object o) throws SQLException {
        currentResultSet.updateObject(i, o);
    }

    @Override
    public void updateObject(String s, Object o, int i) throws SQLException {
        currentResultSet.updateObject(s, o, i);
    }

    @Override
    public void updateObject(String s, Object o) throws SQLException {
        currentResultSet.updateObject(s, o);
    }

    @Override
    public void updateRef(int i, Ref ref) throws SQLException {
        currentResultSet.updateRef(i, ref);
    }

    @Override
    public void updateRef(String s, Ref ref) throws SQLException {
        currentResultSet.updateRef(s, ref);
    }

    @Override
    public void updateBlob(int i, Blob blob) throws SQLException {
        currentResultSet.updateBlob(i, blob);
    }

    @Override
    public void updateBlob(String s, Blob blob) throws SQLException {
        currentResultSet.updateBlob(s, blob);
    }

    @Override
    public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
        currentResultSet.updateBlob(i, inputStream, l);
    }

    @Override
    public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {
        currentResultSet.updateBlob(s, inputStream, l);
    }

    @Override
    public void updateBlob(int i, InputStream inputStream) throws SQLException {
        currentResultSet.updateBlob(i, inputStream);
    }

    @Override
    public void updateBlob(String s, InputStream inputStream) throws SQLException {
        currentResultSet.updateBlob(s, inputStream);
    }

    @Override
    public void updateClob(int i, Clob clob) throws SQLException {
        currentResultSet.updateClob(i, clob);
    }

    @Override
    public void updateClob(String s, Clob clob) throws SQLException {
        currentResultSet.updateClob(s, clob);
    }

    @Override
    public void updateClob(int i, Reader reader, long l) throws SQLException {
        currentResultSet.updateClob(i, reader, l);
    }

    @Override
    public void updateClob(String s, Reader reader, long l) throws SQLException {
        currentResultSet.updateClob(s, reader, l);
    }

    @Override
    public void updateClob(int i, Reader reader) throws SQLException {
        currentResultSet.updateClob(i, reader);
    }

    @Override
    public void updateClob(String s, Reader reader) throws SQLException {
        currentResultSet.updateClob(s, reader);
    }

    @Override
    public void updateRowId(int i, RowId rowId) throws SQLException {
        currentResultSet.updateRowId(i, rowId);
    }

    @Override
    public void updateRowId(String s, RowId rowId) throws SQLException {
        currentResultSet.updateRowId(s, rowId);
    }

    @Override
    public void updateNString(int i, String s) throws SQLException {
        currentResultSet.updateNString(i, s);
    }

    @Override
    public void updateNString(String s, String s1) throws SQLException {
        currentResultSet.updateNString(s, s1);
    }

    @Override
    public void updateNClob(int i, NClob nClob) throws SQLException {
        currentResultSet.updateNClob(i, nClob);
    }

    @Override
    public void updateNClob(String s, NClob nClob) throws SQLException {
        currentResultSet.updateNClob(s, nClob);
    }

    @Override
    public void updateNClob(int i, Reader reader, long l) throws SQLException {
        currentResultSet.updateNClob(i, reader, l);
    }

    @Override
    public void updateNClob(String s, Reader reader, long l) throws SQLException {
        currentResultSet.updateNClob(s, reader, l);
    }

    @Override
    public void updateNClob(int i, Reader reader) throws SQLException {
        currentResultSet.updateNClob(i, reader);
    }

    @Override
    public void updateNClob(String s, Reader reader) throws SQLException {
        currentResultSet.updateNClob(s, reader);
    }

    @Override
    public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
        currentResultSet.updateSQLXML(i, sqlxml);
    }

    @Override
    public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
        currentResultSet.updateSQLXML(s, sqlxml);
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {
        currentResultSet.updateNCharacterStream(i, reader, l);
    }

    @Override
    public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {
        currentResultSet.updateNCharacterStream(s, reader, l);
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader) throws SQLException {
        currentResultSet.updateNCharacterStream(i, reader);
    }

    @Override
    public void updateNCharacterStream(String s, Reader reader) throws SQLException {
        currentResultSet.updateNCharacterStream(s, reader);
    }

    @Override
    public void updateArray(int i, Array array) throws SQLException {
        currentResultSet.updateArray(i, array);
    }

    @Override
    public void updateArray(String s, Array array) throws SQLException {
        currentResultSet.updateArray(s, array);
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (isBeginToReadResultSet) {
            if (currentIndex == 0) {
                return true;
            } else {
                try {
                    return resultSets.get(currentIndex - 1).wasNull();
                } catch (Exception e) {
                    return false;
                }
            }
        } else {
            return currentResultSet.wasNull();
        }
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {
        fetchDirection = i;
        for (ResultSet rs : resultSets) {
            rs.setFetchDirection(i);
        }
    }

    @Override
    public int getFetchDirection() {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int i) throws SQLException {
        fetchSize = i;
        for (ResultSet rs : resultSets) {
            rs.setFetchSize(i);
        }
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public int getType() {
        return resultSetType;
    }

    @Override
    public int getConcurrency() {
        return resultSetConcurrency;
    }

    @Override
    public int getHoldability() {
        return resultSetHoldability;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.statement.clearWarnings();
    }

    @Override
    public int getRow() {
        return currentRow.get() + 1;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return resultSets.get(0).isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return resultSets.get(resultSets.size() - 1).isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return resultSets.get(0).isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        // will not be supported
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public int findColumn(String s) throws SQLException {
        for (int i = 1; i < resultSets.size(); i++) {
            if (resultSets.get(i - 1).findColumn(s) != resultSets.get(i).findColumn(s)) {
                throw new SQLException("ResultSets have different schema (findColumn return different results)");
            }
        }
        return resultSets.get(0).findColumn(s);
    }

    @Override
    public boolean absolute(int idx) throws SQLException {
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public boolean relative(int diff) throws SQLException {
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed) {
            throw new SQLException("Could not call next() on closed ResultSet");
        }
        isBeginToReadResultSet = false;
        if (currentResultSet.next()) {
            currentRow.incrementAndGet();
            return true;
        }
        while (currentIndex < resultSets.size() - 1) {
            currentIndex++;
            isBeginToReadResultSet = true;
            currentResultSet = resultSets.get(currentIndex);
            if (currentResultSet.next()) {
                currentRow.incrementAndGet();
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException(OPERATION_NOT_SUPPORTED);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        boolean ret = false;
        for (ResultSet rs : resultSets) {
            ret |= rs.rowUpdated();
        }
        return ret;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        boolean ret = false;
        for (ResultSet rs : resultSets) {
            ret |= rs.rowInserted();
        }
        return ret;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        boolean ret = false;
        for (ResultSet rs : resultSets) {
            ret |= rs.rowDeleted();
        }
        return ret;
    }

    @Override
    public void insertRow() throws SQLException {
        currentResultSet.insertRow();
        updateResultSetData();
    }

    @Override
    public void updateRow() throws SQLException {
        currentResultSet.updateRow();
        updateResultSetData();
    }

    @Override
    public void deleteRow() throws SQLException {
        currentResultSet.deleteRow();
        updateResultSetData();
    }

    @Override
    public void refreshRow() throws SQLException {
        currentResultSet.refreshRow();
        updateResultSetData();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        currentResultSet.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        currentResultSet.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        currentResultSet.moveToCurrentRow();
    }

    // Wrapper class methods

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (this.isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException("The receiver is not a wrapper for " + iface.getName());
            }
        } catch (Exception e) {
            throw new SQLException("The receiver is not a wrapper and does not implement the interface");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
