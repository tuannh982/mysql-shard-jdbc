package io.github.tuannh982.mux.statements.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class MuxResultSetMetadata implements ResultSetMetaData {
    private final List<ResultSetMetaData> metaDataList;

    public MuxResultSetMetadata(List<ResultSetMetaData> metaDataList) {
        this.metaDataList = metaDataList;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return metaDataList.get(0).getColumnCount();
    }

    @Override
    public boolean isAutoIncrement(int i) throws SQLException {
        return metaDataList.get(0).isAutoIncrement(i);
    }

    @Override
    public boolean isCaseSensitive(int i) throws SQLException {
        return metaDataList.get(0).isCaseSensitive(i);
    }

    @Override
    public boolean isSearchable(int i) throws SQLException {
        return metaDataList.get(0).isSearchable(i);
    }

    @Override
    public boolean isCurrency(int i) throws SQLException {
        return metaDataList.get(0).isCurrency(i);
    }

    @Override
    public int isNullable(int i) throws SQLException {
        return metaDataList.get(0).isNullable(i);
    }

    @Override
    public boolean isSigned(int i) throws SQLException {
        return metaDataList.get(0).isSigned(i);
    }

    @Override
    public int getColumnDisplaySize(int i) throws SQLException {
        return metaDataList.get(0).getColumnDisplaySize(i);
    }

    @Override
    public String getColumnLabel(int i) throws SQLException {
        return metaDataList.get(0).getColumnLabel(i);
    }

    @Override
    public String getColumnName(int i) throws SQLException {
        return metaDataList.get(0).getColumnName(i);
    }

    @Override
    public String getSchemaName(int i) throws SQLException {
        return metaDataList.get(0).getSchemaName(i);
    }

    @Override
    public int getPrecision(int i) throws SQLException {
        return metaDataList.get(0).getPrecision(i);
    }

    @Override
    public int getScale(int i) throws SQLException {
        return metaDataList.get(0).getScale(i);
    }

    @Override
    public String getTableName(int i) throws SQLException {
        return metaDataList.get(0).getTableName(i);
    }

    @Override
    public String getCatalogName(int i) throws SQLException {
        return metaDataList.get(0).getCatalogName(i);
    }

    @Override
    public int getColumnType(int i) throws SQLException {
        return metaDataList.get(0).getColumnType(i);
    }

    @Override
    public String getColumnTypeName(int i) throws SQLException {
        return metaDataList.get(0).getColumnTypeName(i);
    }

    @Override
    public boolean isReadOnly(int i) throws SQLException {
        return metaDataList.get(0).isReadOnly(i);
    }

    @Override
    public boolean isWritable(int i) throws SQLException {
        return metaDataList.get(0).isWritable(i);
    }

    @Override
    public boolean isDefinitelyWritable(int i) throws SQLException {
        return metaDataList.get(0).isDefinitelyWritable(i);
    }

    @Override
    public String getColumnClassName(int i) throws SQLException {
        return metaDataList.get(0).getColumnClassName(i);
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
