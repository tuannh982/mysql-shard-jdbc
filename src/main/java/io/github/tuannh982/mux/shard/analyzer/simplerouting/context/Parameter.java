package io.github.tuannh982.mux.shard.analyzer.simplerouting.context;

import io.github.tuannh982.mux.commons.binary.TypeConverter;
import lombok.Getter;
import net.sf.jsqlparser.expression.*;

import java.sql.SQLException;

import static io.github.tuannh982.mux.connection.Constants.*;

@Getter
public class Parameter {
    private final Integer index;
    private byte[] value;
    private final Expression expression;

    public boolean isPreparedParameter() {
        return index != null;
    }

    public Parameter(Expression expression) throws SQLException {
        this.expression = expression;
        if (expression instanceof JdbcParameter) {
            index = ((JdbcParameter) expression).getIndex();
            value = null;
        } else {
            index = null;
            value = TypeConverter.SQLTypeToBytes(getValue(expression));
        }
    }

    public void updateValue(byte[] value) throws SQLException {
        if (index == null || this.value != null) {
            throw new SQLException(SQL_PARSER_NOT_PREPARED_OR_ALREADY_SET_PARAM);
        }
        this.value = value;
    }

    private static Object getValue(Expression expression) throws SQLException {
        if (expression == null) {
            return null;
        } else if (expression instanceof NullValue) {
            return null;
        } else if (expression instanceof DateValue) {
            return ((DateValue) expression).getValue();
        } else if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        } else if (expression instanceof HexValue) {
            return Long.parseLong(((HexValue) expression).getValue(), 16);
        } else if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        } else if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof TimestampValue) {
            return ((TimestampValue) expression).getValue();
        } else if (expression instanceof ValueListExpression) {
            // TODO support later
            throw new SQLException(SQL_PARSER_TYPE_NOT_SUPPORTED);
        } else {
            throw new SQLException(SQL_PARSER_TYPE_NOT_SUPPORTED);
        }
    }
}
