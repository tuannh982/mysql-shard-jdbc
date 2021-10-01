package io.github.tuannh982.mux.shard.analyzer.simplerouting.context;

import io.github.tuannh982.mux.commons.binary.TypeConverter;
import io.github.tuannh982.mux.shard.analyzer.simplerouting.SQLExceptionRTE;
import lombok.Getter;
import net.sf.jsqlparser.expression.*;

import java.sql.SQLException;

import static io.github.tuannh982.mux.shard.analyzer.ErrorMessages.*;

@Getter
public class Parameter {
    private final Integer index;
    private byte[] value;
    private final Expression expression;

    public boolean isPreparedParameter() {
        return index != null;
    }

    public Parameter(Expression expression) {
        this.expression = expression;
        if (expression instanceof JdbcParameter) {
            index = ((JdbcParameter) expression).getIndex();
            value = null;
        } else {
            index = null;
            try {
                value = TypeConverter.SQLTypeToBytes(getValue(expression));
            } catch (SQLException e) {
                throw new SQLExceptionRTE(e);
            }
        }
    }

    public void updateValue(byte[] value) {
        if (index == null || this.value != null) {
            throw new SQLExceptionRTE(SQL_PARSER_NOT_PREPARED_OR_ALREADY_SET_PARAM);
        }
        this.value = value;
    }

    private static Object getValue(Expression expression) {
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
            throw new SQLExceptionRTE(SQL_PARSER_TYPE_NOT_SUPPORTED);
        } else {
            throw new SQLExceptionRTE(SQL_PARSER_TYPE_NOT_SUPPORTED);
        }
    }
}
