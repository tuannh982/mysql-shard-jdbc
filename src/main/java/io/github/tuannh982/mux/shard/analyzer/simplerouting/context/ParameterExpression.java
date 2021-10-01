package io.github.tuannh982.mux.shard.analyzer.simplerouting.context;

import lombok.AllArgsConstructor;
import lombok.Getter;
import net.sf.jsqlparser.schema.Column;

@Getter
@AllArgsConstructor
public class ParameterExpression {
    private final ExpressionType expressionType;
    private final boolean isNot;
    private final Column column;
    private final Parameter[] parameters;
}
