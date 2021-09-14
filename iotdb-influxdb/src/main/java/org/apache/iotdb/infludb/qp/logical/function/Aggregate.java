package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public abstract class Aggregate extends Function {
    public Aggregate() {

    }

    public Aggregate(List<Expression> expressionList) {
        super(expressionList);
    }

    public abstract void updateValue(FunctionValue functionValue);

}
