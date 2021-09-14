package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class SpreadFunction extends Aggregate {
    private Double maxNum = null;
    private Double minNum = null;

    public SpreadFunction(List<Expression> expressionList) {
        super(expressionList);
    }

    public SpreadFunction() {
    }

    @Override
    public void updateValue(FunctionValue functionValue) {
        Object value = functionValue.getValue();
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("not support this type");
        }

        double tmpValue = ((Number) value).doubleValue();
        if (maxNum == null || tmpValue > maxNum) {
            maxNum = tmpValue;
        }
        if (minNum == null || tmpValue < minNum) {
            minNum = tmpValue;
        }
    }

    @Override
    public FunctionValue calculate() {
        return new FunctionValue(maxNum - minNum, 0L);
    }
}
