package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class FirstFunction extends Selector {
    private Object value;

    public FirstFunction(List<Expression> expressionList) {
        super(expressionList);
        this.setTimestamp(Long.MAX_VALUE);
    }

    @Override
    public void updateValueAndRelate(FunctionValue functionValue, List<Object> values) {
        Object value = functionValue.getValue();
        Long timestamp = functionValue.getTimestamp();
        if (timestamp <= this.getTimestamp()) {
            this.value = value;
            this.setTimestamp(timestamp);
            this.setRelatedValues(values);
        }
    }

    public FirstFunction() {
    }


    @Override
    public FunctionValue calculate() {
        return new FunctionValue(value, this.getTimestamp());
    }

}
