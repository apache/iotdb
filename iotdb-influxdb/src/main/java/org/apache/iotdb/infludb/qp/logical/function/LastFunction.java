package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class LastFunction extends Selector {
    private Object value;

    public LastFunction(List<Expression> expressionList) {
        super(expressionList);
        this.setTimestamp(Long.MIN_VALUE);
    }

    public LastFunction() {
    }

    @Override
    public void updateValueAndRelate(FunctionValue functionValue, List<Object> values) {

        Object value = functionValue.getValue();
        Long timestamp = functionValue.getTimestamp();
        if (timestamp >= this.getTimestamp()) {
            this.value = value;
            this.setTimestamp(timestamp);
            this.setRelatedValues(values);
        }
    }


    @Override
    public FunctionValue calculate() {
        return new FunctionValue(value, this.getTimestamp());
    }

}
