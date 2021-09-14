package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class MaxFunction extends Selector {
    private Double doubleValue = Double.MIN_VALUE;
    private String stringValue = null;
    private boolean isNumber = false;
    private boolean isString = false;


    public MaxFunction(List<Expression> expressionList) {
        super(expressionList);
    }

    public MaxFunction() {
    }

    @Override
    public void updateValueAndRelate(FunctionValue functionValue, List<Object> values) {
        Object value = functionValue.getValue();
        Long timestamp = functionValue.getTimestamp();
        if (value instanceof Number) {
            if (!isNumber) {
                isNumber = true;
            }
            double tmpValue = ((Number) value).doubleValue();
            if (tmpValue >= this.doubleValue) {
                doubleValue = tmpValue;
                this.setTimestamp(timestamp);
                this.setRelatedValues(values);
            }
        } else if (value instanceof String) {
            String tmpValue = (String) value;
            if (!isString) {
                isString = true;
                stringValue = tmpValue;
                this.setTimestamp(timestamp);
                this.setRelatedValues(values);
            } else {
                if (tmpValue.compareTo(this.stringValue) >= 0) {
                    stringValue = tmpValue;
                    this.setTimestamp(timestamp);
                    this.setRelatedValues(values);
                }
            }
        }

    }

    @Override
    public FunctionValue calculate() {
        if (!isString && !isNumber) {
            return new FunctionValue(null, null);
        } else if (isString) {
            return new FunctionValue(stringValue, this.getTimestamp());
        } else {
            return new FunctionValue(doubleValue, this.getTimestamp());
        }
    }
}
