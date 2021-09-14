package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MedianFunction extends Aggregate {
    private List<Double> numbers=new ArrayList<>();

    public MedianFunction(List<Expression> expressionList) {
        super(expressionList);
    }

    @Override
    public void updateValue(FunctionValue functionValue) {
        Object value = functionValue.getValue();
        if (value instanceof Number) {
            numbers.add(((Number) value).doubleValue());
        } else {
            throw new IllegalArgumentException("not support this type");
        }
    }

    public MedianFunction() {
    }


    @Override
    public FunctionValue calculate() {
        Collections.sort(numbers);
        int len = numbers.size();
        if (len % 2 == 0) {
            return new FunctionValue((numbers.get(len / 2) + numbers.get(len / 2 - 1)) / 2, 0L);

        } else {
            return new FunctionValue(numbers.get(len / 2), 0L);
        }
    }
}
