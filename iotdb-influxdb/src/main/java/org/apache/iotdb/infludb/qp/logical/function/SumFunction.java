package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.qp.utils.MathUtil;
import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class SumFunction extends Aggregate {

    private List<Double> numbers = new ArrayList<>();

    public SumFunction(List<Expression> expressionList) {
        super(expressionList);
    }

    public SumFunction() {
    }

    @Override
    public void updateValue(FunctionValue functionValue) {
        Object value = functionValue.getValue();
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("not support this type");
        }

        double tmpValue = ((Number) value).doubleValue();
        numbers.add(tmpValue);
    }

    @Override
    public FunctionValue calculate() {
        return new FunctionValue(
                numbers.size() == 0 ? numbers : MathUtil.Sum(numbers)
                , 0L);
    }
}
