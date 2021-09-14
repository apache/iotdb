package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.qp.utils.MathUtil;
import org.apache.iotdb.infludb.qp.utils.TypeUtil;
import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class MeanFunction extends Aggregate {
    public List<Double> numbers = new ArrayList<>();

    public MeanFunction(List<Expression> expressionList) {
        super(expressionList);
    }

    public MeanFunction() {
    }


    @Override
    public void updateValue(FunctionValue functionValue) {
        Object value = functionValue.getValue();
        if (TypeUtil.checkDecimal(value)) {
            numbers.add(((Number) value).doubleValue());
        } else {
            throw new IllegalArgumentException("mean not valid type");
        }

    }

    @Override
    public FunctionValue calculate() {
        return new FunctionValue(numbers.size() == 0 ? numbers : String.valueOf(MathUtil.Mean(numbers)), 0L);
    }

}
