package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public class CountFunction extends Aggregate {
    public int countNum = 0;

    public CountFunction(List<Expression> expressionList) {
        super(expressionList);
    }

    @Override
    public void updateValue(FunctionValue functionValue) {
        this.countNum++;
    }

    public CountFunction() {
    }

    @Override
    public FunctionValue calculate() {
        return new FunctionValue(String.valueOf(this.countNum), 0L);

    }
}
