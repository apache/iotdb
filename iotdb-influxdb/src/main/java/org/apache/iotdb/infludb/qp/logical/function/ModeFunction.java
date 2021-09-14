package org.apache.iotdb.infludb.qp.logical.function;

import kotlin.Pair;
import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModeFunction extends Aggregate {
    private Map<Object, Integer> valueOrders = new HashMap<>();
    private Map<Object, Long> valueLastTimestamp = new HashMap<>();
    private int maxNumber = 0;
    private Object maxObject = null;

    public ModeFunction(List<Expression> expressionList) {
        super(expressionList);
    }

    public ModeFunction() {
    }

    @Override
    public void updateValue(FunctionValue functionValue) {
        Object value = functionValue.getValue();
        Long timestamp = functionValue.getTimestamp();
        //更新新的数据
        if (!valueOrders.containsKey(value)) {
            valueOrders.put(value, 1);
            valueLastTimestamp.put(value, timestamp);
        } else {
            valueOrders.put(value, valueOrders.get(value) + 1);
            if (timestamp < valueLastTimestamp.get(value)) {
                valueLastTimestamp.put(value, timestamp);
            }
        }
        //判断新的数据是否处于满足条件的情况
        if (maxObject == null) {
            maxObject = value;
            maxNumber = 1;
        } else {
            if (valueOrders.get(value) > maxNumber) {
                maxNumber = valueOrders.get(value);
                maxObject = value;
            } else if (valueOrders.get(value) == maxNumber && timestamp < valueLastTimestamp.get(maxObject)) {
                maxObject = value;
            }
        }
    }


    @Override
    public FunctionValue calculate() {
        return new FunctionValue(maxObject, 0L);
    }
}
