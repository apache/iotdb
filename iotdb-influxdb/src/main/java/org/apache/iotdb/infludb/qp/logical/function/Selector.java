package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public abstract class Selector extends Function {


    //值对应的时间戳
    private Long timestamp;

    private List<Object> relatedValues;

    public Selector() {

    }

    public Selector(List<Expression> expressionList) {
        super(expressionList);
    }

    public abstract void updateValueAndRelate(FunctionValue functionValue, List<Object> values);

    public List<Object> getRelatedValues() {
        return this.relatedValues;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setRelatedValues(List<Object> relatedValues) {
        this.relatedValues = relatedValues;
    }
}
