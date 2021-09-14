package org.apache.iotdb.infludb.qp.logical.function;

public class FunctionValue {
    private Object value;
    private Long timestamp;

    public FunctionValue(Object value, Long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
