package cn.edu.tsinghua.tsfile.timeseries.readV2.datatype;

import java.io.Serializable;

/**
 * @author Jinrui Zhang
 */
public class TimeValuePair implements Serializable{
    private long timestamp;
    private TsPrimitiveType value;

    public TimeValuePair(long timestamp, TsPrimitiveType value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public TsPrimitiveType getValue() {
        return value;
    }

    public void setValue(TsPrimitiveType value) {
        this.value = value;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(timestamp).append(" : ").append(getValue());
        return stringBuilder.toString();
    }

    public boolean equals(Object object) {
        if (object instanceof TimeValuePair) {
            return ((TimeValuePair) object).getTimestamp() == timestamp
                    && ((TimeValuePair) object).getValue() != null
                    && ((TimeValuePair) object).getValue().equals(value);
        }
        return false;
    }

    public int getSize() {
        return 8 + 8 + value.getSize();
    }
}
