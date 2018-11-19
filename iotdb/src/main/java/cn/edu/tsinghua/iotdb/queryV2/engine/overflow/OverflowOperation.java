package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

/**
 * Created by zhangjinrui on 2018/1/21.
 */
public abstract class OverflowOperation {

    protected OverflowOperation(long leftBound, long rightBound) {
        this.leftBound = leftBound;
        this.rightBound = rightBound;
    }

    public enum OperationType {
        UPDATE, DELETE
    }

    private long leftBound;
    private long rightBound;

    public long getLeftBound() {
        return leftBound;
    }

    public long getRightBound() {
        return rightBound;
    }

    public abstract OperationType getType();

    public abstract TsPrimitiveType getValue();

    public boolean verifyTime(long time) {
        return leftBound <= time && rightBound >= time;
    }

    public String toString() {
        return leftBound + "," + rightBound;
    }

}
