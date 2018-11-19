package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public class OverflowUpdateOperation extends OverflowOperation {

    private TsPrimitiveType value;

    public OverflowUpdateOperation(long leftBound, long rightBound, TsPrimitiveType value) {
        super(leftBound, rightBound);
        this.value = value;
    }

    public TsPrimitiveType getValue() {
        return value;
    }

    public void setValue(TsPrimitiveType value) {
        this.value = value;
    }

    @Override
    public OperationType getType() {
        return OperationType.UPDATE;
    }
}
