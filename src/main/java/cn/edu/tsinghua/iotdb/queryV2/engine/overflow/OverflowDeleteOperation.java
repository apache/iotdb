package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

/**
 * Created by zhangjinrui on 2018/1/21.
 */
public class OverflowDeleteOperation extends OverflowOperation {

    public OverflowDeleteOperation(long leftBound, long rightBound) {
        super(leftBound, rightBound);
    }

    @Override
    public OperationType getType() {
        return OperationType.DELETE;
    }

    @Override
    public TsPrimitiveType getValue() {
        throw new UnsupportedOperationException("Delete operation does not support getValue()");
    }
}
