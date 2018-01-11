package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * used for overflow insert operation
 */
public class InsertOperation {
    private DynamicOneColumnData insertOperation;
    private int idx;

    public InsertOperation(TSDataType dataType, DynamicOneColumnData data) {
        if (data == null) {
            this.insertOperation = new DynamicOneColumnData(dataType, true);
        } else {
            this.insertOperation = data;
        }
        idx = 0;
    }

    public boolean hasNext() {
        return idx < insertOperation.valueLength;
    }

    public long getInsertTime() {
        return insertOperation.getTime(idx);
    }

    public void next() {
        idx ++;
    }

    public int getInt() {
        return insertOperation.getInt(idx);
    }

    public long getLong() {
        return insertOperation.getLong(idx);
    }

    public float getFloat() {
        return insertOperation.getFloat(idx);
    }

    public double getDouble() {
        return insertOperation.getDouble(idx);
    }

    public boolean getBoolean() {
        return insertOperation.getBoolean(idx);
    }

    public Binary getText() {
        return insertOperation.getBinary(idx);
    }
}
