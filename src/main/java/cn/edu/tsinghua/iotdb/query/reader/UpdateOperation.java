package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * used for overflow update operation
 */
public class UpdateOperation {
    private DynamicOneColumnData updateOperation;
    private int idx;

    public UpdateOperation(TSDataType dataType, DynamicOneColumnData data) {
        if (data == null) {
            this.updateOperation = new DynamicOneColumnData(dataType, true);
        } else {
            this.updateOperation = data;
        }
        idx = 0;
    }

    public boolean hasNext() {
        return idx < updateOperation.valueLength;
    }

    public long getUpdateStartTime() {
        return updateOperation.getTime(idx * 2);
    }

    public long getUpdateEndTime() {
        return updateOperation.getTime(idx * 2 + 1);
    }

    public void next() {
        idx ++;
    }

    public int getInt() {
        return updateOperation.getInt(idx);
    }

    public long getLong() {
        return updateOperation.getLong(idx);
    }

    public float getFloat() {
        return updateOperation.getFloat(idx);
    }

    public double getDouble() {
        return updateOperation.getDouble(idx);
    }

    public boolean getBoolean() {
        return updateOperation.getBoolean(idx);
    }

    public Binary getText() {
        return updateOperation.getBinary(idx);
    }

    public boolean verify(long time) {
        if (idx < updateOperation.valueLength && getUpdateStartTime() <= time && getUpdateEndTime() >= time)
            return true;

        return false;
    }
}
