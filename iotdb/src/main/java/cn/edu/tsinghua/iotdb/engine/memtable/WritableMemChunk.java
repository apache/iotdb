package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.iotdb.utils.PrimitiveArrayList;
import cn.edu.tsinghua.iotdb.utils.PrimitiveArrayListFactory;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;


public class WritableMemChunk implements IWritableMemChunk {
    private TSDataType dataType;
    private PrimitiveArrayList list;

    public WritableMemChunk(TSDataType dataType) {
        this.dataType = dataType;
        this.list = PrimitiveArrayListFactory.getByDataType(dataType);
    }

    @Override
    public void write(long insertTime, String insertValue) {
        switch (dataType) {
            case BOOLEAN:
                putBoolean(insertTime, Boolean.valueOf(insertValue));
                break;
            case INT32:
                putInt(insertTime, Integer.valueOf(insertValue));
                break;
            case INT64:
                putLong(insertTime, Long.valueOf(insertValue));
                break;
            case FLOAT:
                putFloat(insertTime, Float.valueOf(insertValue));
                break;
            case DOUBLE:
                putDouble(insertTime, Double.valueOf(insertValue));
                break;
            case TEXT:
                putBinary(insertTime, Binary.valueOf(insertValue));
                break;
            default:
                throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
        }
    }

    @Override
    public void putLong(long t, long v) {
        list.putTimestamp(t, v);
    }

    @Override
    public void putInt(long t, int v) {
        list.putTimestamp(t, v);
    }

    @Override
    public void putFloat(long t, float v) {
        list.putTimestamp(t, v);
    }

    @Override
    public void putDouble(long t, double v) {
        list.putTimestamp(t, v);
    }

    @Override
    public void putBinary(long t, Binary v) {
        list.putTimestamp(t, v);
    }

    @Override
    public void putBoolean(long t, boolean v) {
        list.putTimestamp(t, v);
    }

    @Override
    //TODO: Consider using arrays to sort and remove duplicates
    public List<TimeValuePair> getSortedTimeValuePairList() {
        int length = list.size();
        TreeMap<Long, TsPrimitiveType> treeMap = new TreeMap<>();
        for (int i = 0; i < length; i++) {
            treeMap.put(list.getTimestamp(i), TsPrimitiveType.getByType(dataType, list.getValue(i)));
        }
        List<TimeValuePair> ret = new ArrayList<>();
        treeMap.forEach((k, v) -> {
            ret.add(new TimeValuePairInMemTable(k, v));
        });
        return ret;
    }

    @Override
    public void reset() {
        this.list = PrimitiveArrayListFactory.getByDataType(dataType);
    }

    @Override
    public int count() {
        return list.size();
    }

}
