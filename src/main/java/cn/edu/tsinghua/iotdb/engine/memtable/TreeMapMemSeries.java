package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class TreeMapMemSeries implements IMemSeries{
    private static final Logger logger = LoggerFactory.getLogger(TreeMapMemSeries.class);

    private final TreeMap<Long, TsPrimitiveType> treeMap;
    private final TSDataType dataType;

    public TreeMapMemSeries(TSDataType dataType) {
        this.dataType = dataType;
        switch (dataType){
            case BOOLEAN:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case TEXT:
                treeMap = new TreeMap<>();
                break;
            case FIXED_LEN_BYTE_ARRAY:
            case ENUMS:
            case INT96:
            case BIGDECIMAL:
            default:
                logger.error("Not support data type: {}.", dataType);
                treeMap = null;
        }
    }

    private void checkDataType(TSDataType dataType){
        assert dataType == this.dataType;
    }

    @Override
    public void putBoolean(long t, boolean v) {
        checkDataType(TSDataType.BOOLEAN);
        treeMap.put(t,new TsPrimitiveType.TsBoolean(v));
    }

    @Override
    public void putLong(long t, long v) {
        checkDataType(TSDataType.INT64);
        treeMap.put(t,new TsPrimitiveType.TsLong(v));
    }

    @Override
    public void putInt(long t, int v) {
        checkDataType(TSDataType.INT32);
        treeMap.put(t,new TsPrimitiveType.TsInt(v));
    }

    @Override
    public void putFloat(long t, float v) {
        checkDataType(TSDataType.FLOAT);
        treeMap.put(t,new TsPrimitiveType.TsFloat(v));
    }

    @Override
    public void putDouble(long t, double v) {
        checkDataType(TSDataType.DOUBLE);
        treeMap.put(t,new TsPrimitiveType.TsDouble(v));
    }

    @Override
    public void putBinary(long t, Binary v) {
        checkDataType(TSDataType.TEXT);
        treeMap.put(t,new TsPrimitiveType.TsBinary(v));
    }

    @Override
    public void write(TSDataType dataType, long insertTime, String insertValue) {
        switch (dataType){
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
            case FIXED_LEN_BYTE_ARRAY:
            case ENUMS:
            case INT96:
            case BIGDECIMAL:
            default:
                logger.error("Writing data points not support data type:{}.", dataType);
        }
    }

    @Override
    public void sortAndDeduplicate() {
        //Do Nothing
    }

    @Override
    public List<TimeValuePair> getSortedTimeValuePairList() {
        List<TimeValuePair> ret = new ArrayList<>();
        treeMap.forEach((k, v) ->{ret.add(new TimeValuePair(k,v));});
        return ret;
    }

    @Override
    public void reset() {
        treeMap.clear();
    }

    @Override
    public int size() {
        //TODO: this implement just returns the number of data points in the tree set.
        return treeMap.size();
    }
}
