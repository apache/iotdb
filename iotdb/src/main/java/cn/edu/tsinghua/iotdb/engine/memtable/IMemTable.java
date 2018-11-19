package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.util.Map;

/**
 * IMemTable is designed to store data points which are not flushed into TsFile yet. An instance of IMemTable maintains
 * all series belonging to one StorageGroup, corresponding to one FileNodeProcessor.<br>
 * The concurrent control of IMemTable is based on the concurrent control of FileNodeProcessor,
 * i.e., Writing and querying operations have gotten writeLock and readLock respectively.<br>
 *
 * @author Rong Kang
 */
public interface IMemTable {
    Map<String, Map<String, IMemSeries>> getMemTableMap();

    void write(String deltaObject, String measurement, TSDataType dataType, long insertTime, String insertValue);

    int size();

    IMemSeries query(String deltaObject, String measurement,TSDataType dataType);

    void clear();

    boolean isEmpty();

}

