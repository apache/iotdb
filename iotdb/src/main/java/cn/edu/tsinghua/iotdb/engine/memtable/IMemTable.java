package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

/**
 * IMemTable is designed to store data points which are not flushed into TsFile yet. An instance of IMemTable maintains
 * all series belonging to one StorageGroup, corresponding to one FileNodeProcessor.<br>
 * The concurrent control of IMemTable is based on the concurrent control of FileNodeProcessor,
 * i.e., Writing and querying operations have gotten writeLock and readLock respectively.<br>
 */
public interface IMemTable {

    Map<String, Map<String, IWritableMemChunk>> getMemTableMap();

    void write(String deviceId, String measurement, TSDataType dataType, long insertTime, String insertValue);

    int size();

    TimeValuePairSorter query(String deviceId, String measurement,TSDataType dataType);

    /**
     * release all the memory resources
     */
    void clear();

    boolean isEmpty();

}

