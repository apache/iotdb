package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.Map;


public abstract class AbstractMemTable implements IMemTable {

    @Override
    public Map<String, Map<String, IWritableMemChunk>> getMemTableMap() {
        return memTableMap;
    }

    private final Map<String, Map<String, IWritableMemChunk>> memTableMap;

    public AbstractMemTable() {
        this.memTableMap = new HashMap<>();
    }

    /**
     * check whether the given seriesPath is within this memtable.
     *
     * @return true if seriesPath is within this memtable
     */
    private boolean checkPath(String deviceId, String measurement) {
        return memTableMap.containsKey(deviceId) &&
                memTableMap.get(deviceId).containsKey(measurement);
    }

    private IWritableMemChunk createIfNotExistAndGet(String deviceId, String measurement, TSDataType dataType) {
        if (!memTableMap.containsKey(deviceId)) {
            memTableMap.put(deviceId, new HashMap<>());
        }
        Map<String, IWritableMemChunk> memSeries = memTableMap.get(deviceId);
        if (!memSeries.containsKey(measurement)) {
            memSeries.put(measurement, genMemSeries(dataType));
        }
        return memSeries.get(measurement);
    }

    protected abstract IWritableMemChunk genMemSeries(TSDataType dataType);

    @Override
    public void write(String deviceId, String measurement, TSDataType dataType, long insertTime, String insertValue) {
        IWritableMemChunk memSeries = createIfNotExistAndGet(deviceId, measurement, dataType);
        memSeries.write(insertTime, insertValue);
    }

    @Override
    public int size() {
        int sum = 0;
        for (Map<String, IWritableMemChunk> seriesMap : memTableMap.values()) {
            for (IWritableMemChunk iMemSeries : seriesMap.values()) {
                sum += iMemSeries.count();
            }
        }
        return sum;
    }

    @Override
    public void clear() {
        memTableMap.clear();
    }

    @Override
    public boolean isEmpty() {
        return memTableMap.isEmpty();
    }


    @Override
    public TimeValuePairSorter query(String deviceId, String measurement, TSDataType dataType) {
        if (!checkPath(deviceId, measurement))
            return new WritableMemChunk(dataType);
        return memTableMap.get(deviceId).get(measurement);
    }

}
