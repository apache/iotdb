package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Rong Kang
 */
public abstract class AbstractMemTable implements IMemTable{

    @Override
    public Map<String, Map<String, IMemSeries>> getMemTableMap() {
        return memTableMap;
    }

    private final Map<String, Map<String, IMemSeries>> memTableMap;

    public AbstractMemTable() {
        this.memTableMap = new HashMap<>();
    }
    
    /**
     * check whether the given path is within this memtable.
     *
     * @return true if path is within this memtable
     *
     */
    private boolean checkPath(String deltaObject, String measurement) {
        return memTableMap.containsKey(deltaObject) &&
                memTableMap.get(deltaObject).containsKey(measurement);
    }

    private IMemSeries addSeriesIfNone(String deltaObject, String measurement, TSDataType dataType) {
        if(!memTableMap.containsKey(deltaObject)) {
            memTableMap.put(deltaObject, new HashMap<>());
        }
        Map<String, IMemSeries> memSeries = memTableMap.get(deltaObject);
        if(!memSeries.containsKey(measurement)) {
            memSeries.put(measurement, genMemSeries(dataType));
        }
        return memSeries.get(measurement);
    }

    protected abstract IMemSeries genMemSeries(TSDataType dataType);

    @Override
    public void write(String deltaObject, String measurement, TSDataType dataType, long insertTime, String insertValue) {
        IMemSeries memSeries = addSeriesIfNone(deltaObject, measurement, dataType);
        memSeries.write(dataType,insertTime,insertValue);
    }

    @Override
    public int size() {
        int sum = 0;
        for (Map<String, IMemSeries> seriesMap : memTableMap.values()) {
            for (IMemSeries iMemSeries : seriesMap.values()) {
                sum += iMemSeries.size();
            }
        }
        return sum;
    }
    
    @Override
    public void clear(){
    	memTableMap.clear();
    }
    
    @Override
    public boolean isEmpty() {
    	return memTableMap.isEmpty();
    }


    @Override
    public IMemSeries query(String deltaObject, String measurement, TSDataType dataType) {
        if(!checkPath(deltaObject,measurement))
        	return new PrimitiveMemSeries(dataType);
        return memTableMap.get(deltaObject).get(measurement);
    }

    @Override
    public void resetMemSeries(String deltaObject, String measurement) {
        if(!memTableMap.containsKey(deltaObject)) {
            return;
        }
        Map<String, IMemSeries> memSeries = memTableMap.get(deltaObject);
        if(memSeries.containsKey(measurement)) {
            memSeries.get(measurement).reset();
        }
    }
}
