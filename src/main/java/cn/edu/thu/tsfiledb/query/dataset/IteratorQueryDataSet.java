package cn.edu.thu.tsfiledb.query.dataset;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.readSupport.Field;
import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;


public abstract class IteratorQueryDataSet extends QueryDataSet {
    //special for save time values when processing cross getIndex
    public CrossQueryTimeGenerator timeQueryDataSet;
    private LinkedHashMap<Path, DynamicOneColumnData> retMap;

    private LinkedHashMap<Path, Boolean> hasMoreRet;

    public IteratorQueryDataSet(List<Path> paths) throws IOException {
        hasMoreRet = new LinkedHashMap<>();
        retMap = new LinkedHashMap<>();
        timeMap = new HashMap<>();
        for (Path p : paths) {
            DynamicOneColumnData res = getMoreRecordsForOneColumn(p, null);
            if (res == null) {
                throw new IOException("Selected column is NOT EXIST. " + p.getFullPath());
            }
            retMap.put(p, res);
            if (res.length == 0) {
                hasMoreRet.put(p, false);
            } else {
                hasMoreRet.put(p, true);
            }
        }
    }

    public abstract DynamicOneColumnData getMoreRecordsForOneColumn(Path colName, DynamicOneColumnData res) throws IOException;

    public void initForRecord() {
        heap = new PriorityQueue<>(retMap.size());

        for (Path p : retMap.keySet()) {
            DynamicOneColumnData res = retMap.get(p);
            //先将初始化的值放进去
            if (res != null && res.curIdx < res.length) {
                heapPut(res.getTime(res.curIdx));
            }
        }
    }

    public boolean hasNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }
        if (heap.peek() != null) {
            return true;
        }
        return false;
    }

    public RowRecord getNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }

        if (!hasNextRecord()) {
            return null;
        }

        Long minTime = heapGet();
        RowRecord r = new RowRecord(minTime, null, null);
        for (Path p : retMap.keySet()) {
            Field f;
            DynamicOneColumnData res = retMap.get(p);
            if (res.curIdx < res.length && minTime == res.getTime(res.curIdx)) {
                f = new Field(res.dataType, p.getDeltaObjectToString(), p.getMeasurementToString());
                f.setNull(false);
                putValueToField(res, res.curIdx, f);
                res.curIdx++;
                if (hasMoreRet.get(p) && res.curIdx >= res.length) {
                    res.clearData();
                    try {
                        res = getMoreRecordsForOneColumn(p, res);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    retMap.put(p, res);
                    if (res.length == 0) {
                        hasMoreRet.put(p, false);
                    }
                }
                if (res.curIdx < res.length) {
                    heapPut(res.getTime(res.curIdx));
                }
            } else {
                f = new Field(res.dataType, p.getMeasurementToString());
                f.setNull(true);
            }
            r.addField(f);
        }
        return r;
    }
}









