package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public abstract class IteratorOnePassQueryDataSet extends OnePassQueryDataSet {
    private static final Logger logger = LoggerFactory.getLogger(IteratorOnePassQueryDataSet.class);
    public LinkedHashMap<Path, DynamicOneColumnData> retMap;
    private LinkedHashMap<Path, Boolean> hasMoreRet;

    public IteratorOnePassQueryDataSet(List<Path> paths) throws IOException {
        hasMoreRet = new LinkedHashMap<>();
        retMap = new LinkedHashMap<>();
        timeMap = new HashMap<>();
        for (Path p : paths) {
            DynamicOneColumnData res = getMoreRecordsForOneColumn(p, null);

            retMap.put(p, res);
            if (res == null || res.valueLength == 0) {
                hasMoreRet.put(p, false);
            } else {
                hasMoreRet.put(p, true);
            }
        }
    }

    public abstract DynamicOneColumnData getMoreRecordsForOneColumn(Path colName
            , DynamicOneColumnData res) throws IOException;

    //modified by hadoop
    public void initForRecord() {
        size = retMap.size();
        heap = new PriorityQueue<>(size);

        if (size > 0) {
            deltaObjectIds = new String[size];
            measurementIds = new String[size];
        } else {
            LOG.error("OnePassQueryDataSet init row record occurs error! the size of ret is 0.");
        }

        int i = 0;
        for (Path p : retMap.keySet()) {
            deltaObjectIds[i] = p.getDeltaObjectToString();
            measurementIds[i] = p.getMeasurementToString();

            DynamicOneColumnData res = retMap.get(p);
            if (res != null && res.curIdx < res.valueLength) {
                heapPut(res.getTime(res.curIdx));
            }
            i++;
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

    //modified by hadoop
    public OldRowRecord getNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }

        if (!hasNextRecord()) {
            return null;
        }

        Long minTime = heapGet();
        OldRowRecord r = new OldRowRecord(minTime, null, null);
        for (Path p : retMap.keySet()) {
            Field f;
            DynamicOneColumnData res = retMap.get(p);
            if (res.curIdx < res.valueLength && minTime == res.getTime(res.curIdx)) {
                f = new Field(res.dataType, p.getDeltaObjectToString(), p.getMeasurementToString());
                f.setNull(false);
                putValueToField(res, res.curIdx, f);
                res.curIdx++;
                if (hasMoreRet.get(p) && res.curIdx >= res.valueLength) {
                    res.clearData();
                    try {
                        res = getMoreRecordsForOneColumn(p, res);
                    } catch (IOException e) {
                        logger.error("", e);
                    }
                    retMap.put(p, res);
                    if (res.valueLength == 0) {
                        hasMoreRet.put(p, false);
                    }
                }
                if (res.curIdx < res.valueLength) {
                    heapPut(res.getTime(res.curIdx));
                }
            } else {
                f = new Field(res.dataType, p.getDeltaObjectToString(), p.getMeasurementToString());
                f.setNull(true);
            }
            r.addField(f);
        }
        return r;
    }
}