package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This is class is used for {@code OverflowQueryEngine.readWithoutFilter} and
 * {@code OverflowQueryEngine.readOneColumnValueUseFilter} when batch read.
 *
 * @author Jinrui Zhang
 */
public abstract class BatchReadRecordGenerator {
    public LinkedHashMap<Path, DynamicOneColumnData> retMap;
    private LinkedHashMap<Path, Boolean> hasMoreRet;
    private int noRetCount;
    private HashMap<Long, Integer> timeMap;
    private PriorityQueue<Long> heap;
    private int fetchSize;

    public BatchReadRecordGenerator(List<Path> paths, int fetchSize) throws ProcessorException, IOException {
        noRetCount = 0;
        retMap = new LinkedHashMap<>();
        hasMoreRet = new LinkedHashMap<>();
        timeMap = new HashMap<>();
        this.fetchSize = fetchSize;
        // init for every Series
        for (Path p : paths) {
            DynamicOneColumnData res = getMoreRecordsForOneColumn(p, null);
            retMap.put(p, res);
            if (res.valueLength == 0) {
                hasMoreRet.put(p, false);
                noRetCount++;
            } else {
                hasMoreRet.put(p, true);
            }
        }
        initHeap();
    }

    private void initHeap() {
        heap = new PriorityQueue<>();
        for (Path p : retMap.keySet()) {
            DynamicOneColumnData res = retMap.get(p);
            if (res.curIdx < res.valueLength) {
                heapPut(res.getTime(res.curIdx));
            }
        }
    }

    private void heapPut(long t) {
        if (!timeMap.containsKey(t)) {
            heap.add(t);
            timeMap.put(t, 1);
        }
    }

    private Long heapGet() {
        Long t = heap.poll();
        timeMap.remove(t);
        return t;
    }

    public void clearDataInLastQuery(DynamicOneColumnData res) {
        res.clearData();
    }

    public abstract DynamicOneColumnData getMoreRecordsForOneColumn(Path p
            , DynamicOneColumnData res) throws ProcessorException, IOException;

    /**
     * Calculate the fetchSize number RowRecords.
     * Invoking this method will remove the top value in heap until the OldRowRecord number reach to fetchSize.
     *
     * @throws ProcessorException exception in read process
     * @throws IOException exception in IO
     */
    public void calculateRecord() throws ProcessorException, IOException {
        int recordCount = 0;
        while (recordCount < fetchSize && noRetCount < retMap.size()) {
            Long minTime = heapGet();
            if (minTime == null) {
                break;
            }
            for (Path path : retMap.keySet()) {
                if (hasMoreRet.get(path)) {
                    DynamicOneColumnData res = retMap.get(path);
                    if (minTime.equals(res.getTime(res.curIdx))) {
                        res.curIdx++;
                        if (res.curIdx == res.valueLength) {
                            res = getMoreRecordsForOneColumn(path, res);
                            if (res.curIdx == res.valueLength) {
                                hasMoreRet.put(path, false);
                                noRetCount++;
                                continue;
                            }
                        }
                        heapPut(res.getTime(res.curIdx));
                    }
                }
            }
            recordCount++;
        }
    }
}