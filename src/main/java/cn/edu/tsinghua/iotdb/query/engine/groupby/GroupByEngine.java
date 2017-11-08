package cn.edu.tsinghua.iotdb.query.engine.groupby;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggreFuncFactory;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationResult;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.engine.EngineUtils;
import cn.edu.tsinghua.iotdb.query.engine.FilterStructure;
import cn.edu.tsinghua.iotdb.query.engine.OverflowQueryEngine;
import cn.edu.tsinghua.iotdb.query.engine.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NoFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.read.query.BatchReadRecordGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;

import java.io.IOException;
import java.util.*;

/**
 * Group by aggregation implementation.
 */
public class GroupByEngine {

    // ThreadLocal<>
    private int formNumber = -1;
    private List<Path> queryPaths = new ArrayList<>();
    private ThreadLocal<Boolean> threadLocal = new ThreadLocal<>();

    public QueryDataSet groupBy(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures,
                                       long unit, long origin, FilterExpression intervals, int fetchSize)
            throws IOException, ProcessorException, PathErrorException {

        for (Pair<Path, String> pair : aggres) {
            queryPaths.add(pair.left);
        }
        List<Pair<Path, AggregateFunction>> aggregations = new ArrayList<>();
        for (Pair<Path, String> pair : aggres) {
            TSDataType dataType = MManager.getInstance().getSeriesType(pair.left.getFullPath());
            AggregateFunction func = AggreFuncFactory.getAggrFuncByName(pair.right, dataType);
            aggregations.add(new Pair<>(pair.left, func));
        }

        boolean noFilterFlag = false;
        if (filterStructures == null || filterStructures.size() == 0 || (filterStructures.size() == 1 && filterStructures.get(0).noFilter())) {
            noFilterFlag = true;
        }

        QueryDataSet groupByResult = new QueryDataSet();

        // all the split time intervals
        LongInterval longInterval = new LongInterval();
        // longInterval = (LongInterval) FilterVerifier.create(TSDataType.INT64).getInterval((SingleSeriesFilterExpression) intervals);
        longInterval.addValueFlag(1L, true);
        longInterval.addValueFlag(10000L, true);

        if (longInterval.count == 0) {
            return new QueryDataSet();
        }

        long partitionStart = origin; // partition start time
        long partitionEnd = origin + unit - 1; // partition end time
        int intervalIndex = 0;
        long intervalStart = longInterval.flag[0] ? longInterval.v[0] : longInterval.v[0] + 1; // interval start time
        long intervalEnd = longInterval.flag[1] ? longInterval.v[1] : longInterval.v[1] - 1; // interval end time

        // HashMap to record the query result of each aggregation Path
        Map<String, DynamicOneColumnData> queryPathResult = new HashMap<>();
        // TODO HashMap to record the read lock of each query aggregation path
        Map<String, Integer> readLockMap = new HashMap<>();
        // HashSet to record the duplicated queries
        Set<Integer> duplicatedPaths = new HashSet<>();
        for (int i = 0;i < aggregations.size(); i++) {
            String aggregateKey = aggregationKey(aggregations.get(i).left, aggregations.get(i).right);
            if (!groupByResult.mapRet.containsKey(aggregateKey)) {
                groupByResult.mapRet.put(aggregateKey, new DynamicOneColumnData(aggregations.get(i).right.dataType, true, true));
                queryPathResult.put(aggregateKey, null);
            } else {
                duplicatedPaths.add(i);
            }
        }

        // this process is on the basis of the traverse of partition variable
        // in each [partitionStart, partitionEnd], [intervalStart, intervalEnd] would be considered
        while (true) {

            // after this, intervalEnd must be bigger or equals than partitionStart
            while (intervalEnd < partitionStart) {
                intervalIndex += 2;
                if (intervalIndex >= longInterval.count)
                    break;
                intervalStart = longInterval.flag[intervalIndex] ? longInterval.v[intervalIndex] : longInterval.v[intervalIndex] + 1;
                intervalEnd = longInterval.flag[intervalIndex + 1] ? longInterval.v[intervalIndex + 1] : longInterval.v[intervalIndex + 1] - 1;
            }

            // current partition is location in the left of intervals, using mod operator
            // to calculate the first satisfied partition which has intersection with intervals.
            if (partitionEnd < intervalStart) {
                partitionStart = intervalStart - ((intervalStart - origin) % unit);
                partitionEnd = partitionStart + unit - 1;
            }
            if (partitionStart < intervalStart) {
                partitionStart = intervalStart;
            }

            //System.out.println(partitionStart + "---" + partitionEnd);
            if (partitionStart == 9999) {
                System.out.println("haha");
            }
            while (true) {
                int cnt = 0;
                for (Pair<Path, AggregateFunction> pair : aggregations) {
                    if (duplicatedPaths.contains(cnt))
                        continue;

                    Path path = pair.left;
                    AggregateFunction aggregateFunction = pair.right;
                    String aggregationKey = aggregationKey(path, aggregateFunction);
                    DynamicOneColumnData data = queryPathResult.get(aggregationKey);
                    if (data == null || data.curIdx >= data.timeLength) {
                        data = queryOnePath(path, data, filterStructures);
                        queryPathResult.put(aggregationKey, data);
                    }

                    while (true) {
                        aggregateFunction.calcGroupByAggregationWithoutFilter(partitionStart, partitionEnd, intervalStart, intervalEnd, data, false);
                        if (data.timeLength == 0 || data.curIdx < data.timeLength) {
                            break;
                        }
                        if (data.curIdx >= data.timeLength && data.timeLength != 0) {
                            data = queryOnePath(path, data, filterStructures);
                        }
                        if (data.timeLength == 0 || data.curIdx >= data.timeLength) {
                            break;
                        }
                    }
                }

                if (intervalEnd <= partitionEnd) {
                    intervalIndex += 2;
                    if (intervalIndex >= longInterval.count)
                        break;
                    intervalStart = longInterval.flag[intervalIndex] ? longInterval.v[intervalIndex] : longInterval.v[intervalIndex] + 1;
                    intervalEnd = longInterval.flag[intervalIndex + 1] ? longInterval.v[intervalIndex + 1] : longInterval.v[intervalIndex + 1] - 1;
                } else {
                    break;
                }
            }

            if (intervalIndex >= longInterval.count)
                break;

            partitionStart = partitionEnd + 1;
            partitionEnd = partitionStart + unit - 1;
        }

        int cnt = 0;
        for (Pair<Path, AggregateFunction> pair : aggregations) {
            if (duplicatedPaths.contains(cnt))
                cnt++;
            Path path = pair.left;
            AggregateFunction aggregateFunction = pair.right;
            groupByResult.mapRet.put(aggregationKey(path, aggregateFunction), aggregateFunction.result.data);
        }
        return groupByResult;
    }

    private DynamicOneColumnData queryOnePath(Path path, DynamicOneColumnData data, List<FilterStructure> filterStructures)
            throws PathErrorException, IOException, ProcessorException {
        if (filterStructures == null || filterStructures.size() == 0 || (filterStructures.size() == 1 && filterStructures.get(0).noFilter())) {
            return readOneColumnWithoutFilter(path, data, 10000, null);
        }
        return null;
    }

    private DynamicOneColumnData readOneColumnWithoutFilter(Path path, DynamicOneColumnData res, int fetchSize, Integer readLock) throws ProcessorException, IOException, PathErrorException {

        String deltaObjectID = path.getDeltaObjectToString();
        String measurementID = path.getMeasurementToString();
        String recordReaderPrefix = ReadCachePrefix.addQueryPrefix(formNumber);

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectID, measurementID,
                null, null, null, readLock, recordReaderPrefix);

        if (res == null) {
            // get overflow params merged with bufferwrite insert data
            List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null,
                    res, recordReader.insertPageInMemory, recordReader.overflowInfo);

            DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
            DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
            DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
            SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    newTimeFilter, null, null, MManager.getInstance().getSeriesType(path.getFullPath()));
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectID, measurementID,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, null, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectID, measurementID,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, null, res, fetchSize);
        }

        return res;
    }

    private String aggregationKey(Path path, AggregateFunction aggregateFunction) {
        return aggregateFunction.name + "(" + path.getFullPath() + ")";
    }
}
