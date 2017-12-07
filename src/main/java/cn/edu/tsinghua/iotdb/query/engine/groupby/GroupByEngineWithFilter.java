package cn.edu.tsinghua.iotdb.query.engine.groupby;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.engine.EngineUtils;
import cn.edu.tsinghua.iotdb.query.engine.FilterStructure;
import cn.edu.tsinghua.iotdb.query.engine.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Group by aggregation implementation with <code>FilterStructure</code>.
 */
public class GroupByEngineWithFilter {

    private static final Logger LOG = LoggerFactory.getLogger(GroupByEngineWithFilter.class);

    /** aggregateFetchSize is set to calculate the result of timestamps, when the size of common timestamps is
     * up to aggregateFetchSize, the aggregation calculation process will begin**/
    private int aggregateFetchSize = 100000;

    /** crossQueryFetchSize is sed to read one column data, this variable is mainly used to debug to verify
     * the rightness of iterative readOneColumnWithoutFilter **/
    private int crossQueryFetchSize = 100000;

    /** all the group by Path ans its AggregateFunction **/
    private List<Pair<Path, AggregateFunction>> aggregations;

    /** group by origin **/
    private long origin;

    /** group by unit **/
    private long unit;

    /** SingleSeriesFilterExpression intervals is transformed to longInterval, all the split time intervals **/
    private LongInterval longInterval;

    /** represent the usage count of longInterval **/
    private int intervalIndex;

    /** filter in group by where clause **/
    private List<FilterStructure> filterStructures;

    /** group by partition fetch size, when result size is reach to partitionSize, the current
     *  calculation will be terminated
     */
    private int partitionFetchSize;

    /** HashMap to record the query result of each aggregation Path **/
    private Map<String, DynamicOneColumnData> queryPathResult = new HashMap<>();

    /** represent duplicated path index **/
    private Set<Integer> duplicatedPaths = new HashSet<>();

    /** group by result **/
    private QueryDataSet groupByResult = new QueryDataSet();


    // variables below are used to calculate the common timestamps of FilterStructures

    /** stores the query QueryDataSet of each FilterStructure in filterStructures **/
    private List<QueryDataSet> fsDataSets = new ArrayList<>();

    /** stores calculated common timestamps of each FilterStructure**/
    private List<long[]> fsTimeList = new ArrayList<>();

    /** stores used index of each fsTimeList **/
    private List<Integer> fsTimeIndexList = new ArrayList<>();

    /** represents whether this FilterStructure answer still has unread data **/
    private List<Boolean> fsHasUnReadDataList = new ArrayList<>();

    /** the aggregate timestamps calculated by all FilterStructures **/
    private List<Long> aggregateTimestamps = new ArrayList<>();

    /** priority queue to store timestamps of each FilterStructure **/
    private PriorityQueue<Long> commonTimeQueue = new PriorityQueue<>();

    private boolean queryCalcFlag = true;

    public GroupByEngineWithFilter(List<Pair<Path, AggregateFunction>> aggregations, List<FilterStructure> filterStructures,
                                 long origin, long unit, SingleSeriesFilterExpression intervals, int partitionFetchSize) throws IOException, ProcessorException {
        this.aggregations = aggregations;
        this.filterStructures = filterStructures;
        this.origin = origin;
        this.unit = unit;
        this.partitionFetchSize = partitionFetchSize;
        // test partitionFetchSize
        // this.partitionFetchSize = 2;
        this.longInterval = (LongInterval) FilterVerifier.create(TSDataType.INT64).getInterval(intervals);
        this.intervalIndex = 0;

        this.queryPathResult = new HashMap<>();
        for (int i = 0; i < aggregations.size(); i++) {
            String aggregateKey = aggregationKey(aggregations.get(i).left, aggregations.get(i).right);
            if (!groupByResult.mapRet.containsKey(aggregateKey)) {
                groupByResult.mapRet.put(aggregateKey,
                        new DynamicOneColumnData(aggregations.get(i).right.dataType, true, true));
                queryPathResult.put(aggregateKey, null);
            } else {
                duplicatedPaths.add(i);
            }
        }

        for (int idx = 0; idx < filterStructures.size(); idx++) {
            FilterStructure filterStructure = filterStructures.get(idx);
            QueryDataSet queryDataSet = new QueryDataSet();
            queryDataSet.crossQueryTimeGenerator = new CrossQueryTimeGenerator(filterStructure.getTimeFilter(),
                    filterStructure.getFrequencyFilter(), filterStructure.getValueFilter(), crossQueryFetchSize) {
                @Override
                public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                               SingleSeriesFilterExpression valueFilter, int valueFilterNumber)
                        throws ProcessorException, IOException {
                    try {
                        return getDataUseSingleValueFilter(valueFilter, freqFilter, res, fetchSize, valueFilterNumber);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
            fsDataSets.add(queryDataSet);
            long[] commonTimestamps = queryDataSet.crossQueryTimeGenerator.generateTimes();
            fsTimeList.add(commonTimestamps);
            fsTimeIndexList.add(0);
            if (commonTimestamps.length > 0) {
                fsHasUnReadDataList.add(true);
            } else {
                fsHasUnReadDataList.add(false);
            }
        }

        LOG.debug("construct GroupByEngineWithFilter successfully");
    }

    /**
     * @return
     * @throws IOException
     * @throws ProcessorException
     */
    public QueryDataSet groupBy() throws IOException, ProcessorException, PathErrorException {

        groupByResult.clear();
        int partitionBatchCount = 0;

        if (intervalIndex >= longInterval.count) {
            groupByResult.clear();
            return groupByResult;
        }

        long partitionStart = origin; // partition start time
        long partitionEnd = origin + unit - 1; // partition end time
        long intervalStart = longInterval.flag[intervalIndex] ? longInterval.v[intervalIndex] : longInterval.v[intervalIndex] + 1; // interval start time
        long intervalEnd = longInterval.flag[intervalIndex+1] ? longInterval.v[intervalIndex+1] : longInterval.v[intervalIndex+1] - 1; // interval end time

        if (commonTimeQueue.isEmpty()) {
            for (int i = 0; i < fsTimeList.size(); i++) {
                boolean flag = fsHasUnReadDataList.get(i);
                if (flag) {
                    commonTimeQueue.add(fsTimeList.get(i)[fsTimeIndexList.get(i)]);
                }
            }
        }

        while (true) {
            while (aggregateTimestamps.size() < aggregateFetchSize && !commonTimeQueue.isEmpty()) {
                // add the minimum timestamp in commonTimeQueue,
                // remove other time which is equals to minimum timestamp in fsTimeList
                long minTime = commonTimeQueue.poll();
                aggregateTimestamps.add(minTime);
                while (!commonTimeQueue.isEmpty() && minTime == commonTimeQueue.peek())
                    commonTimeQueue.poll();

                for (int i = 0; i < fsTimeList.size(); i++) {
                    boolean flag = fsHasUnReadDataList.get(i);
                    if (flag) {
                        int curTimeIdx = fsTimeIndexList.get(i);
                        long[] curTimestamps = fsTimeList.get(i);
                        // remove all timestamps equal to min time in all series
                        while (curTimeIdx < curTimestamps.length && curTimestamps[curTimeIdx] == minTime) {
                            curTimeIdx++;
                        }
                        if (curTimeIdx < curTimestamps.length) {
                            fsTimeIndexList.set(i, curTimeIdx);
                            commonTimeQueue.add(curTimestamps[curTimeIdx]);
                        } else {
                            long[] newTimeStamps = fsDataSets.get(i).crossQueryTimeGenerator.generateTimes();
                            if (newTimeStamps.length > 0) {
                                fsTimeList.set(i, newTimeStamps);
                                fsTimeIndexList.set(i, 0);
                            } else {
                                fsHasUnReadDataList.set(i, false);
                            }
                        }
                    }
                }
            }

            //LOG.debug("common timestamps calculated in GroupBy process : " + aggregateTimestamps.toString());

            if (queryCalcFlag) {
                calcPathQueryData();
                queryCalcFlag = false;
            }

            // this process is on the basis of the traverse of partition variable,
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

                // current partition is locate in the left of intervals, using mod operator
                // to calculate the first satisfied partition which has intersection with intervals.
                if (partitionEnd < intervalStart) {
                    partitionStart = intervalStart - ((intervalStart - origin) % unit);
                    partitionEnd = partitionStart + unit - 1;
                }
                if (partitionStart < intervalStart) {
                    partitionStart = intervalStart;
                }

                while (true) {
                    int cnt = 0;
                    for (Pair<Path, AggregateFunction> pair : aggregations) {
                        if (duplicatedPaths.contains(cnt))
                            continue;
                        cnt++;
                        Path path = pair.left;
                        AggregateFunction aggregateFunction = pair.right;
                        String aggregationKey = aggregationKey(path, aggregateFunction);
                        DynamicOneColumnData data = queryPathResult.get(aggregationKey);

                        aggregateFunction.calcGroupByAggregation(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                    }

                    // TODO need consider that, the aggregateTimestamps is smaller than intervalEnd, intervalEnd is smaller than partitionEnd
                    if (intervalEnd <= partitionEnd &&
                            (aggregateTimestamps.size() == 0 || intervalEnd < aggregateTimestamps.get(aggregateTimestamps.size()-1))) {
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

                if (aggregateTimestamps.size() > 0 && partitionEnd < aggregateTimestamps.get(aggregateTimestamps.size()-1)) {
                    partitionStart = partitionEnd + 1;
                    partitionEnd = partitionStart + unit - 1;
                    partitionBatchCount += 1;

                    if (partitionBatchCount > partitionFetchSize) {
                        origin = partitionStart;
                        break;
                    }
                } else if (aggregateTimestamps.size() == 0) {
                    // aggregate timestamps is empty
                    // calculate the next partition range directly
                    partitionStart = partitionEnd + 1;
                    partitionEnd = partitionStart + unit - 1;
                    partitionBatchCount += 1;

                    if (partitionBatchCount > partitionFetchSize) {
                        origin = partitionStart;
                        break;
                    }
                } else if (partitionEnd >= aggregateTimestamps.get(aggregateTimestamps.size()-1)){
                    // partitionEnd is greater or equals than the last value of aggregate timestamps
                    aggregateTimestamps.clear();
                    queryCalcFlag = true;
                    break;
                } else {
                    break;
                }
            }

            if (intervalIndex >= longInterval.count)
                break;

            if (partitionBatchCount > partitionFetchSize) {
                break;
            }

            // partitionStart is greater or equals than the last value of aggregateTimestamps
            // the next batch aggregateTimestamps should be loaded
            if (aggregateTimestamps.size() > 0 && partitionStart >= aggregateTimestamps.get(aggregateTimestamps.size()-1)) {
                aggregateTimestamps.clear();
                queryCalcFlag = true;
            }
        }

        int cnt = 0;
        for (Pair<Path, AggregateFunction> pair : aggregations) {
            if (duplicatedPaths.contains(cnt))
                continue;
            cnt++;
            Path path = pair.left;
            AggregateFunction aggregateFunction = pair.right;
            groupByResult.mapRet.put(aggregationKey(path, aggregateFunction), aggregateFunction.resultData);
        }
        //LOG.debug("calculate group by result successfully.");
        return groupByResult;
    }

    private void calcPathQueryData() throws ProcessorException, PathErrorException, IOException {
        int aggregationOrdinal = 0;
        for (Pair<Path, AggregateFunction> pair : aggregations) {
            Path path = pair.left;
            AggregateFunction aggregateFunction = pair.right;
            String aggregationKey = aggregationKey(path, aggregateFunction);
            if (duplicatedPaths.contains(aggregationOrdinal)) {
                continue;
            }
            aggregationOrdinal++;

            DynamicOneColumnData data = queryPathResult.get(aggregationKey);
            // common aggregate timestamps is empty
            // the query data of path should be clear too
            if (aggregateTimestamps.size() == 0) {
                if (data != null) {
                    data.clearData();
                } else {
                    data = new DynamicOneColumnData(aggregateFunction.dataType, true);
                    queryPathResult.put(aggregationKey, data);
                }
                queryCalcFlag = true;
                continue;
            }

            String deltaObjectId = path.getDeltaObjectToString();
            String measurementId = path.getMeasurementToString();
            String recordReaderPrefix = ReadCachePrefix.addQueryPrefix(aggregationOrdinal);
            RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectId, measurementId,
                    null, null, null, null, recordReaderPrefix);

            if (recordReader.insertAllData == null) {
                List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null,
                        null, recordReader.insertPageInMemory, recordReader.overflowInfo);
                DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
                DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
                DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
                SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

                recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                        insertTrue, updateTrue, updateFalse,
                        newTimeFilter, null, null, MManager.getInstance().getSeriesType(path.getFullPath()));
                data = recordReader.queryUsingTimestamps(deltaObjectId, measurementId,
                        recordReader.insertAllData.timeFilter, aggregateTimestamps.stream().mapToLong(i->i).toArray(), recordReader.insertAllData);
                data.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
                queryPathResult.put(aggregationKey, data);
            } else {
                data = recordReader.queryUsingTimestamps(deltaObjectId, measurementId,
                        recordReader.insertAllData.timeFilter, aggregateTimestamps.stream().mapToLong(i->i).toArray(), recordReader.insertAllData);
                queryPathResult.put(aggregationKey, data);
            }
        }
    }

    /**
     * This function is only used for CrossQueryTimeGenerator.
     * A CrossSeriesFilterExpression is consist of many SingleSeriesFilterExpression.
     * e.g. CSAnd(d1.s1, d2.s1) is consist of d1.s1 and d2.s1, so this method would be invoked twice,
     * once for querying d1.s1, once for querying d2.s1.
     * <p>
     * When this method is invoked, need add the filter index as a new parameter, for the reason of exist of
     * <code>RecordReaderCache</code>, if the composition of CrossFilterExpression exist same SingleFilterExpression,
     * we must guarantee that the <code>RecordReaderCache</code> doesn't cause conflict to the same SingleFilterExpression.
     */
    private static DynamicOneColumnData getDataUseSingleValueFilter(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter,
                                                                    DynamicOneColumnData res, int fetchSize, int valueFilterNumber)
            throws ProcessorException, IOException, PathErrorException {

        String deltaObjectUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getDeltaObjectUID();
        String measurementUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getMeasurementUID();
        //TODO may have dnf conflict
        String valueFilterPrefix = ReadCachePrefix.addFilterPrefix(valueFilterNumber);

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                null, freqFilter, valueFilter, null, valueFilterPrefix);

        if (res == null) {
            // get four overflow params
            List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, freqFilter, valueFilter,
                    res, recordReader.insertPageInMemory, recordReader.overflowInfo);

            DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
            DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
            DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
            SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    newTimeFilter, valueFilter, null, MManager.getInstance().getSeriesType(deltaObjectUID + "." + measurementUID));
            res = recordReader.queryOneSeries(deltaObjectUID, measurementUID,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, valueFilter, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.queryOneSeries(deltaObjectUID, measurementUID,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, valueFilter, res, fetchSize);
        }

        return res;
    }

    private String aggregationKey(Path path, AggregateFunction aggregateFunction) {
        return aggregateFunction.name + "(" + path.getFullPath() + ")";
    }
}
