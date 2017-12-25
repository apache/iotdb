package cn.edu.tsinghua.iotdb.query.engine.groupby;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.engine.EngineUtils;
import cn.edu.tsinghua.iotdb.query.engine.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.aggregationKey;

/**
 * Group by aggregation implementation without <code>FilterStructure</code>.
 */
public class GroupByEngineNoFilter {

    private static final Logger LOG = LoggerFactory.getLogger(GroupByEngineNoFilter.class);

    /** queryFetchSize is sed to read one column data, this variable is mainly used to debug to verify
     * the rightness of iterative readOneColumnWithoutFilter **/
    private int queryFetchSize = 100000;

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

    /** group by partition fetch size, when result size is reach to partitionSize, the current
     *  calculation will be terminated.
     *  this variable could be set small to test
     */
    private int partitionFetchSize;

    /** HashMap to record the query result of each aggregation Path **/
    private Map<String, DynamicOneColumnData> queryPathResult = new HashMap<>();

    /** represent duplicated path index **/
    private Set<Integer> duplicatedPaths = new HashSet<>();

    private QueryDataSet groupByResult = new QueryDataSet();

    private SingleSeriesFilterExpression timeFilter;

    public GroupByEngineNoFilter(List<Pair<Path, AggregateFunction>> aggregations, SingleSeriesFilterExpression timeFilter,
                                  long origin, long unit, SingleSeriesFilterExpression intervals, int partitionFetchSize) {
        this.aggregations = aggregations;
        this.timeFilter = timeFilter;
        this.queryPathResult = new HashMap<>();
        for (int i = 0; i < aggregations.size(); i++) {
            String aggregateKey = aggregationKey(aggregations.get(i).right, aggregations.get(i).left);
            if (!groupByResult.mapRet.containsKey(aggregateKey)) {
                groupByResult.mapRet.put(aggregateKey, new DynamicOneColumnData(aggregations.get(i).right.dataType, true, true));
                queryPathResult.put(aggregateKey, null);
            } else {
                duplicatedPaths.add(i);
            }
        }

        this.origin = origin;
        this.unit = unit;
        this.partitionFetchSize = partitionFetchSize;

        this.longInterval = (LongInterval) FilterVerifier.create(TSDataType.INT64).getInterval(intervals);
        this.intervalIndex = 0;

        if (longInterval.count > 0 && origin > longInterval.v[0]) {
            long intervalStart = longInterval.flag[0] ? longInterval.v[0] : longInterval.v[0] + 1;
            this.origin = origin - (long)Math.ceil((double)(origin-intervalStart) / unit) * unit;
        }
    }

    public QueryDataSet groupBy()
            throws IOException, ProcessorException, PathErrorException {

        groupByResult.clear();
        int partitionBatchCount = 0;

        // all the interval has been calculated
        if (intervalIndex >= longInterval.count) {
            groupByResult.clear();
            return groupByResult;
        }

        long partitionStart = origin; // partition start time
        long partitionEnd = origin + unit - 1; // partition end time
        long intervalStart = longInterval.flag[intervalIndex] ? longInterval.v[intervalIndex] : longInterval.v[intervalIndex] + 1; // interval start time
        long intervalEnd = longInterval.flag[intervalIndex+1] ? longInterval.v[intervalIndex+1] : longInterval.v[intervalIndex+1] - 1; // interval end time

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

            while (true) {
                int aggregationOrdinal = 0;
                for (Pair<Path, AggregateFunction> pair : aggregations) {
                    if (duplicatedPaths.contains(aggregationOrdinal))
                        continue;
                    aggregationOrdinal++;

                    Path path = pair.left;
                    AggregateFunction aggregateFunction = pair.right;
                    String aggregationKey = aggregationKey(aggregateFunction, path);
                    DynamicOneColumnData data = queryPathResult.get(aggregationKey);
                    if (data == null || (data.curIdx >= data.timeLength && !data.hasReadAll)) {
                        data = readOneColumnWithoutFilter(path, data, null, aggregationOrdinal);
                        queryPathResult.put(aggregationKey, data);
                    }

                    while (true) {
                        aggregateFunction.calcGroupByAggregation(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                        if (data.timeLength == 0 || data.hasReadAll || data.curIdx < data.timeLength) {
                            break;
                        }
                        if (data.curIdx >= data.timeLength && data.timeLength != 0) {
                            data = readOneColumnWithoutFilter(path, data, null, aggregationOrdinal);
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
            partitionBatchCount += 1;
            if (partitionBatchCount >= partitionFetchSize) {
                origin = partitionStart;
                break;
            }
        }

        int cnt = 0;
        for (Pair<Path, AggregateFunction> pair : aggregations) {
            if (duplicatedPaths.contains(cnt))
                cnt++;
            Path path = pair.left;
            AggregateFunction aggregateFunction = pair.right;
            groupByResult.mapRet.put(aggregationKey(aggregateFunction, path), aggregateFunction.resultData);
        }

        //LOG.debug("current group by function with no filter is over.");
        return groupByResult;
    }

    private DynamicOneColumnData readOneColumnWithoutFilter(Path path, DynamicOneColumnData res, Integer readLock, int aggregationOrdinal)
            throws ProcessorException, IOException, PathErrorException {

        // this read process is batch read
        // every time the ```partitionFetchSize``` data size will be return

        String deltaObjectID = path.getDeltaObjectToString();
        String measurementID = path.getMeasurementToString();
        String recordReaderPrefix = ReadCachePrefix.addQueryPrefix(aggregationOrdinal);

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectID, measurementID,
                timeFilter, null, null, readLock, recordReaderPrefix);

        if (res == null) {

            // get overflow params merged with bufferwrite insert data
            List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(timeFilter, null, null,
                    res, recordReader.insertPageInMemory, recordReader.overflowInfo);

            DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
            DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
            DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
            SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    newTimeFilter, null, null, MManager.getInstance().getSeriesType(path.getFullPath()));
            res = recordReader.queryOneSeries(deltaObjectID, measurementID,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, null, res, queryFetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res.clearData();
            res = recordReader.queryOneSeries(deltaObjectID, measurementID,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, null, res, queryFetchSize);
        }

        return res;
    }
}
