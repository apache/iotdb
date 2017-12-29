package cn.edu.tsinghua.iotdb.query.engine;

import java.io.IOException;
import java.util.*;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.*;

public class AggregateEngine {

    private static final Logger logger = LoggerFactory.getLogger(AggregateEngine.class);

    /** aggregation batch calculation size **/
    public static int aggregateFetchSize = 50000;

    /** cross read query fetch size **/
    public static int crossQueryFetchSize = 50000;

    /**
     * <p>Public invoking method of multiple aggregation.
     *
     * @param aggregations           aggregation pairs
     * @param filterStructures list of <code>FilterStructure</code>
     * @throws ProcessorException read or write lock error etc
     * @throws IOException        read TsFile error
     * @throws PathErrorException path resolving error
     */
    public static void multiAggregate(List<Pair<Path, AggregateFunction>> aggregations, List<FilterStructure> filterStructures)
            throws IOException, PathErrorException, ProcessorException {
        if (noFilterOrOnlyHasTimeFilter(filterStructures)) {
            if (filterStructures != null && filterStructures.size() == 1 && filterStructures.get(0).onlyHasTimeFilter()) {
                multiAggregateWithoutFilter(aggregations, filterStructures.get(0).getTimeFilter());
            } else {
                multiAggregateWithoutFilter(aggregations, null);
            }
        } else {
            multiAggregateWithFilter(aggregations, filterStructures);
        }
    }

    private static void multiAggregateWithFilter(List<Pair<Path, AggregateFunction>> aggregations, List<FilterStructure> filterStructures)
            throws IOException, PathErrorException, ProcessorException {

        // stores the query QueryDataSet of each FilterStructure in filterStructures
        List<QueryDataSet> fsDataSets = new ArrayList<>();

        // stores calculated common timestamps of each FilterStructure answer
        List<long[]> fsTimeList = new ArrayList<>();

        // stores used index of each fsTimeList
        List<Integer> fsTimeIndexList = new ArrayList<>();

        // represents whether this FilterStructure answer still has unread data
        List<Boolean> fsHasUnReadDataList = new ArrayList<>();

        // represents that whether the 'key' ordinal aggregation still has unread data
        Map<Integer, Boolean> aggregationHasUnReadDataMap = new HashMap<>();

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
            long[] curCommonTimestamps = queryDataSet.crossQueryTimeGenerator.generateTimes();
            fsTimeList.add(curCommonTimestamps);
            fsTimeIndexList.add(0);
            if (curCommonTimestamps.length > 0) {
                fsHasUnReadDataList.add(true);
            } else {
                fsHasUnReadDataList.add(false);
            }
        }

        // the aggregate timestamps calculated by filterStructures
        List<Long> aggregateTimestamps = new ArrayList<>();
        PriorityQueue<Long> priorityQueue = new PriorityQueue<>();

        for (int i = 0; i < fsTimeList.size(); i++) {
            boolean flag = fsHasUnReadDataList.get(i);
            if (flag) {
                priorityQueue.add(fsTimeList.get(i)[fsTimeIndexList.get(i)]);
            }
        }

        // if there still has any uncompleted read data, hasAnyUnReadDataFlag is true
        boolean hasAnyUnReadDataFlag = true;

        while (true) {
            while (aggregateTimestamps.size() < aggregateFetchSize && !priorityQueue.isEmpty() && hasAnyUnReadDataFlag) {
                // add the minimum timestamp and remove others in timeArray
                long minTime = priorityQueue.poll();
                aggregateTimestamps.add(minTime);
                while (!priorityQueue.isEmpty() && minTime == priorityQueue.peek())
                    priorityQueue.poll();

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
                            priorityQueue.add(curTimestamps[curTimeIdx]);
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

            logger.debug(String.format("common timestamps in multiple aggregation process, timestamps size : %s, timestamps: %s",
                    String.valueOf(aggregateTimestamps.size()), aggregateTimestamps.toString()));

            if (aggregateTimestamps.size() == 0)
                break;

            //TODO optimize it using multi process

            hasAnyUnReadDataFlag = false;
            int aggregationPathOrdinal = 0;
            for (Pair<Path, AggregateFunction> pair : aggregations) {
                Path path = pair.left;
                AggregateFunction aggregateFunction = pair.right;
                String deltaObjectUID = path.getDeltaObjectToString();
                String measurementUID = path.getMeasurementToString();
                TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
                aggregationPathOrdinal++;

                // current aggregation has no un read data
                if (aggregationHasUnReadDataMap.containsKey(aggregationPathOrdinal) && !aggregationHasUnReadDataMap.get(aggregationPathOrdinal)) {
                    continue;
                }

                // the query prefix here must not be confilct with method querySeriesForCross()
                RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                        null, null, null, null,
                        ReadCachePrefix.addQueryPrefix("AggQuery", aggregationPathOrdinal));

                if (recordReader.insertAllData == null) {

                    // TODO the parameter there could be optimized using aggregateTimestamps
                    List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null, null,
                            recordReader.insertPageInMemory, recordReader.overflowInfo);
                    DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
                    DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
                    DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
                    SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);
                    recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                            insertTrue, updateTrue, updateFalse,
                            newTimeFilter, null, null, dataType);

                    Pair<AggregateFunction, Boolean> aggrPair = recordReader.aggregateUsingTimestamps(deltaObjectUID, measurementUID, aggregateFunction,
                            recordReader.insertAllData.updateTrue, recordReader.insertAllData.updateFalse, recordReader.insertAllData,
                            newTimeFilter, null, aggregateTimestamps);

                    boolean hasUnReadDataFlag = aggrPair.right;
                    aggregationHasUnReadDataMap.put(aggregationPathOrdinal, hasUnReadDataFlag);
                    if (hasUnReadDataFlag) {
                        hasAnyUnReadDataFlag = true;
                    }

                } else {
                    Pair<AggregateFunction, Boolean> aggrPair = recordReader.aggregateUsingTimestamps(deltaObjectUID, measurementUID, aggregateFunction,
                            recordReader.insertAllData.updateTrue, recordReader.insertAllData.updateFalse, recordReader.insertAllData,
                            recordReader.insertAllData.timeFilter, null, aggregateTimestamps);
                    boolean hasUnReadDataFlag = aggrPair.right;
                    aggregationHasUnReadDataMap.put(aggregationPathOrdinal, hasUnReadDataFlag);
                    if (hasUnReadDataFlag) {
                        hasAnyUnReadDataFlag = true;
                    }
                }
            }

            // current batch timestamps has been used all
            aggregateTimestamps.clear();
        }
    }

    /**
     * Calculate the aggregation without filter is easy,
     * we don't need to consider the common aggregation timestamps.
     *
     * @param aggres
     * @return
     * @throws PathErrorException
     * @throws ProcessorException
     * @throws IOException
     */
    private static void multiAggregateWithoutFilter(List<Pair<Path, AggregateFunction>> aggres, SingleSeriesFilterExpression timeFilter)
            throws PathErrorException, ProcessorException, IOException {

        int aggreNumber = 0;
        for (Pair<Path, AggregateFunction> pair : aggres) {
            aggreNumber++;
            Path path = pair.left;
            AggregateFunction aggregateFunction = pair.right;
            String deltaObjectUID = path.getDeltaObjectToString();
            String measurementUID = path.getMeasurementToString();
            TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());

            RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                    timeFilter, null, null, null, ReadCachePrefix.addQueryPrefix(aggreNumber));

            if (recordReader.insertAllData == null) {
                // get overflow params merged with bufferwrite insert data
                List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(timeFilter, null, null, null,
                        recordReader.insertPageInMemory, recordReader.overflowInfo);
                DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
                DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
                DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
                SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

                recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                        insertTrue, updateTrue, updateFalse,
                        newTimeFilter, null, null, dataType);

                recordReader.aggregate(deltaObjectUID, measurementUID, aggregateFunction,
                        updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, null, null);

            } else {
                DynamicOneColumnData aggData = aggregateFunction.resultData;
                if (aggData != null) {
                    aggData.clearData();
                }
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

        // query prefix here must not be conflict with query in multiAggregate method
        String valueFilterPrefix = ReadCachePrefix.addFilterPrefix("AggFilterStructure", valueFilterNumber);

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                null, freqFilter, valueFilter, null, valueFilterPrefix);

        if (res == null) {
            // get four overflow params
            List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, freqFilter, valueFilter,
                    res, recordReader.insertPageInMemory, recordReader.overflowInfo);

            DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
            DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
            DynamicOneColumnData updateTrue2 = copy(updateTrue);
            DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
            DynamicOneColumnData updateFalse2 = copy(updateFalse);
            SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    newTimeFilter, valueFilter, null, MManager.getInstance().getSeriesType(deltaObjectUID + "." + measurementUID));

            res = recordReader.queryOneSeries(deltaObjectUID, measurementUID,
                    updateTrue2, updateFalse2, recordReader.insertAllData, newTimeFilter, valueFilter, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.queryOneSeries(deltaObjectUID, measurementUID,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, valueFilter, res, fetchSize);
        }

        return res;
    }


}
