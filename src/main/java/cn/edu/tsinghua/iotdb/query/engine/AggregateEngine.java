package cn.edu.tsinghua.iotdb.query.engine;


import java.io.IOException;
import java.util.*;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationResult;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateEngine {

    private static final Logger LOG = LoggerFactory.getLogger(AggregateEngine.class);

    /**
     * Public invoking method of multiple aggregation.
     *
     * @param aggres aggregation pairs
     * @param filterStructures list of <code>FilterStructure</code>
     *
     * @return QueryDataSet result of multi aggregation
     * @throws ProcessorException read or write lock error etc
     * @throws IOException read tsfile error
     * @throws PathErrorException path resolving error
     */
    public static QueryDataSet multiAggregate(List<Pair<Path, AggregateFunction>> aggres, List<FilterStructure> filterStructures)
            throws IOException, PathErrorException, ProcessorException {

        // LOG.info("multip");

        if (filterStructures == null || filterStructures.size() == 0 || (filterStructures.size()==1 && filterStructures.get(0).noFilter()) ) {
            return multiAggregateWithoutFilter(aggres, filterStructures);
        }

        QueryDataSet ansQueryDataSet = new QueryDataSet();
        List<QueryDataSet> filterQueryDataSets = new ArrayList<>(); // stores QueryDataSet of each FilterStructure answer
        List<long[]> timeArray = new ArrayList<>(); // stores calculated common timestamps of each FilterStructure answer
        List<Integer> indexArray = new ArrayList<>(); // stores used index of each timeArray
        List<Boolean> hasDataArray = new ArrayList<>(); // represents whether this FilterStructure answer still has unread data
        for (int idx = 0;idx < filterStructures.size();idx++) {
            FilterStructure filterStructure = filterStructures.get(idx);
            QueryDataSet queryDataSet = new QueryDataSet();
            queryDataSet.timeQueryDataSet = new CrossQueryTimeGenerator(filterStructure.getTimeFilter(), filterStructure.getFrequencyFilter(), filterStructure.getValueFilter(), 10000) {
                @Override
                public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                               SingleSeriesFilterExpression valueFilter, int valueFilterNumber) throws ProcessorException, IOException {
                    try {
                        return getDataUseSingleValueFilter(valueFilter, freqFilter, res, fetchSize, valueFilterNumber);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
            filterQueryDataSets.add(queryDataSet);
            long[] curTimestamps = ansQueryDataSet.timeQueryDataSet.generateTimes();
            timeArray.add(curTimestamps);
            indexArray.add(0);
            if (curTimestamps.length > 0) {
                hasDataArray.add(true);
            } else {
                hasDataArray.add(false);
            }
        }

        // the aggregate timestamps calculated by all dnf
        List<Long> timestamps = new ArrayList<>();
        PriorityQueue<Long> priorityQueue = new PriorityQueue<>();

        for (int i = 0;i < timeArray.size();i++) {
            boolean flag = hasDataArray.get(i);
            if (flag) {
                priorityQueue.add(timeArray.get(i)[indexArray.get(i)]);
            }
        }

        int batchSize = 50000;
        while (true) {


            while (timestamps.size() < batchSize && !priorityQueue.isEmpty()) {

                // add the minimum timestamp and remove others in timeArray
                long minTime = priorityQueue.poll();
                timestamps.add(minTime);
                while (!priorityQueue.isEmpty() && minTime == priorityQueue.peek())
                    priorityQueue.poll();

                for (int i = 0;i < timeArray.size();i++) {
                    boolean flag = hasDataArray.get(i);
                    if (flag) {
                        int curTimeIdx = indexArray.get(i);
                        long[] curTimestamps = timeArray.get(i);
                        // remove all timestamps equal to min time in all series
                        while (curTimeIdx < curTimestamps.length && curTimestamps[curTimeIdx] == minTime) {
                            curTimeIdx++;
                        }
                        if (curTimeIdx < curTimestamps.length) {
                            indexArray.set(i, curTimeIdx);
                            priorityQueue.add(curTimestamps[curTimeIdx]);
                        } else {
                            long[] newTimeStamps = filterQueryDataSets.get(i).timeQueryDataSet.generateTimes();
                            if (newTimeStamps.length > 0) {
                                indexArray.set(i, 0);
                            } else {
                                hasDataArray.set(i, false);
                            }
                        }
                    }
                }
            }

            LOG.debug("common timestamps in multiple aggregation process : " + timestamps.toString());
            if (timestamps.size() == 0)
                break;

            //TODO optimize it using multi process

            Set<String> aggrePathSet = new HashSet<>();
            for (Pair<Path, AggregateFunction> pair : aggres) {
                Path path = pair.left;
                AggregateFunction aggregateFunction = pair.right;
                String deltaObjectUID = path.getDeltaObjectToString();
                String measurementUID = path.getMeasurementToString();
                TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
                String aggregationKey = aggregateFunction.name + "(" + path.getFullPath() + ")";
                if (aggrePathSet.contains(aggregationKey)) {
                    continue;
                } else {
                    aggrePathSet.add(aggregationKey);
                }

                RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                        null, null, null, null, "MultiAggre_Query");

                if (recordReader.insertAllData == null) {
                    // get overflow params merged with bufferwrite insert data
                    List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null, null, recordReader.insertPageInMemory, recordReader.overflowInfo);
                    DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
                    DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
                    DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
                    SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

                    recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                            insertTrue, updateTrue, updateFalse,
                            newTimeFilter, null, null, dataType);

                    AggregationResult aggrResult = recordReader.aggregateUsingTimestamps(deltaObjectUID, measurementUID, aggregateFunction,
                            recordReader.insertAllData.updateTrue, recordReader.insertAllData.updateFalse, recordReader.insertAllData,
                            newTimeFilter, null, null, timestamps, null);
                    ansQueryDataSet.mapRet.put(aggregationKey, aggrResult.data);
                } else {
                    DynamicOneColumnData aggreData = ansQueryDataSet.mapRet.get(aggregationKey);
                    AggregationResult aggrResult = recordReader.aggregateUsingTimestamps(deltaObjectUID, measurementUID, aggregateFunction,
                            recordReader.insertAllData.updateTrue, recordReader.insertAllData.updateFalse, recordReader.insertAllData,
                            recordReader.insertAllData.timeFilter, null, null, timestamps, aggreData);
                    ansQueryDataSet.mapRet.put(aggregationKey, aggrResult.data);
                }
            }
        }

        return ansQueryDataSet;
    }

    private static QueryDataSet multiAggregateWithoutFilter(List<Pair<Path, AggregateFunction>> aggres, List<FilterStructure> filterStructures) throws PathErrorException, ProcessorException, IOException {

        QueryDataSet ansQueryDataSet = new QueryDataSet();

        int aggreNumber = 0;
        for (Pair<Path, AggregateFunction> pair : aggres) {
            aggreNumber ++;
            Path path = pair.left;
            AggregateFunction aggregateFunction = pair.right;
            String deltaObjectUID = path.getDeltaObjectToString();
            String measurementUID = path.getMeasurementToString();
            TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
            String aggregationKey = aggregateFunction.name + "(" + path.getFullPath() + ")";
            if (ansQueryDataSet.mapRet.size() > 0 && ansQueryDataSet.mapRet.containsKey(aggregationKey)) {
                continue;
            }

            RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                    null, null, null, null, ReadCachePrefix.addQueryPrefix(aggreNumber));

            if (recordReader.insertAllData == null) {
                // get overflow params merged with bufferwrite insert data
                List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null, null, recordReader.insertPageInMemory, recordReader.overflowInfo);
                DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
                DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
                DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
                SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

                recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                        insertTrue, updateTrue, updateFalse,
                        newTimeFilter, null, null, dataType);

                AggregationResult aggrRet = recordReader.aggregate(deltaObjectUID, measurementUID, aggregateFunction,
                        updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, null, null);
                ansQueryDataSet.mapRet.put(aggregationKey, aggrRet.data);
            } else {
                DynamicOneColumnData aggData = ansQueryDataSet.mapRet.get(aggregationKey);
                if (aggData != null) {
                    aggData.clearData();
                }
                ansQueryDataSet.mapRet.put(aggregationKey, aggData);
            }
        }

        return ansQueryDataSet;
    }

    /**
     *  This function is only used for CrossQueryTimeGenerator.
     *  A CrossSeriesFilterExpression is consist of many SingleSeriesFilterExpression.
     *  e.g. CSAnd(d1.s1, d2.s1) is consist of d1.s1 and d2.s1, so this method would be invoked twice,
     *  once for querying d1.s1, once for querying d2.s1.
     *  <p>
     *  When this method is invoked, need add the filter index as a new parameter, for the reason of exist of
     *  <code>RecordReaderCache</code>, if the composition of CrossFilterExpression exist same SingleFilterExpression,
     *  we must guarantee that the <code>RecordReaderCache</code> doesn't cause conflict to the same SingleFilterExpression.
     *
     */
    private static DynamicOneColumnData getDataUseSingleValueFilter(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter,
                                                            DynamicOneColumnData res, int fetchSize, int valueFilterNumber)
            throws ProcessorException, IOException, PathErrorException {

        String deltaObjectUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getDeltaObjectUID();
        String measurementUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getMeasurementUID();
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
                    newTimeFilter, valueFilter, null, MManager.getInstance().getSeriesType(deltaObjectUID+"."+measurementUID));
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectUID, measurementUID,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, valueFilter, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectUID, measurementUID,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, valueFilter, res, fetchSize);
        }

        return res;
    }
}
