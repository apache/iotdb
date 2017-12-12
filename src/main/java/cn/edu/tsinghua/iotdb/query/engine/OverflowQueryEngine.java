package cn.edu.tsinghua.iotdb.query.engine;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggreFuncFactory;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.engine.groupby.GroupByEngineNoFilter;
import cn.edu.tsinghua.iotdb.query.engine.groupby.GroupByEngineWithFilter;
import cn.edu.tsinghua.iotdb.query.management.ReadLockManager;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.tsinghua.tsfile.timeseries.read.query.BatchReadRecordGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class OverflowQueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverflowQueryEngine.class);

    /** the formNumber represents the ordinal of disjunctive normal form of each query part **/
    private int formNumber = -1;

    public OverflowQueryEngine() {
    }

    /**
     * <p>
     * Basic query method.
     * This method may be invoked many times due to the restriction of fetchSize.
     *
     * @param formNumber   a complex query will be taken out to some disjunctive normal forms in query process,
     *                     the formNumber represent the number of normal form.
     * @param paths        query paths
     * @param queryDataSet query data set to return
     * @param fetchSize    fetch size for batch read
     * @return basic QueryDataSet
     * @throws ProcessorException series resolve error
     * @throws IOException        TsFile read error
     */
    public QueryDataSet query(int formNumber, List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                              FilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize)
            throws ProcessorException, IOException, PathErrorException {
        this.formNumber = formNumber;
        if (queryDataSet != null) {
            queryDataSet.clear();
        }
        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return querySeriesWithoutFilter(paths, queryDataSet, fetchSize, null);
        } else if (valueFilter != null && valueFilter instanceof CrossSeriesFilterExpression) {
            return crossSeriesQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter, queryDataSet, fetchSize);
        } else {
            return querySeriesUsingFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter, queryDataSet, fetchSize, null);
        }
    }

    /**
     * <p>
     * Basic aggregation method,
     * both single aggregation method or multi aggregation method is implemented here.
     *
     * @param aggres           a list of aggregations and corresponding path
     * @param filterStructures see <code>FilterStructure</code>, a list of all conjunction form
     * @return result QueryDataSet
     * @throws ProcessorException series resolve error
     * @throws IOException        TsFile read error
     * @throws PathErrorException path resolve error
     */
    public QueryDataSet aggregate(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures)
            throws ProcessorException, IOException, PathErrorException {

        ThreadLocal<QueryDataSet> aggregateThreadLocal = ReadLockManager.getInstance().getAggregateThreadLocal();

        // the aggregation method will only be executed once
        if (aggregateThreadLocal.get() != null) {
            QueryDataSet ans = aggregateThreadLocal.get();
            ans.clear();
            aggregateThreadLocal.remove();
            return ans;
        }

        List<Pair<Path, AggregateFunction>> aggregations = new ArrayList<>();

        // to remove duplicate queries, such as select count(s0),count(s0).
        Set<String> duplicatedAggregations = new HashSet<>();
        for (Pair<Path, String> pair : aggres) {
            TSDataType dataType = MManager.getInstance().getSeriesType(pair.left.getFullPath());
            AggregateFunction aggregateFunction = AggreFuncFactory.getAggrFuncByName(pair.right, dataType);
            if (duplicatedAggregations.contains(EngineUtils.aggregationKey(aggregateFunction, pair.left))) {
                continue;
            }
            duplicatedAggregations.add(EngineUtils.aggregationKey(aggregateFunction, pair.left));
            aggregations.add(new Pair<>(pair.left, aggregateFunction));
        }

        AggregateEngine.multiAggregate(aggregations, filterStructures);
        QueryDataSet ansQueryDataSet = new QueryDataSet();
        for (Pair<Path, AggregateFunction> pair : aggregations) {
            AggregateFunction aggregateFunction = pair.right;
            if (aggregateFunction.resultData.timeLength == 0) {
                aggregateFunction.putDefaultValue();
            }
            ansQueryDataSet.mapRet.put(EngineUtils.aggregationKey(aggregateFunction, pair.left), aggregateFunction.resultData);
        }
        aggregateThreadLocal.set(ansQueryDataSet);
        return ansQueryDataSet;
    }

    /**
     * Group by function implementation.
     *
     * @param aggres aggregation path and corresponding function
     * @param filterStructures all filters in where clause
     * @param unit
     * @param origin
     * @param intervals
     * @param fetchSize
     * @return QueryDataSet
     * @throws ProcessorException
     * @throws PathErrorException
     * @throws IOException
     */
    public QueryDataSet groupBy(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures,
                                long unit, long origin, List<Pair<Long, Long>> intervals, int fetchSize) {

        ThreadLocal<Integer> groupByCalcTime = ReadLockManager.getInstance().getGroupByCalcTime();
        ThreadLocal<GroupByEngineNoFilter>  groupByEngineNoFilterLocal = ReadLockManager.getInstance().getGroupByEngineNoFilterLocal();
        ThreadLocal<GroupByEngineWithFilter> groupByEngineWithFilterLocal = ReadLockManager.getInstance().getGroupByEngineWithFilterLocal();

        if (groupByCalcTime.get() == null) {

            LOGGER.info("calculate aggregations the 1 time");
            groupByCalcTime.set(2);

            SingleSeriesFilterExpression intervalFilter = null;
            for (Pair<Long, Long> pair : intervals) {
                if (intervalFilter == null) {
                    SingleSeriesFilterExpression left = FilterFactory.gtEq(FilterFactory.timeFilterSeries(), pair.left, true);
                    SingleSeriesFilterExpression right = FilterFactory.ltEq(FilterFactory.timeFilterSeries(), pair.right, true);
                    intervalFilter = (And) FilterFactory.and(left, right);
                } else {
                    SingleSeriesFilterExpression left = FilterFactory.gtEq(FilterFactory.timeFilterSeries(), pair.left, true);
                    SingleSeriesFilterExpression right = FilterFactory.ltEq(FilterFactory.timeFilterSeries(), pair.right, true);
                    intervalFilter = (Or) FilterFactory.or(intervalFilter, FilterFactory.and(left, right));
                }
            }

            List<Pair<Path, AggregateFunction>> aggregations = new ArrayList<>();
            try {
                for (Pair<Path, String> pair : aggres) {
                    TSDataType dataType = MManager.getInstance().getSeriesType(pair.left.getFullPath());
                    AggregateFunction func = AggreFuncFactory.getAggrFuncByName(pair.right, dataType);
                    aggregations.add(new Pair<>(pair.left, func));
                }

                if (noFilterOrOnlyHasTimeFilter(filterStructures)) {
                    SingleSeriesFilterExpression timeFilter = null;
                    if (filterStructures != null && filterStructures.size() == 1 && filterStructures.get(0).onlyHasTimeFilter()) {
                        timeFilter = filterStructures.get(0).getTimeFilter();
                    }
                    GroupByEngineNoFilter groupByEngineNoFilter = new GroupByEngineNoFilter(aggregations, timeFilter, origin, unit, intervalFilter, fetchSize);
                    groupByEngineNoFilterLocal.set(groupByEngineNoFilter);
                    return groupByEngineNoFilter.groupBy();
                }  else {
                    GroupByEngineWithFilter groupByEngineWithFilter = new GroupByEngineWithFilter(aggregations, filterStructures, origin, unit, intervalFilter, fetchSize);
                    groupByEngineWithFilterLocal.set(groupByEngineWithFilter);
                    return groupByEngineWithFilter.groupBy();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {

            LOGGER.info(String.format("calculate group by result function the %s time", String.valueOf(groupByCalcTime.get())));

            groupByCalcTime.set(groupByCalcTime.get() + 1);
            try {
                if (noFilterOrOnlyHasTimeFilter(filterStructures)) {
                    QueryDataSet ans = groupByEngineNoFilterLocal.get().groupBy();
                    if (!ans.hasNextRecord()) {
                        groupByCalcTime.remove();
                        groupByEngineNoFilterLocal.remove();
                        groupByEngineWithFilterLocal.remove();
                        LOGGER.debug("group by function without filter has no result");
                    }
                    return ans;
                } else {
                    QueryDataSet ans = groupByEngineWithFilterLocal.get().groupBy();
                    if (!ans.hasNextRecord()) {
                        groupByCalcTime.remove();
                        groupByEngineNoFilterLocal.remove();
                        groupByEngineWithFilterLocal.remove();
                        LOGGER.debug("group by function with filter has no result");
                    }
                    return ans;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * Query type 1: query without filter.
     */
    private QueryDataSet querySeriesWithoutFilter(List<Path> paths, QueryDataSet queryDataSet, int fetchSize, Integer readLock)
            throws ProcessorException, IOException {
        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize) {
                @Override
                public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
                    try {
                        return queryOneSeriesWithoutFilter(p, res, fetchSize, readLock);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
            queryDataSet.setBatchReadGenerator(batchReaderRetGenerator);
        }

        queryDataSet.clear();
        queryDataSet.getBatchReadGenerator().calculateRecord();
        EngineUtils.putRecordFromBatchReadGenerator(queryDataSet);
        return queryDataSet;
    }

    private DynamicOneColumnData queryOneSeriesWithoutFilter(Path path, DynamicOneColumnData res, int fetchSize, Integer readLock)
            throws ProcessorException, IOException, PathErrorException {

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
                    newTimeFilter, null, null, getDataTypeByPath(path));
            res = recordReader.queryOneSeries(deltaObjectID, measurementID,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, null, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.queryOneSeries(deltaObjectID, measurementID,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, null, res, fetchSize);
        }

        return res;
    }

    /**
     * Query type 2: query with filter.
     */
    private QueryDataSet querySeriesUsingFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                QueryDataSet queryDataSet, int fetchSize,
                                                Integer readLock) throws ProcessorException, IOException {
        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize) {
                @Override
                public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
                    try {
                        return queryOneSeriesUsingFilter(p, timeFilter, freqFilter, valueFilter, res, fetchSize, readLock);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
            queryDataSet.setBatchReadGenerator(batchReaderRetGenerator);
        }

        queryDataSet.clear();
        queryDataSet.getBatchReadGenerator().calculateRecord();
        EngineUtils.putRecordFromBatchReadGenerator(queryDataSet);

        return queryDataSet;
    }

    private DynamicOneColumnData queryOneSeriesUsingFilter(Path path, SingleSeriesFilterExpression timeFilter,
                                                           SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                           DynamicOneColumnData res, int fetchSize, Integer readLock)
            throws ProcessorException, IOException, PathErrorException {

        String deltaObjectId = path.getDeltaObjectToString();
        String measurementId = path.getMeasurementToString();
        String recordReaderPrefix = ReadCachePrefix.addQueryPrefix(formNumber);

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectId, measurementId,
                timeFilter, freqFilter, valueFilter, readLock, recordReaderPrefix);

        if (res == null) {

            // get overflow params merged with bufferwrite insert data
            List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(timeFilter, freqFilter, valueFilter,
                    res, recordReader.insertPageInMemory, recordReader.overflowInfo);

            DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
            DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
            DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
            SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    newTimeFilter, valueFilter, null, getDataTypeByPath(path));
            res = recordReader.queryOneSeries(deltaObjectId, measurementId,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, valueFilter, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.queryOneSeries(deltaObjectId, measurementId,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, valueFilter, res, fetchSize);
        }

        return res;
    }

    /**
     * Query type 3: cross series read.
     */
    private QueryDataSet crossSeriesQuery(List<Path> paths,
                                          SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                          CrossSeriesFilterExpression valueFilter,
                                          QueryDataSet queryDataSet, int fetchSize)
            throws ProcessorException, IOException, PathErrorException {

        if (queryDataSet != null) {
            queryDataSet.clear();
        }

        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            queryDataSet.crossQueryTimeGenerator = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, fetchSize) {
                @Override
                public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                               SingleSeriesFilterExpression valueFilter, int valueFilterNumber)
                        throws ProcessorException, IOException {

                    try {
                        return querySeriesForCross(valueFilter, freqFilter, res, fetchSize, valueFilterNumber);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
        }

        // calculate common timestamps
        long[] timestamps = queryDataSet.crossQueryTimeGenerator.generateTimes();
        LOGGER.debug("calculate common timestamps complete.");

        QueryDataSet ret = queryDataSet;
        for (Path path : paths) {

            String deltaObjectId = path.getDeltaObjectToString();
            String measurementId = path.getMeasurementToString();
            String recordReaderPrefix = ReadCachePrefix.addQueryPrefix(formNumber);
            String queryKey = String.format("%s.%s", deltaObjectId, measurementId);

            RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectId, measurementId,
                    null, null, null, null, recordReaderPrefix);

            // valueFilter is null, determine the common timeRet used valueFilter firstly.
            if (recordReader.insertAllData == null) {

                // get overflow params merged with bufferwrite insert data
                List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null,
                        null, recordReader.insertPageInMemory, recordReader.overflowInfo);
                DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
                DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
                DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
                SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

                recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                        insertTrue, updateTrue, updateFalse,
                        newTimeFilter, null, freqFilter, getDataTypeByPath(path));

                DynamicOneColumnData queryResult = recordReader.queryUsingTimestamps(
                        deltaObjectId, measurementId, newTimeFilter, timestamps, recordReader.insertAllData);
                queryResult.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
                ret.mapRet.put(queryKey, queryResult);
            } else {
                DynamicOneColumnData queryAnswer = recordReader.queryUsingTimestamps(
                        deltaObjectId, measurementId, recordReader.insertAllData.timeFilter, timestamps, recordReader.insertAllData);
                ret.mapRet.put(queryKey, queryAnswer);
            }
        }

        return ret;
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
    public DynamicOneColumnData querySeriesForCross(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter,
                                                    DynamicOneColumnData res, int fetchSize, int valueFilterNumber)
            throws ProcessorException, IOException, PathErrorException {

        // form.V.valueFilterNumber.deltaObjectId.measurementId
        String deltaObjectUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getDeltaObjectUID();
        String measurementUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getMeasurementUID();
        String valueFilterPrefix = ReadCachePrefix.addFilterPrefix(ReadCachePrefix.addFilterPrefix(valueFilterNumber), formNumber);

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

    private TSDataType getDataTypeByPath(Path path) throws PathErrorException {
        return MManager.getInstance().getSeriesType(path.getFullPath());
    }

}