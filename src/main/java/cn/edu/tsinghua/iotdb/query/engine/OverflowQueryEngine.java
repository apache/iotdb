package cn.edu.tsinghua.iotdb.query.engine;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggreFuncFactory;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.engine.groupby.GroupByEngine;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.BatchReadRecordGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class OverflowQueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverflowQueryEngine.class);
    private MManager mManager;
    private int formNumber = -1;
    /**
     * this variable is represent that whether it is
     * the second execution of aggregate method.
     */
    private static ThreadLocal<Boolean> threadLocal = new ThreadLocal<>();

    public OverflowQueryEngine() {
        mManager = MManager.getInstance();
    }

    private TSDataType getDataTypeByPath(Path path) throws PathErrorException {
        return mManager.getSeriesType(path.getFullPath());
    }

    /**
     * <p>
     * Basic query method.
     *
     * @param formNumber a complex query will be taken out to some disjunctive normal forms in query process,
     *                   the formNumber represent the number of normal form.
     * @param paths query paths
     * @param queryDataSet query data set to return
     * @param fetchSize fetch size for batch read
     * @return basic QueryDataSet
     * @throws ProcessorException series resolve error
     * @throws IOException TsFile read error
     */
    public QueryDataSet query(int formNumber, List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                              FilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException, PathErrorException {
        this.formNumber = formNumber;
        LOGGER.info("\r\nFormNumber: " + formNumber + ", TimeFilter: " + timeFilter + "; ValueFilter: " + valueFilter + "\r\nQuery Paths: " + paths.toString());
        if (queryDataSet != null) {
            queryDataSet.clear();
        }
        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return readWithoutFilter(paths, queryDataSet, fetchSize, null);
        } else if (valueFilter != null && valueFilter instanceof CrossSeriesFilterExpression) {
            return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter, queryDataSet, fetchSize);
        } else {
            return readOneColumnUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter, queryDataSet, fetchSize, null);
        }
    }

    /**
     * <p>
     * Basic aggregation method,
     * both single aggregation method or multi aggregation method is implemented here.
     *
     * @param aggres a list of aggregations and corresponding path
     * @param filterStructures see <code>FilterStructure</code>, a list of all conjunction form
     * @return result QueryDataSet
     * @throws ProcessorException series resolve error
     * @throws IOException TsFile read error
     * @throws PathErrorException path resolve error
     */
    public QueryDataSet aggregate(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures)
            throws ProcessorException, IOException, PathErrorException {
        LOGGER.info("Aggregation content: {}", aggres.toString());
        List<Pair<Path, AggregateFunction>> aggregations = new ArrayList<>();
        for (Pair<Path, String> pair : aggres) {
            TSDataType dataType= MManager.getInstance().getSeriesType(pair.left.getFullPath());
            AggregateFunction func = AggreFuncFactory.getAggrFuncByName(pair.right, dataType);
            aggregations.add(new Pair<>(pair.left, func));
        }

        if (threadLocal.get() != null && threadLocal.get()) {
            threadLocal.remove();
            return new QueryDataSet();
        }
        threadLocal.set(true);
        return AggregateEngine.multiAggregate(aggregations, filterStructures);
    }

    ThreadLocal<Boolean> testFloag = new ThreadLocal<>();
    public QueryDataSet groupBy(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures,
                                long unit, long origin, List<Pair<Long, Long>> intervals, int fetchSize)
            throws ProcessorException, PathErrorException, IOException {
        if (testFloag.get() != null && testFloag.get() == true) {
            return new QueryDataSet();
        }
        testFloag.set(true);
//        QueryDataSet testQueryDataSet = new QueryDataSet();
//        DynamicOneColumnData data1 = new DynamicOneColumnData(TSDataType.INT32, true, true);
//        for (int i = 1;i <= 10;i ++) {
//            if (i % 2 == 0) {
//                data1.putEmptyTime(i);
//            } else {
//                data1.putTime(i);
//                data1.putInt(i*2);
//            }
//        }
//
//        DynamicOneColumnData data2 = new DynamicOneColumnData(TSDataType.FLOAT, true, false);
//        for (int i = 5;i <= 20;i ++) {
//            data2.putTime(i);
//            data2.putFloat(i * 3);
//        }
//
//        testQueryDataSet.mapRet.put("count(root.vehicle.d0.s0)", data1);
//        testQueryDataSet.mapRet.put("count(root.vehicle.d0.s1)", data2);
//
//        testFloag.set(true);
//        return testQueryDataSet;

        GroupByEngine groupByEngine = new GroupByEngine();
        return null;

//        return groupByEngine.groupBy(aggres, filterStructures, unit, origin, intervals, fetchSize);
    }

    /**
     * Query type 1: read without filter.
     */
    private QueryDataSet readWithoutFilter(List<Path> paths, QueryDataSet queryDataSet, int fetchSize, Integer readLock) throws ProcessorException, IOException {
        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize) {
                @Override
                public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
                    try {
                        return readOneColumnWithoutFilter(p, res, fetchSize, readLock);
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
        // remove RecordReader cache of paths here is not collect, because of batch read,
        // must store the position offset status in RecordReader.
        return queryDataSet;
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
                    newTimeFilter, null, null, getDataTypeByPath(path));
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectID, measurementID,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, null, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectID, measurementID,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, null, res, fetchSize);
        }

        // close current recordReader
        // recordReader.closeFromFactory();
        return res;
    }

    /**
     * Query type 2: read one series with filter.
     */
    private QueryDataSet readOneColumnUseFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize,
                                                Integer readLock) throws ProcessorException, IOException {
        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize) {
                @Override
                public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
                    try {
                        return readOneColumnUseFilter(p, timeFilter, freqFilter, valueFilter, res, fetchSize, readLock);
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

    private DynamicOneColumnData readOneColumnUseFilter(Path path, SingleSeriesFilterExpression timeFilter,
                                                        SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                        DynamicOneColumnData res, int fetchSize, Integer readLock) throws ProcessorException, IOException, PathErrorException {
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
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectId, measurementId,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, valueFilter, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectId, measurementId,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, valueFilter, res, fetchSize);
        }

        return res;
    }

    /**
     * Query type 3: cross series read.
     */
    private QueryDataSet crossColumnQuery(List<Path> paths,
                                          SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, CrossSeriesFilterExpression valueFilter,
                                          QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException, PathErrorException {
        if (queryDataSet != null) {
            queryDataSet.clear();
        }
        if (queryDataSet == null) {
            // reset status of RecordReader used ValueFilter
            // resetRecordStatusUsingValueFilter(valueFilter, new HashSet<>());
            queryDataSet = new QueryDataSet();
            queryDataSet.crossQueryTimeGenerator = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, fetchSize) {
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
                List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null, null, recordReader.insertPageInMemory, recordReader.overflowInfo);
                DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
                DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
                DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
                SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

                recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                        insertTrue, updateTrue, updateFalse,
                        newTimeFilter, null, freqFilter, getDataTypeByPath(path));
                DynamicOneColumnData queryResult = recordReader.getValuesUseTimestampsWithOverflow(deltaObjectId, measurementId,
                        timestamps, updateTrue, recordReader.insertAllData, newTimeFilter);
                queryResult.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
                ret.mapRet.put(queryKey, queryResult);
            } else {
                // reset the insertMemory read status
                // recordReader.insertAllData.readStatusReset();
                // recordReader.insertAllData.setCurrentPageBuffer(insertTrue);
                DynamicOneColumnData oneColDataList = ret.mapRet.get(queryKey);
                oneColDataList = recordReader.getValuesUseTimestampsWithOverflow(deltaObjectId, measurementId,
                        timestamps, oneColDataList.updateTrue, recordReader.insertAllData, oneColDataList.timeFilter);
                ret.mapRet.put(queryKey, oneColDataList);
            }

            // recordReader.closeFromFactory();
        }

        return ret;
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
     */
    public DynamicOneColumnData getDataUseSingleValueFilter(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter,
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
                    newTimeFilter, valueFilter, null, mManager.getSeriesType(deltaObjectUID+"."+measurementUID));
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