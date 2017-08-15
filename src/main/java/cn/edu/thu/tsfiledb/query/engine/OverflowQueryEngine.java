package cn.edu.thu.tsfiledb.query.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.SingleBinaryExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.SingleUnaryExpression;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.BatchReadRecordGenerator;
import cn.edu.thu.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggreFuncFactory;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;


public class OverflowQueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverflowQueryEngine.class);
    private MManager mManager;

    public OverflowQueryEngine() {
        mManager = MManager.getInstance();
    }

    private static void clearQueryDataSet(QueryDataSet queryDataSet) {
        if (queryDataSet != null) {
            queryDataSet.clear();
        }
    }

    private TSDataType getDataTypeByPath(Path path) throws PathErrorException {
        return mManager.getSeriesType(path.getFullPath());
    }

    /**
     * Basic query function.
     *
     * @param formNumber a complex query will be taken out to some disjunctive normal forms in query process,
     *                   the formNumber represent the number of normal form.
     * @param paths query paths
     * @param queryDataSet query data set to return
     * @param fetchSize fetch size for batch read
     * @return
     * @throws ProcessorException
     * @throws IOException
     */
    public QueryDataSet query(int formNumber, List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                              FilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException, PathErrorException {
        System.out.println("======== formNumber: " + formNumber + " =====================");
        System.out.println("Query Paths: ");
        for (Path path : paths) {
            System.out.println(path);
        }
        System.out.println("TimeFilter: " + timeFilter);
        System.out.println("ValueFilter: " + valueFilter);
        System.out.println("=====================");
        clearQueryDataSet(queryDataSet);
        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return readWithoutFilter(formNumber, paths, queryDataSet, fetchSize, null);
        } else if (valueFilter != null && valueFilter instanceof CrossSeriesFilterExpression) {
            return crossColumnQuery(formNumber, paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter, queryDataSet, fetchSize);
        } else {
            return readOneColumnUseFilter(formNumber, paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter, queryDataSet, fetchSize, null);
        }
    }

    /**
     * Basic aggregate function.
     *
     * @param path aggregate paths
     * @param aggreFuncName aggregate function name
     * @return QueryDataSet for aggregation
     * @throws ProcessorException
     * @throws IOException
     */
    public QueryDataSet aggregate(Path path, String aggreFuncName
            , FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws ProcessorException, IOException, PathErrorException {
        TSDataType dataType= MManager.getInstance().getSeriesType(path.getFullPath());
        AggregateFunction func = AggreFuncFactory.getAggrFuncByName(aggreFuncName, dataType);
        RecordReaderFactory.getInstance().removeRecordReader(path.getDeltaObjectToString(), path.getMeasurementToString());
        return AggregateEngine.aggregate(path, func, timeFilter, freqFilter, valueFilter);
    }

    /**
     * Query type 1: read without filter.
     */
    private QueryDataSet readWithoutFilter(int formNumber, List<Path> paths, QueryDataSet queryDataSet, int fetchSize, Integer readLock) throws ProcessorException, IOException {
        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize) {
                @Override
                public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
                    try {
                        return readOneColumnWithoutFilter(formNumber, p, res, fetchSize, readLock);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
            queryDataSet.setBatchReaderRetGenerator(batchReaderRetGenerator);
        }
        clearQueryDataSet(queryDataSet);
        queryDataSet.getBatchReaderRetGenerator().calculateRecord();
        queryDataSet.putRecordFromBatchReadRetGenerator();
//        for (Path path : paths) {
//            RecordReaderFactory.getInstance().removeRecordReader(path.getDeltaObjectToString(), path.getMeasurementToString());
//        }
        return queryDataSet;
    }

    private DynamicOneColumnData readOneColumnWithoutFilter(int formNumber, Path path, DynamicOneColumnData res, int fetchSize, Integer readLock) throws ProcessorException, IOException, PathErrorException {

        String deltaObjectID = path.getDeltaObjectToString();
        String measurementID = path.getMeasurementToString();
        String deltaKey = "Q" + formNumber + "." + deltaObjectID;

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectID, measurementID,
                null, null, null, readLock, deltaKey);

        if (res == null) {
            // get four overflow params
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
    private QueryDataSet readOneColumnUseFilter(int formNumber, List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize,
                                                Integer readLock) throws ProcessorException, IOException {
        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize) {
                @Override
                public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
                    try {
                        return readOneColumnUseFilter(formNumber, p, timeFilter, freqFilter, valueFilter, res, fetchSize, readLock);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
            queryDataSet.setBatchReaderRetGenerator(batchReaderRetGenerator);
        }
        clearQueryDataSet(queryDataSet);
        queryDataSet.getBatchReaderRetGenerator().calculateRecord();
        queryDataSet.putRecordFromBatchReadRetGenerator();
//        for (Path path : paths) {
//            RecordReaderFactory.getInstance().removeRecordReader(path.getDeltaObjectToString(), path.getMeasurementToString());
//        }
        return queryDataSet;
    }

    private DynamicOneColumnData readOneColumnUseFilter(int formNumber, Path path, SingleSeriesFilterExpression timeFilter,
                                                        SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                        DynamicOneColumnData res, int fetchSize, Integer readLock) throws ProcessorException, IOException, PathErrorException {
        String deltaObjectId = path.getDeltaObjectToString();
        String measurementId = path.getMeasurementToString();
        String deltaKey = "Q" + formNumber + "." + deltaObjectId;

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectId, measurementId,
                timeFilter, freqFilter, valueFilter, readLock, deltaKey);

        if (res == null) {
            // get four overflow params
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
    private QueryDataSet crossColumnQuery(int formNumber, List<Path> paths,
                                          SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, CrossSeriesFilterExpression valueFilter,
                                          QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException, PathErrorException {
        clearQueryDataSet(queryDataSet);
        if (queryDataSet == null) {
            // reset status of RecordReader used ValueFilter
            resetRecordStatusUsingValueFilter(valueFilter, new HashSet<>());
            queryDataSet = new QueryDataSet();
            queryDataSet.timeQueryDataSet = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, fetchSize) {
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

        // calculate common timestamp
        long[] timeRet = queryDataSet.timeQueryDataSet.generateTimes();
        LOGGER.info("calculate common timestamps complete.");

        QueryDataSet ret = queryDataSet;
        for (Path path : paths) {

            String deltaObject = path.getDeltaObjectToString();
            String measurement = path.getMeasurementToString();
            String deltaKey = "Q" + formNumber + "." + deltaObject;
            String device_sensor = deltaObject + "." + measurement;

            RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObject, measurement,
                    null, null, null, null, deltaKey);

            // get overflow params merged with bufferwrite insert data
            List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null, null, recordReader.insertPageInMemory, recordReader.overflowInfo);
            DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
            DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
            DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
            SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);

            // valueFilter is null, determine the common timeRet used valueFilter firstly.
            if (recordReader.insertAllData == null) {
                recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                        insertTrue, updateTrue, updateFalse,
                        deleteFilter, null, freqFilter, getDataTypeByPath(path));
            } else {
                // reset the insertMemory read status
                recordReader.insertAllData.readStatusReset();
                recordReader.insertAllData.setCurrentPageBuffer(insertTrue);
            }

            DynamicOneColumnData oneColDataList = recordReader.getValuesUseTimeValueWithOverflow(deltaObject, measurement,
                    timeRet, updateTrue, recordReader.insertAllData, deleteFilter);
            ret.mapRet.put(device_sensor, oneColDataList);

            // recordReader.closeFromFactory();
        }
        for (Path path : paths) {
            RecordReaderFactory.getInstance().removeRecordReader(path.getDeltaObjectToString(), path.getMeasurementToString());
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
    private DynamicOneColumnData getDataUseSingleValueFilter(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter,
                                                             DynamicOneColumnData res, int fetchSize, int valueFilterNumber)
            throws ProcessorException, IOException, PathErrorException {

        // V.valueFilterNumber.deltaObjectId.measurementId
        String deltaObjectUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getDeltaObjectUID();
        String measurementUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getMeasurementUID();

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                null, freqFilter, valueFilter, null, "V" + valueFilterNumber + ".");

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

        // get overflow params merged with bufferwrite insert data
//        List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem( null, freqFilter, valueFilter, res, recordReader.insertPageInMemory, recordReader.overflowInfo);
//        DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
//        DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
//        DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
//        SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);
//
//        if (recordReader.insertAllData == null) {
//            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
//                    insertTrue, updateTrue, updateFalse,
//                    newTimeFilter, valueFilter, freqFilter, mManager.getSeriesType(deltaObjectUID+"."+measurementUID));
//        } else {
//            recordReader.insertAllData.setBufferWritePageList(recordReader.bufferWritePageList);
//            recordReader.insertAllData.setCurrentPageBuffer(insertTrue);
//        }
//
//        res = recordReader.getValueWithFilterAndOverflow(deltaObjectUID, measurementUID, updateTrue, updateFalse, recordReader.insertAllData,
//                newTimeFilter, freqFilter, valueFilter, res, fetchSize);
//        res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);

        return res;
    }

    /**
     * In cross column query, such as "select s0,s1,s2 from root.vehicle.d0 where time < 106 and (s0 >= 60 or s1 <= 200),"
     * when calculate the common time, the position in InsertDynamicData may be wrong.
     *
     * @param filter
     * @param hashSet
     */
    private void resetRecordStatusUsingValueFilter(FilterExpression filter, HashSet<String> hashSet) throws ProcessorException {
        if (filter instanceof SingleSeriesFilterExpression) {
            if (filter instanceof SingleUnaryExpression) {
                FilterSeries series = ((SingleUnaryExpression) filter).getFilterSeries();
                String key = series.getDeltaObjectUID() + "." + series.getMeasurementUID();
                if (!hashSet.contains(key)) {
                    RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(series.getDeltaObjectUID(), series.getMeasurementUID(),
                            null, null, null, null, "");
                    if (recordReader.insertAllData != null) {
                        recordReader.insertAllData.readStatusReset();
                    }
                    hashSet.add(key);
                }
            } else if (filter instanceof SingleBinaryExpression) {
                resetRecordStatusUsingValueFilter(((SingleBinaryExpression) filter).getLeft(), hashSet);
                resetRecordStatusUsingValueFilter(((SingleBinaryExpression) filter).getRight(), hashSet);
            }
        } else if (filter instanceof CrossSeriesFilterExpression) {
            resetRecordStatusUsingValueFilter(((CrossSeriesFilterExpression) filter).getLeft(), hashSet);
            resetRecordStatusUsingValueFilter(((CrossSeriesFilterExpression) filter).getRight(), hashSet);
        }
    }
}