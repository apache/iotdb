package cn.edu.thu.tsfiledb.query.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.SingleBinaryExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.SingleUnaryExpression;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.BatchReadRecordGenerator;
import cn.edu.thu.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggreFuncFactory;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.aggregation.AggregationResult;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;


public class OverflowQueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverflowQueryEngine.class);
    //private RecordReaderFactory recordReaderFactory;
    private MManager mManager;

    public OverflowQueryEngine() {
        //recordReaderFactory = RecordReaderFactory.getInstance();
        mManager = MManager.getInstance();
    }

    private static void clearQueryDataSet(QueryDataSet queryDataSet) {
        if (queryDataSet != null) {
            queryDataSet.clear();
        }
    }

    public TSDataType getDataTypeByDeviceAndSensor(String device, String sensor) throws PathErrorException {
        String path = device + "." + sensor;
        return mManager.getSeriesType(path);
    }

    private TSDataType getDataTypeByPath(Path path) throws PathErrorException {
        return mManager.getSeriesType(path.getFullPath());
    }

    /**
     * Basic query function.
     *
     * @param paths query paths
     * @param queryDataSet query data set to return
     * @param fetchSize fetch size for batch read
     * @return
     * @throws ProcessorException
     * @throws IOException
     */
    public QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                              FilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException, PathErrorException {
        clearQueryDataSet(queryDataSet);
        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return readWithoutFilter(paths, queryDataSet, fetchSize);
        } else if (valueFilter != null && valueFilter instanceof CrossSeriesFilterExpression) {
            return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter, queryDataSet, fetchSize);
        } else {
            return readOneColumnUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter, queryDataSet, fetchSize);
        }
    }

    /**
     * Basic aggregate function.
     *
     * @param path aggregate paths
     * @param aggreFuncName aggregate function name
     * @return
     * @throws ProcessorException
     * @throws IOException
     */
    public QueryDataSet aggregate(Path path, String aggreFuncName
            , FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws ProcessorException, IOException, PathErrorException {
        TSDataType dataType;
        try {
            dataType = MManager.getInstance().getSeriesType(path.getFullPath());
        } catch (PathErrorException e) {
            throw new ProcessorException(e.getMessage());
        }
        AggregateFunction func = AggreFuncFactory.getAggrFuncByName(aggreFuncName, dataType);
        return aggregate(path, func, timeFilter, freqFilter, valueFilter);
    }

    private QueryDataSet aggregate(Path path, AggregateFunction func
            , FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws ProcessorException, IOException, PathErrorException {
        if (!(timeFilter == null || timeFilter instanceof SingleSeriesFilterExpression) ||
                !(freqFilter == null || freqFilter instanceof SingleSeriesFilterExpression) ||
                !(valueFilter == null || valueFilter instanceof SingleSeriesFilterExpression)) {
            throw new ProcessorException("Filter must be SingleSeriesFilterExpression");
        }

        QueryDataSet queryDataSet = new QueryDataSet();
        String deltaObjectUID = path.getDeltaObjectToString();
        String measurementUID = path.getMeasurementToString();

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                (SingleSeriesFilterExpression) timeFilter,
                (SingleSeriesFilterExpression) freqFilter,
                (SingleSeriesFilterExpression) valueFilter);

        // Get 4 params
        List<Object> params = getOverflowInfoAndFilterDataInMem((SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                (SingleSeriesFilterExpression) valueFilter, null, recordReader.insertPageInMemory, recordReader.overflowInfo);
        DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
        DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
        DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
        SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);

        if (recordReader.insertAllData == null) {
            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    deleteFilter, (SingleSeriesFilterExpression)valueFilter, (SingleSeriesFilterExpression)freqFilter, getDataTypeByPath(path));
        } else {
            recordReader.insertAllData.readStatusReset();
            recordReader.insertAllData.setBufferWritePageList(recordReader.bufferWritePageList);
            recordReader.insertAllData.setCurrentPageBuffer(insertTrue);
        }

        AggregationResult aggrRet = recordReader.aggregate(deltaObjectUID, measurementUID, func,
                updateTrue, updateFalse, recordReader.insertAllData
                , deleteFilter, (SingleSeriesFilterExpression) freqFilter, (SingleSeriesFilterExpression) valueFilter);

        queryDataSet.mapRet.put(func.name + "(" + path.getFullPath() + ")", aggrRet.data);
        //close current recordReader
        recordReader.closeFromFactory();
        return queryDataSet;
    }

    /**
     * Query type 1: read without filter.
     */
    private QueryDataSet readWithoutFilter(List<Path> paths, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize) {
                @Override
                public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
                    try {
                        return readOneColumnWithoutFilter(p, res, fetchSize);
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
        return queryDataSet;
    }

    private DynamicOneColumnData readOneColumnWithoutFilter(Path path, DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException, PathErrorException {

        String deltaObjectUID = path.getDeltaObjectToString();
        String measurementUID = path.getMeasurementToString();

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID, null, null, null);

        // Get 4 params
        List<Object> params = getOverflowInfoAndFilterDataInMem( null, null, null,
                res, recordReader.insertPageInMemory, recordReader.overflowInfo);

        DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
        DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
        DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
        SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);

        if (recordReader.insertAllData == null) {
            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    deleteFilter, null, null, getDataTypeByPath(path));
        } else {
            recordReader.insertAllData.setBufferWritePageList(recordReader.bufferWritePageList);
            recordReader.insertAllData.setCurrentPageBuffer(insertTrue);
        }
        //System.out.println("---------" + recordReader.insertAllData.insertTrue);
        //System.out.println("---------" + recordReader.insertAllData.pageList.get(0));
        res = recordReader.getValueInOneColumnWithOverflow(deltaObjectUID, measurementUID,
                updateTrue, updateFalse, recordReader.insertAllData, deleteFilter, res, fetchSize);

        res.putOverflowInfo(insertTrue, updateTrue, updateFalse, deleteFilter);

        //close current recordReader
        recordReader.closeFromFactory();

        return res;
    }

    /**
     * Query type 2: read one series with filter.
     */
    private QueryDataSet readOneColumnUseFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException {
        if (queryDataSet == null) {
            queryDataSet = new QueryDataSet();
            BatchReadRecordGenerator batchReaderRetGenerator = new BatchReadRecordGenerator(paths, fetchSize) {
                @Override
                public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws ProcessorException, IOException {
                    try {
                        return readOneColumnUseFilter(p, timeFilter, freqFilter, valueFilter, res, fetchSize);
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
        return queryDataSet;
    }

    private DynamicOneColumnData readOneColumnUseFilter(Path path, SingleSeriesFilterExpression timeFilter,
                                                        SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException, PathErrorException {
        String device = path.getDeltaObjectToString();
        String sensor = path.getMeasurementToString();

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(device, sensor, timeFilter, freqFilter, valueFilter);

        // Get 4 params
        List<Object> params = getOverflowInfoAndFilterDataInMem(timeFilter, freqFilter, valueFilter, res, recordReader.insertPageInMemory, recordReader.overflowInfo);
        DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
        DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
        DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
        SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);

        if (recordReader.insertAllData == null) {
            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    deleteFilter, valueFilter, freqFilter, getDataTypeByPath(path));
        } else {
            recordReader.insertAllData.setBufferWritePageList(recordReader.bufferWritePageList);
            recordReader.insertAllData.setCurrentPageBuffer(insertTrue);
        }

        DynamicOneColumnData oneColDataList = recordReader.getValueWithFilterAndOverflow(device, sensor, updateTrue,
                updateFalse, recordReader.insertAllData, deleteFilter, freqFilter, valueFilter, res, fetchSize);
        oneColDataList.putOverflowInfo(insertTrue, updateTrue, updateFalse, deleteFilter);
        recordReader.closeFromFactory();
        return oneColDataList;
    }

    /**
     * Query type 3: cross series read.
     */
    private QueryDataSet crossColumnQuery(List<Path> paths,
                                                 SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, CrossSeriesFilterExpression valueFilter,
                                                 QueryDataSet queryDataSet, int fetchSize) throws ProcessorException, IOException, PathErrorException {

        LOGGER.info("start cross columns getIndex...");
        clearQueryDataSet(queryDataSet);
        // Step 1: calculate common timestamp
        LOGGER.info("step 1: init time value generator...");
        if (queryDataSet == null) {
            // reset status of RecordReader used ValueFilter
            resetRecordStatusUsingValueFilter(valueFilter, new HashSet<String>());
            queryDataSet = new QueryDataSet();
            queryDataSet.timeQueryDataSet = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, fetchSize) {
                @Override
                public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                               SingleSeriesFilterExpression valueFilter) throws ProcessorException, IOException {
                    try {
                        return readOneColumnValueUseValueFilter(timeFilter, valueFilter, freqFilter, res, fetchSize);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
        }

        LOGGER.info("step 1 done.");
        LOGGER.info("step 2: calculate timeRet...");
        long[] timeRet = queryDataSet.timeQueryDataSet.generateTimes();
        LOGGER.info("step 2 done. timeRet size is: " + timeRet.length + ", FetchSize is: " + fetchSize);

        // Step 3: Get result using common timestamp
        LOGGER.info("step 3: Get result using common timestamp");

        QueryDataSet ret = queryDataSet;
        for (Path path : paths) {

            String deltaObject = path.getDeltaObjectToString();
            String measurement = path.getMeasurementToString();
            String device_sensor = deltaObject + "." + measurement;

            RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObject, measurement, null, null, null);

            // Get 4 params
            List<Object> params = getOverflowInfoAndFilterDataInMem(null, null, null, null, recordReader.insertPageInMemory, recordReader.overflowInfo);
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

            recordReader.closeFromFactory();
        }
        return ret;
    }

    /**
     *  This function is only used for CrossQueryTimeGenerator.
     */
    private DynamicOneColumnData readOneColumnValueUseValueFilter(SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                                                         SingleSeriesFilterExpression freqFilter, DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException, PathErrorException {

        String deltaObjectUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getDeltaObjectUID();
        String measurementUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getMeasurementUID();

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID, null, freqFilter, valueFilter);
        // Get 4 params
        List<Object> params = getOverflowInfoAndFilterDataInMem( null, freqFilter, valueFilter, res, recordReader.insertPageInMemory, recordReader.overflowInfo);
        DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
        DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
        DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
        SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);

        if (recordReader.insertAllData == null) {
            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    deleteFilter, valueFilter, freqFilter, mManager.getSeriesType(deltaObjectUID+"."+measurementUID));
        } else {
            recordReader.insertAllData.setBufferWritePageList(recordReader.bufferWritePageList);
            recordReader.insertAllData.setCurrentPageBuffer(insertTrue);
        }

        res = recordReader.getValueWithFilterAndOverflow(deltaObjectUID, measurementUID, updateTrue, updateFalse, recordReader.insertAllData,
                deleteFilter, freqFilter, valueFilter, res, fetchSize);
        res.putOverflowInfo(insertTrue, updateTrue, updateFalse, deleteFilter);

        recordReader.closeFromFactory();
        return res;
    }

    private static List<Object> getOverflowInfoAndFilterDataInMem(SingleSeriesFilterExpression timeFilter,
                                                                  SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter
            , DynamicOneColumnData res, DynamicOneColumnData insertDataInMemory, List<Object> overflowParams) throws ProcessorException {

        List<Object> paramList = new ArrayList<>();

        if (res == null) {
            // filter satisfied value from insertDataInMemory
            timeFilter = (SingleSeriesFilterExpression) overflowParams.get(3);

            DynamicOneColumnData updateTrue = (DynamicOneColumnData) overflowParams.get(1);
            insertDataInMemory = getSatisfiedData(updateTrue, timeFilter, freqFilter, valueFilter, insertDataInMemory);

            DynamicOneColumnData overflowInsertTrue = (DynamicOneColumnData) overflowParams.get(0);
            // add insert records from memory in BufferWriter stage
            if (overflowInsertTrue == null) {
                overflowInsertTrue = insertDataInMemory;
            } else {
                overflowInsertTrue = mergeOverflowAndMemory(overflowInsertTrue, insertDataInMemory);
            }
            paramList.add(overflowInsertTrue);
            paramList.add(overflowParams.get(1));
            paramList.add(overflowParams.get(2));
            paramList.add(overflowParams.get(3));
        } else {
            paramList.add(res.insertTrue);
            paramList.add(res.updateTrue);
            paramList.add(res.updateFalse);
            paramList.add(res.timeFilter);
        }

        return paramList;
    }

    /**
     * Merge insert data in overflow and buffer writer memory.<br>
     * Important: If there is two fields whose timestamp are equal, use the value
     * from overflow.
     *
     * @param overflowData data in overflow insert
     * @param memoryData data in buffer write insert
     * @return
     */
    private static DynamicOneColumnData mergeOverflowAndMemory(
            DynamicOneColumnData overflowData, DynamicOneColumnData memoryData) {
        if (overflowData == null && memoryData == null) {
            return null;
        } else if (overflowData != null && memoryData == null) {
            return overflowData;
        } else if (overflowData == null) {
            return memoryData;
        }

        DynamicOneColumnData res = new DynamicOneColumnData(overflowData.dataType, true);
        int overflowIdx = 0;
        int memoryIdx = 0;
        while (overflowIdx < overflowData.length || memoryIdx < memoryData.length) {
            while (overflowIdx < overflowData.length && (memoryIdx >= memoryData.length ||
                    memoryData.getTime(memoryIdx) >= overflowData.getTime(overflowIdx))) {
                res.putTime(overflowData.getTime(overflowIdx));
                res.putAValueFromDynamicOneColumnData(overflowData, overflowIdx);
                if (memoryIdx < memoryData.length && memoryData.getTime(memoryIdx) == overflowData.getTime(overflowIdx)) {
                    memoryIdx++;
                }
                overflowIdx++;
            }

            while (memoryIdx < memoryData.length && (overflowIdx >= overflowData.length ||
                    overflowData.getTime(overflowIdx) > memoryData.getTime(memoryIdx))) {
                res.putTime(memoryData.getTime(memoryIdx));
                res.putAValueFromDynamicOneColumnData(memoryData, memoryIdx);
                memoryIdx++;
            }
        }

        return res;
    }

    /**
     * Get satisfied values from a DynamicOneColumnData
     *
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @param oneColData
     * @return
     */
    private static DynamicOneColumnData getSatisfiedData(DynamicOneColumnData updateTrue, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter
            , SingleSeriesFilterExpression valueFilter, DynamicOneColumnData oneColData) {
        if (oneColData == null) {
            return null;
        }
        if (oneColData.length == 0) {
            return oneColData;
        }

        //update the value in oneColData according to updateTrue
        oneColData = updateValueAccordingToUpdateTrue(updateTrue, oneColData);
        DynamicOneColumnData res = new DynamicOneColumnData(oneColData.dataType, true);
        SingleValueVisitor<?> timeVisitor = null;
        if (timeFilter != null) {
            timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
        }
        SingleValueVisitor<?> valueVisitor = null;
        if (valueFilter != null) {
            valueVisitor = getSingleValueVisitorByDataType(oneColData.dataType, valueFilter);
        }

        switch (oneColData.dataType) {
            case BOOLEAN:
                for (int i = 0; i < oneColData.length; i++) {
                    boolean v = oneColData.getBoolean(i);
                    if ((valueFilter == null && timeFilter == null) ||
                            (valueFilter != null && timeFilter == null && valueVisitor.satisfyObject(v, valueFilter)) ||
                            (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
                            (valueFilter != null && timeFilter != null &&
                                    valueVisitor.satisfyObject(v, valueFilter) &&
                                    timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putBoolean(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case DOUBLE:
                for (int i = 0; i < oneColData.length; i++) {
                    double v = oneColData.getDouble(i);
                    if ((valueFilter == null && timeFilter == null) ||
                            (valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
                            (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
                            (valueFilter != null && timeFilter != null &&
                                    valueVisitor.verify(v) &&
                                    timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putDouble(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case FLOAT:
                for (int i = 0; i < oneColData.length; i++) {
                    float v = oneColData.getFloat(i);
                    if ((valueFilter == null && timeFilter == null) ||
                            (valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
                            (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
                            (valueFilter != null && timeFilter != null &&
                                    valueVisitor.verify(v) &&
                                    timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putFloat(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case INT32:
                for (int i = 0; i < oneColData.length; i++) {
                    int v = oneColData.getInt(i);
                    if ((valueFilter == null && timeFilter == null) ||
                            (valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
                            (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
                            (valueFilter != null && timeFilter != null &&
                                    valueVisitor.verify(v) &&
                                    timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putInt(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case INT64:
                for (int i = 0; i < oneColData.length; i++) {
                    long v = oneColData.getLong(i);
                    if ((valueFilter == null && timeFilter == null) ||
                            (valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
                            (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
                            (valueFilter != null && timeFilter != null &&
                                    valueVisitor.verify(v) &&
                                    timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putLong(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case BYTE_ARRAY:
                for (int i = 0; i < oneColData.length; i++) {
                    Binary v = oneColData.getBinary(i);
                    if ((valueFilter == null && timeFilter == null) ||
                            (valueFilter != null && timeFilter == null && valueVisitor.satisfyObject(v, valueFilter)) ||
                            (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
                            (valueFilter != null && timeFilter != null &&
                                    valueVisitor.satisfyObject(v, valueFilter) &&
                                    timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putBinary(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported data type for read:" + oneColData.dataType);
        }

        return res;
    }

    private static DynamicOneColumnData updateValueAccordingToUpdateTrue(DynamicOneColumnData updateTrue
            , DynamicOneColumnData oneColData) {
        if (updateTrue == null) {
            return oneColData;
        }
        if (oneColData == null) {
            return null;
        }
        int idx = 0;
        for (int i = 0; i < updateTrue.length; i++) {
            while (idx < oneColData.length && updateTrue.getTime(i * 2 + 1) >= oneColData.getTime(idx)) {
                if (updateTrue.getTime(i) <= oneColData.getTime(idx)) {
                    oneColData.updateAValueFromDynamicOneColumnData(updateTrue, i, idx);
                }
                idx++;
            }
        }

        return oneColData;
    }

    private static SingleValueVisitor<?> getSingleValueVisitorByDataType(TSDataType type, SingleSeriesFilterExpression filter) {
        switch (type) {
            case INT32:
                return new SingleValueVisitor<Integer>(filter);
            case INT64:
                return new SingleValueVisitor<Long>(filter);
            case FLOAT:
                return new SingleValueVisitor<Float>(filter);
            case DOUBLE:
                return new SingleValueVisitor<Double>(filter);
            default:
                return SingleValueVisitorFactory.getSingleValueVisitor(type);
        }
    }

    /**
     * In cross column query, such as "select s0,s1,s2 from root.vehicle.d0 where time < 106 and (s0 >= 60 or s1 <= 200)," </br>
     * when calculate the common time, the position in InsertDynamicData may be wrong. </br>
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
                            null, null, null);
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















