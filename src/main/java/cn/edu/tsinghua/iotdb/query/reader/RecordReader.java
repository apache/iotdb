package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.UnSupportedFillTypeException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.fill.FillProcessor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.copy;
import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.and;

/**
 * A <code>RecordReader</code> contains all the data variables which is needed in read process.
 * Note that : it only contains the data of a (deltaObjectID, measurementID).
 *
 */
public class RecordReader {

    static final Logger logger = LoggerFactory.getLogger(RecordReader.class);

    private String deltaObjectID, measurementID;

    /** for read lock **/
    private int lockToken;

    /** data type **/
    private TSDataType dataType;

    /** compression type in this series **/
    public CompressionTypeName compressionTypeName;

    /** 1. TsFile ReaderManager for current (deltaObjectID, measurementID) **/
    public ReaderManager tsFileReaderManager;

    /** 2. bufferwrite data, the unsealed page **/
    public List<ByteArrayInputStream> bufferWritePageList;

    /** 3. bufferwrite insert data, the last page data in memory **/
    public DynamicOneColumnData lastPageInMemory;

    /** 4. overflow insert data **/
    public DynamicOneColumnData overflowInsertData;

    /** 5. overflow update data **/
    public DynamicOneColumnData overflowUpdateTrue;
    private UpdateOperation overflowUpdateTrueOperation;

    /** 6. overflow update data **/
    public DynamicOneColumnData overflowUpdateFalse;
    private UpdateOperation overflowUpdateFalseOperation;

    /** 7. series time filter, this filter is the filter **/
    public SingleSeriesFilterExpression overflowTimeFilter;

    /** 8. series value filter **/
    public SingleSeriesFilterExpression valueFilter;

    /** bufferWritePageList + lastPageInMemory + overflow **/
    public InsertDynamicData insertMemoryData;

    /**
     * @param filePathList bufferwrite file has been serialized completely
     */
    public RecordReader(List<String> filePathList, String deltaObjectID, String measurementID, int lockToken,
                        DynamicOneColumnData lastPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws PathErrorException {
        this.tsFileReaderManager = new ReaderManager(filePathList);
        this.deltaObjectID = deltaObjectID;
        this.measurementID = measurementID;
        this.lockToken = lockToken;
        this.lastPageInMemory = lastPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.dataType = MManager.getInstance().getSeriesType(deltaObjectID + "." + measurementID);

        // to make sure that overflow data will not be null
        this.overflowInsertData = overflowInfo.get(0) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(0);
        this.overflowUpdateTrue = overflowInfo.get(1) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(1);
        this.overflowUpdateFalse = overflowInfo.get(2) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(2);
        this.overflowTimeFilter = (SingleSeriesFilterExpression) overflowInfo.get(3);
        this.overflowUpdateTrueOperation = new UpdateOperation(dataType, overflowUpdateTrue);
        this.overflowUpdateFalseOperation = new UpdateOperation(dataType, overflowUpdateFalse);
    }

    /**
     * @param filePathList       bufferwrite file has been serialized completely
     * @param unsealedFilePath   unsealed file reader
     * @param rowGroupMetadataList unsealed RowGroupMetadataList to construct unsealedFileReader
     */
    public RecordReader(List<String> filePathList, String unsealedFilePath,
                        List<RowGroupMetaData> rowGroupMetadataList, String deltaObjectID, String measurementID, int lockToken,
                        DynamicOneColumnData lastPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws PathErrorException {
        this.tsFileReaderManager = new ReaderManager(filePathList, unsealedFilePath, rowGroupMetadataList);
        this.deltaObjectID = deltaObjectID;
        this.measurementID = measurementID;
        this.lockToken = lockToken;
        this.lastPageInMemory = lastPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.dataType = MManager.getInstance().getSeriesType(deltaObjectID + "." + measurementID);

        // to make sure that overflow data will not be null
        this.overflowInsertData = overflowInfo.get(0) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(0);
        this.overflowUpdateTrue = overflowInfo.get(1) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(1);
        this.overflowUpdateFalse = overflowInfo.get(2) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(2);
        this.overflowTimeFilter = (SingleSeriesFilterExpression) overflowInfo.get(3);
        this.overflowUpdateTrueOperation = new UpdateOperation(dataType, overflowUpdateTrue);
        this.overflowUpdateFalseOperation = new UpdateOperation(dataType, overflowUpdateFalse);
    }

    public void buildInsertMemoryData(SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter) {

        DynamicOneColumnData overflowUpdateTrueCopy = copy(overflowUpdateTrue);
        DynamicOneColumnData overflowUpdateFalseCopy = copy(overflowUpdateFalse);

        insertMemoryData = new InsertDynamicData(dataType, compressionTypeName,
                bufferWritePageList, lastPageInMemory,
                overflowInsertData, overflowUpdateTrueCopy, overflowUpdateFalseCopy,
                mergeTimeFilter(overflowTimeFilter, queryTimeFilter), queryValueFilter);
    }

    /**
     * Read one series with overflow and bufferwrite, no filter.
     *
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData queryOneSeries(String deltaObjectId, String measurementId,
                                               SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter,
                                               DynamicOneColumnData res, int fetchSize)
            throws ProcessorException, IOException, PathErrorException {

        SingleSeriesFilterExpression mergeTimeFilter = mergeTimeFilter(overflowTimeFilter, queryTimeFilter);
        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, mergeTimeFilter);
        int rowGroupIndex = 0;
        if (res != null) {
            rowGroupIndex = res.getRowGroupIndex();
        }

        // iterative res, res may be expand
        for (; rowGroupIndex < rowGroupReaderList.size(); rowGroupIndex++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(rowGroupIndex);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                res = ValueReaderProcessor.getValuesWithOverFlow(rowGroupReader.getValueReaders().get(measurementId),
                        overflowUpdateTrue, overflowUpdateFalse, insertMemoryData, mergeTimeFilter, queryValueFilter, res, fetchSize);
                if (res.valueLength >= fetchSize) {
                    return res;
                }
            }
        }

        if (res == null) {
            res = new DynamicOneColumnData(dataType, true);
        }

        while (insertMemoryData.hasInsertData()) {
            putMemoryDataToResult(res, insertMemoryData);
            insertMemoryData.removeCurrentValue();

            // when the length reach to fetchSize, stop put values and return false
            if (res.valueLength >= fetchSize) {
                return res;
            }
        }

        return res;
    }

    /**
     * Aggregation calculate function of <code>RecordReader</code> without filter.
     *
     * @param deltaObjectId deltaObjectId of <code>Path</code>
     * @param measurementId measurementId of <code>Path</code>
     * @param aggregateFunction aggregation function
     * @param queryTimeFilter time filter
     * @param valueFilter value filter
     * @return aggregation result
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public AggregateFunction aggregate(String deltaObjectId, String measurementId, AggregateFunction aggregateFunction,
                                       SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression valueFilter
    ) throws ProcessorException, IOException, PathErrorException {

        SingleSeriesFilterExpression mergeTimeFilter = mergeTimeFilter(queryTimeFilter, overflowTimeFilter);

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, mergeTimeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                ValueReaderProcessor.aggregate(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, insertMemoryData, overflowUpdateTrue, overflowUpdateFalse, mergeTimeFilter, valueFilter);
            }
        }

        // consider left insert values
        // all timestamp of these values are greater than timestamp in List<RowGroupReader>
        if (insertMemoryData != null && insertMemoryData.hasInsertData()) {
            aggregateFunction.calculateValueFromLeftMemoryData(insertMemoryData);
        }

        return aggregateFunction;
    }

    /**
     * <p>
     * Calculate the aggregate result using the given timestamps.
     * Return a pair of AggregationResult and Boolean, AggregationResult represents the aggregation result,
     * Boolean represents that whether there still has unread data.
     *
     * @param deltaObjectId deltaObjectId deltaObjectId of <code>Path</code>
     * @param measurementId measurementId of <code>Path</code>
     * @param aggregateFunction aggregation function
     * @param queryTimeFilter time filter
     * @param timestamps timestamps calculated by the cross filter
     * @return aggregation result and whether still has unread data
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public Pair<AggregateFunction, Boolean> aggregateUsingTimestamps(
            String deltaObjectId, String measurementId, AggregateFunction aggregateFunction,
            SingleSeriesFilterExpression queryTimeFilter, List<Long> timestamps)
            throws ProcessorException, IOException, PathErrorException {

        boolean stillHasUnReadData;

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, overflowTimeFilter);

        int commonTimestampsIndex = 0;

        int rowGroupIndex = aggregateFunction.resultData.rowGroupIndex;

        for (; rowGroupIndex < rowGroupReaderList.size(); rowGroupIndex++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(rowGroupIndex);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {

                // TODO commonTimestampsIndex could be saved as a parameter

                commonTimestampsIndex = ValueReaderProcessor.aggregateUsingTimestamps(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, insertMemoryData, overflowUpdateTrue, overflowUpdateFalse, overflowTimeFilter, timestamps);

                // all value of commonTimestampsIndex has been used,
                // the next batch of commonTimestamps should be loaded
                if (commonTimestampsIndex >= timestamps.size()) {
                    return new Pair<>(aggregateFunction, true);
                }
            }
        }

        // calculate aggregation using unsealed file data and memory data
        if (insertMemoryData.hasInsertData()) {
            stillHasUnReadData = aggregateFunction.calcAggregationUsingTimestamps(insertMemoryData, timestamps, commonTimestampsIndex);
        } else {
            if (commonTimestampsIndex < timestamps.size()) {
                stillHasUnReadData = false;
            } else {
                stillHasUnReadData = true;
            }
        }

        return new Pair<>(aggregateFunction, stillHasUnReadData);
    }

    /**
     *  <p> This function is used for cross series query.
     *  Notice that: query using timestamps, query time filter and value filter is not needed,
     *  but overflow time filter, insert data and overflow update true data is needed.
     *
     * @param deltaObjectId
     * @param measurementId
     * @param commonTimestamps
     * @return cross query result
     * @throws IOException file read error
     */
    public DynamicOneColumnData queryUsingTimestamps(String deltaObjectId, String measurementId, long[] commonTimestamps)
            throws IOException, PathErrorException {

        SingleValueVisitor filterVerifier = null;
        if (this.overflowTimeFilter != null) {
            filterVerifier = new SingleValueVisitor(this.overflowTimeFilter);
        }

        DynamicOneColumnData originalQueryData = queryOriginalDataUsingTimestamps(deltaObjectId, measurementId, overflowTimeFilter, commonTimestamps);
        if (originalQueryData == null) {
            originalQueryData = new DynamicOneColumnData(dataType, true);
        }
        DynamicOneColumnData queryResult = new DynamicOneColumnData(dataType, true);

        int oldDataIdx = 0;
        for (long commonTime : commonTimestamps) {

            // the time in originalQueryData must in commonTimestamps
            if (oldDataIdx < originalQueryData.timeLength && originalQueryData.getTime(oldDataIdx) == commonTime) {
                boolean isOldDataAdoptedFlag = true;
                while (insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= commonTime) {
                    if (insertMemoryData.getCurrentMinTime() < commonTime) {
                        insertMemoryData.removeCurrentValue();
                    } else if (insertMemoryData.getCurrentMinTime() == commonTime) {
                        putMemoryDataToResult(queryResult, insertMemoryData);
                        insertMemoryData.removeCurrentValue();
                        oldDataIdx++;
                        isOldDataAdoptedFlag = false;
                        break;
                    }
                }

                if (!isOldDataAdoptedFlag) {
                    continue;
                }

                if (overflowTimeFilter == null || filterVerifier.verify(commonTime)) {
                    putFileDataToResult(queryResult, originalQueryData, oldDataIdx);
                }

                oldDataIdx++;
            }

            // consider memory data
            while (insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= commonTime) {
                if (commonTime == insertMemoryData.getCurrentMinTime()) {
                    putMemoryDataToResult(queryResult, insertMemoryData);
                }
                insertMemoryData.removeCurrentValue();
            }
        }

        return queryResult;
    }

    private DynamicOneColumnData queryOriginalDataUsingTimestamps(String deltaObjectId, String measurementId,
                                                                  SingleSeriesFilterExpression overflowTimeFilter, long[] timestamps)
            throws IOException, PathErrorException {

        DynamicOneColumnData res = null;

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, overflowTimeFilter);
        for (int i = 0; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                if (res == null) {
                    res = rowGroupReader.readValueUseTimestamps(measurementId, timestamps);
                } else {
                    DynamicOneColumnData tmpRes = rowGroupReader.readValueUseTimestamps(measurementId, timestamps);
                    res.mergeRecord(tmpRes);
                }
            }
        }
        return res;
    }

    private void putMemoryDataToResult(DynamicOneColumnData res, InsertDynamicData insertMemoryData) {
        res.putTime(insertMemoryData.getCurrentMinTime());

        switch (insertMemoryData.getDataType()) {
            case BOOLEAN:
                res.putBoolean(insertMemoryData.getCurrentBooleanValue());
                break;
            case INT32:
                res.putInt(insertMemoryData.getCurrentIntValue());
                break;
            case INT64:
                res.putLong(insertMemoryData.getCurrentLongValue());
                break;
            case FLOAT:
                res.putFloat(insertMemoryData.getCurrentFloatValue());
                break;
            case DOUBLE:
                res.putDouble(insertMemoryData.getCurrentDoubleValue());
                break;
            case TEXT:
                res.putBinary(insertMemoryData.getCurrentBinaryValue());
                break;
            default:
                throw new UnSupportedDataTypeException("UnuSupported DataType : " + insertMemoryData.getDataType());
        }
    }

    private void putFileDataToResult(DynamicOneColumnData queryResult, DynamicOneColumnData originalQueryData, int dataIdx) {
        long time = originalQueryData.getTime(dataIdx);

        while(overflowUpdateTrueOperation.hasNext() && overflowUpdateTrueOperation.getUpdateEndTime() < time)
            overflowUpdateTrueOperation.next();
        while(overflowUpdateFalseOperation.hasNext() && overflowUpdateFalseOperation.getUpdateEndTime() < time)
            overflowUpdateFalseOperation.next();

        if (overflowUpdateFalseOperation.verify(time)) {
            return;
        }

        queryResult.putTime(time);
        switch (dataType) {
            case BOOLEAN:
                if (overflowUpdateTrueOperation.verify(time)) {
                    queryResult.putBoolean(overflowUpdateTrueOperation.getBoolean());
                    return;
                }
                queryResult.putBoolean(originalQueryData.getBoolean(dataIdx));
                break;
            case INT32:
                if (overflowUpdateTrueOperation.verify(time)) {
                    queryResult.putInt(overflowUpdateTrueOperation.getInt());
                    return;
                }
                queryResult.putInt(originalQueryData.getInt(dataIdx));
                break;
            case INT64:
                if (overflowUpdateTrueOperation.verify(time)) {
                    queryResult.putLong(overflowUpdateTrueOperation.getLong());
                    return;
                }
                queryResult.putLong(originalQueryData.getLong(dataIdx));
                break;
            case FLOAT:
                if (overflowUpdateTrueOperation.verify(time)) {
                    queryResult.putFloat(overflowUpdateTrueOperation.getFloat());
                    return;
                }
                queryResult.putFloat(originalQueryData.getFloat(dataIdx));
                break;
            case DOUBLE:
                if (overflowUpdateTrueOperation.verify(time)) {
                    queryResult.putDouble(overflowUpdateTrueOperation.getDouble());
                    return;
                }
                queryResult.putDouble(originalQueryData.getDouble(dataIdx));
                break;
            case TEXT:
                if (overflowUpdateTrueOperation.verify(time)) {
                    queryResult.putBinary(overflowUpdateTrueOperation.getText());
                    return;
                }
                queryResult.putBinary(originalQueryData.getBinary(dataIdx));
                break;
            default:
                throw new UnSupportedDataTypeException("UnuSupported DataType : " + insertMemoryData.getDataType());
        }
    }

    /**
     * Get the time which is smaller than queryTime and is biggest and its value.
     *
     * @param deltaObjectId
     * @param measurementId
     * @param beforeTime
     * @param queryTime
     * @param result
     * @throws PathErrorException
     * @throws IOException
     */
    public void getPreviousFillResult(DynamicOneColumnData result, String deltaObjectId, String measurementId,
                                      SingleSeriesFilterExpression fillTimeFilter, long beforeTime, long queryTime)
            throws PathErrorException, IOException {

        SingleSeriesFilterExpression mergeTimeFilter = mergeTimeFilter(overflowTimeFilter, fillTimeFilter);

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, mergeTimeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                // get fill result in ValueReader
                if (FillProcessor.getPreviousFillResultInFile(result, rowGroupReader.getValueReaders().get(measurementId),
                        beforeTime, queryTime, mergeTimeFilter, overflowUpdateTrue)) {
                    break;
                }
            }
        }

        // get fill result in InsertMemoryData
        FillProcessor.getPreviousFillResultInMemory(result, insertMemoryData, beforeTime, queryTime);

        if (result.valueLength == 0) {
            result.putEmptyTime(queryTime);
        } else {
            result.setTime(0, queryTime);
        }
    }

    /**
     * Get the time which is smaller than queryTime and is biggest and its value.
     *
     * @param deltaObjectId
     * @param measurementId
     * @param beforeTime
     * @param queryTime
     * @param result
     * @throws PathErrorException
     * @throws IOException
     */
    public void getLinearFillResult(DynamicOneColumnData result, String deltaObjectId, String measurementId,
                                      SingleSeriesFilterExpression fillTimeFilter, long beforeTime, long queryTime, long afterTime)
            throws PathErrorException, IOException {

        SingleSeriesFilterExpression mergeTimeFilter = mergeTimeFilter(overflowTimeFilter, fillTimeFilter);

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, mergeTimeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {

                // has get fill result in ValueReader
                if (FillProcessor.getLinearFillResultInFile(result, rowGroupReader.getValueReaders().get(measurementId), beforeTime, queryTime, afterTime,
                        mergeTimeFilter, overflowUpdateTrue)) {
                    break;
                }
            }
        }

        // get fill result in InsertMemoryData
        FillProcessor.getLinearFillResultInMemory(result, insertMemoryData, beforeTime, queryTime, afterTime);

        if (result.timeLength == 0) {
            result.putEmptyTime(queryTime);
        } else if (result.valueLength == 1) {
            // only has previous or after time
            if (result.getTime(0) != queryTime) {
                result.timeLength = result.valueLength = 0;
                result.putEmptyTime(queryTime);
            }
        } else {
            // startTime and endTime will not be equals to queryTime
            long startTime = result.getTime(0);
            long endTime = result.getTime(1);

            switch (result.dataType) {
                case INT32:
                    int startIntValue = result.getInt(0);
                    int endIntValue = result.getInt(1);
                    result.timeLength = result.valueLength = 1;
                    result.setTime(0, queryTime);
                    int fillIntValue = startIntValue + (int)((double)(endIntValue-startIntValue)/(double)(endTime-startTime)*(double)(queryTime-startTime));
                    result.setInt(0, fillIntValue);
                    break;
                case INT64:
                    long startLongValue = result.getLong(0);
                    long endLongValue = result.getLong(1);
                    result.timeLength = result.valueLength = 1;
                    result.setTime(0, queryTime);
                    long fillLongValue = startLongValue + (long)((double)(endLongValue-startLongValue)/(double)(endTime-startTime)*(double)(queryTime-startTime));
                    result.setLong(0, fillLongValue);
                    break;
                case FLOAT:
                    float startFloatValue = result.getFloat(0);
                    float endFloatValue = result.getFloat(1);
                    result.timeLength = result.valueLength = 1;
                    result.setTime(0, queryTime);
                    float fillFloatValue = startFloatValue + (float)((endFloatValue-startFloatValue)/(endTime-startTime)*(queryTime-startTime));
                    result.setFloat(0, fillFloatValue);
                    break;
                case DOUBLE:
                    double startDoubleValue = result.getDouble(0);
                    double endDoubleValue = result.getDouble(1);
                    result.timeLength = result.valueLength = 1;
                    result.setTime(0, queryTime);
                    double fillDoubleValue = startDoubleValue + (double)((endDoubleValue-startDoubleValue)/(endTime-startTime)*(queryTime-startTime));
                    result.setDouble(0, fillDoubleValue);
                    break;
                default:
                    throw new UnSupportedFillTypeException("Unsupported linear fill data type : " + result.dataType);

            }

        }
    }

    private SingleSeriesFilterExpression mergeTimeFilter(SingleSeriesFilterExpression overflowTimeFilter, SingleSeriesFilterExpression queryTimeFilter) {

        if (overflowTimeFilter == null && queryTimeFilter == null) {
            return null;
        } else if (overflowTimeFilter != null && queryTimeFilter == null) {
            return overflowTimeFilter;
        } else if (overflowTimeFilter == null) {
            return queryTimeFilter;
        } else {
            return (SingleSeriesFilterExpression) and(overflowTimeFilter, queryTimeFilter);
        }
    }

    /**
     * Close current RecordReader.
     *
     * @throws IOException
     * @throws ProcessorException
     */
    public void close() throws IOException, ProcessorException {
        tsFileReaderManager.close();
    }
}
