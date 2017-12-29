package cn.edu.tsinghua.iotdb.query.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.UnSupportedFillTypeException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.fill.FillProcessor;
import cn.edu.tsinghua.iotdb.query.management.ReadLockManager;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.*;

/**
 * This class implements several read methods which can read data in different ways.<br>
 * A RecordReader only represents a (deltaObject, measurement).
 * This class provides some APIs for reading.
 *
 */
public class RecordReader {

    static final Logger logger = LoggerFactory.getLogger(RecordReader.class);

    private String deltaObjectUID, measurementID;

    /** compression type in this series **/
    public CompressionTypeName compressionTypeName;

    /** ReaderManager for current (deltaObjectUID, measurementID) **/
    private ReaderManager readerManager;

    /** for lock **/
    private int lockToken;

    /** bufferwrite data, the data page in memory **/
    public DynamicOneColumnData insertPageInMemory;

    /** bufferwrite data, the unsealed page **/
    public List<ByteArrayInputStream> bufferWritePageList;

    /** insertPageInMemory + bufferWritePageList + overflow **/
    public InsertDynamicData insertAllData;

    /** overflow data **/
    public List<Object> overflowInfo;

    /**
     * @param filePathList bufferwrite file has been serialized completely
     * @throws IOException file error
     */
    public RecordReader(List<String> filePathList, String deltaObjectUID, String measurementID, int lockToken,
                        DynamicOneColumnData insertPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws IOException {
        this.readerManager = new ReaderManager(filePathList);
        this.deltaObjectUID = deltaObjectUID;
        this.measurementID = measurementID;
        this.lockToken = lockToken;
        this.insertPageInMemory = insertPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.overflowInfo = overflowInfo;
    }

    /**
     * @param filePathList       bufferwrite file has been serialized completely
     * @param unsealedFilePath   unsealed file reader
     * @param rowGroupMetadataList unsealed RowGroupMetadataList to construct unsealedFileReader
     * @throws IOException file error
     */
    public RecordReader(List<String> filePathList, String unsealedFilePath,
                        List<RowGroupMetaData> rowGroupMetadataList, String deltaObjectUID, String measurementID, int lockToken,
                        DynamicOneColumnData insertPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws IOException {
        this.readerManager = new ReaderManager(filePathList, unsealedFilePath, rowGroupMetadataList);
        this.deltaObjectUID = deltaObjectUID;
        this.measurementID = measurementID;
        this.lockToken = lockToken;
        this.insertPageInMemory = insertPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.overflowInfo = overflowInfo;
    }

    /**
     * Read one series with overflow and bufferwrite, no filter.
     *
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData queryOneSeries(String deltaObjectId, String measurementId,
                                               DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                               SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter, DynamicOneColumnData res, int fetchSize)
            throws ProcessorException, IOException, PathErrorException {

        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        List<RowGroupReader> dbRowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, timeFilter);
        int rowGroupIndex = 0;
        if (res != null) {
            rowGroupIndex = res.getRowGroupIndex();
        }

        // iterative res, res may be expand
        for (; rowGroupIndex < dbRowGroupReaderList.size(); rowGroupIndex++) {
            RowGroupReader dbRowGroupReader = dbRowGroupReaderList.get(rowGroupIndex);
            if (dbRowGroupReader.getValueReaders().containsKey(measurementId) &&
                    dbRowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                res = ValueReaderProcessor.getValuesWithOverFlow(dbRowGroupReader.getValueReaders().get(measurementId),
                        updateTrue, updateFalse, insertMemoryData, timeFilter, null, valueFilter, res, fetchSize);
                if (res.valueLength >= fetchSize) {
                    return res;
                }
            }
        }

        if (res == null) {
            res = new DynamicOneColumnData(dataType, true);
        }

        // add left insert values
        if (insertMemoryData.hasInsertData()) {
            // TODO the timeFilter, updateTrue, updateFalse in addLeftInsertValue method is unnecessary?
            res.hasReadAll = addLeftInsertValue(res, insertMemoryData, fetchSize, timeFilter, updateTrue, updateFalse);
        } else {
            res.hasReadAll = true;
        }
        return res;
    }

    /**
     * Aggregation calculate function of <code>RecordReader</code> without filter.
     *
     * @param deltaObjectId deltaObjectId of <code>Path</code>
     * @param measurementId measurementId of <code>Path</code>
     * @param aggregateFunction aggregation function
     * @param updateTrue update operation which satisfies the filter
     * @param updateFalse update operation which doesn't satisfy the filter
     * @param insertMemoryData memory bufferwrite insert data
     * @param timeFilter time filter
     * @param freqFilter frequency filter
     * @param valueFilter value filter
     * @return aggregation result
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public AggregateFunction aggregate(String deltaObjectId, String measurementId, AggregateFunction aggregateFunction,
                                       DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                       SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter
    ) throws ProcessorException, IOException, PathErrorException {

        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, timeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                ValueReaderProcessor.aggregate(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, insertMemoryData, updateTrue, updateFalse, timeFilter, freqFilter, valueFilter);
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
     * @param updateTrue update operation which satisfies the filter
     * @param updateFalse update operation which doesn't satisfy the filter
     * @param insertMemoryData memory bufferwrite insert data
     * @param overflowTimeFilter time filter
     * @param freqFilter frequency filter
     * @param timestamps timestamps calculated by the cross filter
     * @return aggregation result and whether still has unread data
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public Pair<AggregateFunction, Boolean> aggregateUsingTimestamps(
            String deltaObjectId, String measurementId, AggregateFunction aggregateFunction,
            DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
            SingleSeriesFilterExpression overflowTimeFilter, SingleSeriesFilterExpression freqFilter, List<Long> timestamps)
            throws ProcessorException, IOException, PathErrorException {

        boolean stillHasUnReadData;

        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, overflowTimeFilter);

        int commonTimestampsIndex = 0;

        int rowGroupIndex = aggregateFunction.resultData.rowGroupIndex;

        for (; rowGroupIndex < rowGroupReaderList.size(); rowGroupIndex++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(rowGroupIndex);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {

                // TODO commonTimestampsIndex could be saved as a parameter

                commonTimestampsIndex = ValueReaderProcessor.aggregateUsingTimestamps(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, insertMemoryData, updateTrue, updateFalse, overflowTimeFilter, freqFilter, timestamps);

                // all value of commonTimestampsIndex has been used,
                // the next batch of commonTimestamps should be loaded
                if (commonTimestampsIndex >= timestamps.size()) {
                    return new Pair<>(aggregateFunction, true);
                }
            }
        }

        // calculate aggregation using unsealed file data and memory data
        if (insertMemoryData != null && insertMemoryData.hasInsertData()) {
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
     *  This function is used for cross column query.
     *
     * @param deltaObjectId
     * @param measurementId
     * @param overflowTimeFilter overflow time filter for this query path
     * @param commonTimestamps
     * @param insertMemoryData
     * @return
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData queryUsingTimestamps(String deltaObjectId, String measurementId,
                                                     SingleSeriesFilterExpression overflowTimeFilter, long[] commonTimestamps,
                                                     InsertDynamicData insertMemoryData)
            throws ProcessorException, IOException, PathErrorException {
        // TODO a hasNext method in IoTDB read process is needed!
        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);
        SingleValueVisitor filterVerifier = null;
        if (overflowTimeFilter != null) {
            filterVerifier = new SingleValueVisitor(overflowTimeFilter);
        }

        DynamicOneColumnData originalQueryData = queryOriginalDataUsingTimestamps(deltaObjectId, measurementId, overflowTimeFilter, commonTimestamps);
        if (originalQueryData == null) {
            originalQueryData = new DynamicOneColumnData(dataType, true);
        }
        DynamicOneColumnData newQueryData = new DynamicOneColumnData(dataType, true);

        int oldDataIdx = 0;
        for (long commonTime : commonTimestamps) {

            // the time in originalQueryData must in commonTimestamps
            if (oldDataIdx < originalQueryData.timeLength && originalQueryData.getTime(oldDataIdx) == commonTime) {
                boolean isOldDataAdoptedFlag = true;
                while (insertMemoryData != null && insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= commonTime) {
                    if (insertMemoryData.getCurrentMinTime() < commonTime) {
                        insertMemoryData.removeCurrentValue();
                    } else if (insertMemoryData.getCurrentMinTime() == commonTime) {
                        newQueryData.putTime(insertMemoryData.getCurrentMinTime());
                        putValueFromMemoryData(newQueryData, insertMemoryData);
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
                    newQueryData.putTime(commonTime);
                    newQueryData.putAValueFromDynamicOneColumnData(originalQueryData, oldDataIdx);
                }

                oldDataIdx++;
            }

            // consider memory data
            while (insertMemoryData != null && insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= commonTime) {
                if (commonTime == insertMemoryData.getCurrentMinTime()) {
                    newQueryData.putTime(insertMemoryData.getCurrentMinTime());
                    putValueFromMemoryData(newQueryData, insertMemoryData);
                }
                insertMemoryData.removeCurrentValue();
            }
        }

        return newQueryData;
    }

    private DynamicOneColumnData queryOriginalDataUsingTimestamps(String deltaObjectId, String measurementId,
                                                                  SingleSeriesFilterExpression overflowTimeFilter, long[] timestamps)
            throws IOException, PathErrorException {

        DynamicOneColumnData res = null;
        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, overflowTimeFilter);
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

    /**
     * Calculate the left value in memory.
     *
     * @param res result answer
     * @param insertMemoryData memory data
     * @param fetchSize read fetch size
     * @param timeFilter filter to select time
     * @param updateTrue <code>DynamicOneColumnData</code> represents which value to update to new value
     * @param updateFalse <code>DynamicOneColumnData</code> represents which value of update to new value is
     *                    not satisfied with the filter
     * @return true represents that all the values has been read
     * @throws IOException TsFile read error
     */
    private boolean addLeftInsertValue(DynamicOneColumnData res, InsertDynamicData insertMemoryData, int fetchSize,
                                       SingleSeriesFilterExpression timeFilter, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse) throws IOException {
        SingleValueVisitor<?> timeVisitor = null;
        if (timeFilter != null) {
            timeVisitor = new SingleValueVisitor(timeFilter);
        }
        long maxTime;
        if (res.valueLength > 0) {
            maxTime = res.getTime(res.valueLength - 1);
        } else {
            maxTime = -1;
        }

        while (insertMemoryData.hasInsertData()) {
            long curTime = insertMemoryData.getCurrentMinTime(); // current insert time
            if (maxTime < curTime) {
                res.putTime(curTime);
                putValueFromMemoryData(res, insertMemoryData);
                insertMemoryData.removeCurrentValue();
            }
            // when the length reach to fetchSize, stop put values and return false
            if (res.valueLength >= fetchSize) {
                return false;
            }
        }
        return true;
    }

    private void putValueFromMemoryData(DynamicOneColumnData res, InsertDynamicData insertMemoryData) {
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

    /**
     * Get the time which is smaller than queryTime and is biggest and its value.
     *
     * @param deltaObjectId
     * @param measurementId
     * @param updateTrue
     * @param insertMemoryData
     * @param beforeTime
     * @param queryTime
     * @param result
     * @throws PathErrorException
     * @throws IOException
     */
    public void getPreviousFillResult(DynamicOneColumnData result, String deltaObjectId, String measurementId,
                                      DynamicOneColumnData updateTrue, InsertDynamicData insertMemoryData,
                                      SingleSeriesFilterExpression overflowTimeFilter, long beforeTime, long queryTime) throws PathErrorException, IOException {

        SingleSeriesFilterExpression leftFilter = gtEq(timeFilterSeries(), beforeTime, true);
        SingleSeriesFilterExpression rightFilter = ltEq(timeFilterSeries(), queryTime, true);
        SingleSeriesFilterExpression fillTimeFilter = (SingleSeriesFilterExpression) and(leftFilter, rightFilter);

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, fillTimeFilter);
        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                // get fill result in ValueReader
                if (FillProcessor.getPreviousFillResultInFile(result, rowGroupReader.getValueReaders().get(measurementId),
                        beforeTime, queryTime, overflowTimeFilter, updateTrue)) {
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
     * @param updateTrue
     * @param insertMemoryData
     * @param beforeTime
     * @param queryTime
     * @param result
     * @throws PathErrorException
     * @throws IOException
     */
    public void getLinearFillResult(DynamicOneColumnData result, String deltaObjectId, String measurementId,
                                      DynamicOneColumnData updateTrue, InsertDynamicData insertMemoryData,
                                      SingleSeriesFilterExpression overflowTimeFilter, long beforeTime, long queryTime, long afterTime)
            throws PathErrorException, IOException {

        SingleSeriesFilterExpression leftFilter = gtEq(timeFilterSeries(), beforeTime, true);
        SingleSeriesFilterExpression rightFilter = ltEq(timeFilterSeries(), afterTime, true);
        SingleSeriesFilterExpression fillTimeFilter = (SingleSeriesFilterExpression) and(leftFilter, rightFilter);

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, fillTimeFilter);
        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                // has get fill result in ValueReader
                if (FillProcessor.getLinearFillResultInFile(result, rowGroupReader.getValueReaders().get(measurementId), beforeTime, queryTime, afterTime,
                        overflowTimeFilter, updateTrue)) {
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

    /**
     * Close current RecordReader.
     *
     * @throws IOException
     * @throws ProcessorException
     */
    public void close() throws IOException, ProcessorException {
        readerManager.close();
    }
}
