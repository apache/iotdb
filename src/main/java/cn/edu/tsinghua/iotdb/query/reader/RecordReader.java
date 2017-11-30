package cn.edu.tsinghua.iotdb.query.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationResult;
import cn.edu.tsinghua.iotdb.query.management.ReadLockManager;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
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

/**
 * This class implements several read methods which can read data in different ways.<br>
 * A RecordReader only represents a (deltaObject, measurement).
 * This class provides some APIs for reading.
 *
 */
public class RecordReader {

    static final Logger logger = LoggerFactory.getLogger(RecordReader.class);

    private ReaderManager readerManager;
    private int lockToken;  // for lock
    private String deltaObjectUID, measurementID;
    public DynamicOneColumnData insertPageInMemory;  // bufferwrite insert memory page unsealed
    public List<ByteArrayInputStream> bufferWritePageList;  // bufferwrite insert memory page
    public CompressionTypeName compressionTypeName;
    public InsertDynamicData insertAllData;  // insertPageInMemory + bufferWritePageList;
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
     * @param filePathList              bufferwrite file has been serialized completely
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
     * read one column function with overflow, no filter.
     *
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData getValueInOneColumnWithOverflow(String deltaObjectId, String measurementId,
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
                res = OverflowBufferWriteProcessor.getValuesWithOverFlow(dbRowGroupReader.getValueReaders().get(measurementId),
                        updateTrue, updateFalse, insertMemoryData, timeFilter, null, valueFilter, res, fetchSize);
                if (res.valueLength >= fetchSize) {
                    return res;
                }
            }
        }

        if (res == null) {
            res = createAOneColRetByFullPath(deltaObjectId + "." + measurementId);
        }

        // add left insert values
        if (insertMemoryData.hasInsertData()) {
            res.hasReadAll = addLeftInsertValue(res, insertMemoryData, fetchSize, timeFilter, updateTrue, updateFalse);
        } else {
            res.hasReadAll = true;
        }
        return res;
    }

    private DynamicOneColumnData createAOneColRetByFullPath(String fullPath) throws ProcessorException {
        try {
            TSDataType type = MManager.getInstance().getSeriesType(fullPath);
            DynamicOneColumnData res = new DynamicOneColumnData(type, true);
            return res;
        } catch (PathErrorException e) {
            throw new ProcessorException(e.getMessage());
        }
    }

    /**
     * Aggregation calculate function of <code>RecordReader</code> without filter.
     *
     * @param deltaObjectId deltaObjectId of <code>Path</code>
     * @param measurementId measurementId of <code>Path</code>
     * @param func aggregation function
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
    public AggregationResult aggregate(String deltaObjectId, String measurementId, AggregateFunction func,
                                       DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                       SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter
    ) throws ProcessorException, IOException, PathErrorException {

        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);
        List<RowGroupReader> dbRowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, timeFilter);

        for (RowGroupReader dbRowGroupReader : dbRowGroupReaderList) {
            if (dbRowGroupReader.getValueReaders().containsKey(measurementId) &&
                    dbRowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                OverflowBufferWriteProcessor.aggregate(dbRowGroupReader.getValueReaders().get(measurementId),
                        func, insertMemoryData, updateTrue, updateFalse, timeFilter, freqFilter, valueFilter);
            }
        }

        // add left insert values
        if (insertMemoryData != null && insertMemoryData.hasInsertData()) {
            func.calculateValueFromLeftMemoryData(insertMemoryData);
        }
        return func.result;
    }

    /**
     * <p>
     * Calculate the aggregate result using the given timestamps.
     * Return a pair of AggregationResult and Boolean, AggregationResult represents the aggregation result,
     * Boolean represents that whether there still has unread data.
     * </p>
     *
     * @param deltaObjectId deltaObjectId deltaObjectId of <code>Path</code>
     * @param measurementId measurementId of <code>Path</code>
     * @param func aggregation function
     * @param updateTrue update operation which satisfies the filter
     * @param updateFalse update operation which doesn't satisfy the filter
     * @param insertMemoryData memory bufferwrite insert data
     * @param timeFilter time filter
     * @param freqFilter frequency filter
     * @param valueFilter value filter
     * @param timestamps timestamps calculated by the cross filter
     * @param aggreData aggregation result calculated last time //TODO this parameter is unnecessary?
     * @return aggregation result and whether still has unread data
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public Pair<AggregationResult, Boolean> aggregateUsingTimestamps(String deltaObjectId, String measurementId, AggregateFunction func,
                                                                     DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                                                     SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                                     List<Long> timestamps, DynamicOneColumnData aggreData
    ) throws ProcessorException, IOException {

        boolean hasUnReadData = false;

        List<RowGroupReader> dbRowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, timeFilter);

        int commonTimestampsIndex = 0;
        // TODO if the DbRowGroupReader.ValueReaders.get(measurementId) has been read, how to avoid it?
        for (RowGroupReader dbRowGroupReader : dbRowGroupReaderList) {
            if (dbRowGroupReader.getValueReaders().containsKey(measurementId)) {
                commonTimestampsIndex = OverflowBufferWriteProcessor.aggregateUsingTimestamps(dbRowGroupReader.getValueReaders().get(measurementId),
                        func, insertMemoryData, updateTrue, updateFalse, timeFilter, freqFilter, timestamps, aggreData);
            }
        }

        // calc aggregation using memory data
        if (insertMemoryData != null && insertMemoryData.hasInsertData()) {
            hasUnReadData = func.calcAggregationUsingTimestamps(insertMemoryData, timestamps, commonTimestampsIndex);
        } else {
            if (commonTimestampsIndex < timestamps.size()) {
                hasUnReadData = false;
            } else {
                hasUnReadData = true;
            }
        }

        return new Pair<>(func.result, hasUnReadData);
    }

    /**
     *  This function is used for cross column query.
     *
     * @return
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData getValuesUseTimestampsWithOverflow(String deltaObjectId, String measurementId, long[] timestamps,
                                                                   DynamicOneColumnData updateTrue, InsertDynamicData insertMemoryData,
                                                                   SingleSeriesFilterExpression deleteFilter)
            throws ProcessorException, IOException {

        TSDataType dataType;
        try {
            dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);
        } catch (PathErrorException e) {
            throw new ProcessorException(e.getMessage());
        }

        DynamicOneColumnData oldRes = getValuesUseTimestamps(deltaObjectId, measurementId, timestamps);
        if (oldRes == null) {
            oldRes = new DynamicOneColumnData(dataType, true);
        }
        DynamicOneColumnData res = new DynamicOneColumnData(dataType, true);

        // the timestamps of timeData is eventual, its has conclude the value of insertMemory.
        int oldResIdx = 0;

        for (int i = 0; i < timestamps.length; i++) {
            // no need to consider update data, because insertMemoryData has dealed with update data.
            if (oldResIdx < oldRes.timeLength && timestamps[i] == oldRes.getTime(oldResIdx)) {
                if (insertMemoryData != null && insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= timestamps[i]) {
                    if (insertMemoryData.getCurrentMinTime() == timestamps[i]) {
                        res.putTime(insertMemoryData.getCurrentMinTime());
                        putValueUseDataType(res, insertMemoryData);
                        insertMemoryData.removeCurrentValue();
                        oldResIdx++;
                        continue;
                    } else {
                        insertMemoryData.removeCurrentValue();
                    }
                }
                res.putTime(timestamps[i]);
                res.putAValueFromDynamicOneColumnData(oldRes, oldResIdx);
                oldResIdx++;
            }

            // deal with insert data
            while (insertMemoryData != null && insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= timestamps[i]) {
                if (timestamps[i] == insertMemoryData.getCurrentMinTime()) {
                    res.putTime(insertMemoryData.getCurrentMinTime());
                    putValueUseDataType(res, insertMemoryData);
                }
                insertMemoryData.removeCurrentValue();
            }
        }

        return res;
    }

    /**
     * for cross getIndex, to get values in one column according to common timestamps.
     *
     * @return
     * @throws IOException
     */
    private DynamicOneColumnData getValuesUseTimestamps(String deltaObjectId, String measurementId, long[] timestamps)
            throws IOException {
        DynamicOneColumnData res = null;

        //TODO could the parameter timestamps be optimized?
        List<RowGroupReader> dbRowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, null);
        for (int i = 0; i < dbRowGroupReaderList.size(); i++) {
            RowGroupReader dbRowGroupReader = dbRowGroupReaderList.get(i);
            if (i == 0) {
                res = dbRowGroupReader.readValueUseTimestamps(measurementId, timestamps);
            } else {
                DynamicOneColumnData tmpRes = dbRowGroupReader.readValueUseTimestamps(measurementId, timestamps);
                res.mergeRecord(tmpRes);
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
     * @throws IOException
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
                putValueUseDataType(res, insertMemoryData);
                insertMemoryData.removeCurrentValue();
            }
            // when the length reach to fetchSize, stop put values and return false
            if (res.valueLength >= fetchSize) {
                return false;
            }
        }
        return true;
    }

    private void putValueUseDataType(DynamicOneColumnData res, InsertDynamicData insertMemoryData) {
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
     * Use {@code RecordReaderFactory} to manage all RecordReader.
     *
     * @throws ProcessorException
     */
    public void closeFromFactory() throws ProcessorException {
        RecordReaderFactory.getInstance().closeOneRecordReader(this);
    }

    /**
     * Close current RecordReader.
     *
     * @throws IOException
     * @throws ProcessorException
     */
    public void close() throws IOException, ProcessorException {
        readerManager.close();
        // unlock for one subQuery
        ReadLockManager.getInstance().unlockForSubQuery(deltaObjectUID, measurementID, lockToken);
    }
}
