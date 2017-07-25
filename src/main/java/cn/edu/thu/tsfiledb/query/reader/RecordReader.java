package cn.edu.thu.tsfiledb.query.reader;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.aggregation.AggregationResult;
import cn.edu.thu.tsfiledb.query.management.ReadLockManager;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.support.ColumnInfo;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfile.common.exception.ProcessorException;


/**
 * This class implements several read methods which can read data in different ways.<br>
 * A RecordReader only represents a (deltaObject, measurement).
 * This class provides some APIs for reading.
 *
 * @author ZJR, CGF
 */

public class RecordReader {

    static final Logger LOG = LoggerFactory.getLogger(RecordReader.class);
    private ReaderManager readerManager;
    private int lockToken;  // for lock
    private String deltaObjectUID, measurementID;
    public DynamicOneColumnData insertPageInMemory;  // bufferwrite insert memory page unsealed
    public List<ByteArrayInputStream> bufferWritePageList;  // bufferwrite insert memory page
    public CompressionTypeName compressionTypeName;
    public InsertDynamicData insertAllData;  // insertPageInMemory + bufferWritePageList;
    public List<Object> overflowInfo;

    /**
     * @param rafList bufferwrite file has been serialized completely
     * @throws IOException file error
     */
    public RecordReader(List<TSRandomAccessFileReader> rafList, String deltaObjectUID, String measurementID, int lockToken,
                        DynamicOneColumnData insertPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws IOException {
        this.readerManager = new ReaderManager(rafList);
        this.deltaObjectUID = deltaObjectUID;
        this.measurementID = measurementID;
        this.lockToken = lockToken;
        this.insertPageInMemory = insertPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.overflowInfo = overflowInfo;
    }

    /**
     * @param rafList              bufferwrite file has been serialized completely
     * @param unsealedFileReader   unsealed file reader
     * @param rowGroupMetadataList unsealed RowGroupMetadataList to construct unsealedFileReader
     * @throws IOException file error
     */
    public RecordReader(List<TSRandomAccessFileReader> rafList, TSRandomAccessFileReader unsealedFileReader,
                        List<RowGroupMetaData> rowGroupMetadataList, String deltaObjectUID, String measurementID, int lockToken,
                        DynamicOneColumnData insertPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws IOException {
        this.readerManager = new ReaderManager(rafList, unsealedFileReader, rowGroupMetadataList);
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
    public DynamicOneColumnData getValueInOneColumnWithOverflow(String deviceUID, String sensorId,
                                                                DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                                                SingleSeriesFilterExpression timeFilter, DynamicOneColumnData res, int fetchSize)
            throws ProcessorException, IOException {

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deviceUID);
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        // iterative res, res may be expand
        for (; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            if (rowGroupReader.getValueReaders().containsKey(sensorId)) {
                res = rowGroupReader.getValueReaders().get(sensorId)
                        .getValuesWithOverFlow(updateTrue, updateFalse, insertMemoryData, timeFilter, null, null, res, fetchSize);
                res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
                if (res.valueLength >= fetchSize) {
                    res.hasReadAll = false;
                    return res;
                }
            }
        }

        if (res == null) {
            res = createAOneColRetByFullPath(deviceUID + "." + sensorId);
        }
        // add left insert values
        if (insertMemoryData.hasInsertData()) {
            res.hasReadAll = addLeftInsertValue(res, insertMemoryData, fetchSize, timeFilter, updateTrue, updateFalse);
        } else {
            res.hasReadAll = true;
        }
        return res;
    }

    /**
     * read one column with filter and overflow.
     *
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData getValueWithFilterAndOverflow(String deviceUID, String sensorId,
                                                              DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                                              SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                              DynamicOneColumnData res, int fetchSize) throws ProcessorException, IOException {

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deviceUID);

        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        for (; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            if (rowGroupReader.getValueReaders().containsKey(sensorId)) {
                res = rowGroupReader.getValueReaders().get(sensorId)
                        .getValuesWithOverFlow(updateTrue, updateFalse, insertMemoryData, timeFilter, freqFilter, valueFilter, res,
                                fetchSize);
                res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
                if (res.valueLength >= fetchSize) {
                    res.hasReadAll = false;
                    return res;
                }
            }
        }

        if (res == null) {
            res = createAOneColRetByFullPath(deviceUID + "." + sensorId);
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
            res.setDeltaObjectType(MManager.getInstance().getDeltaObjectTypeByPath(fullPath));
            return res;
        } catch (PathErrorException e) {
            throw new ProcessorException(e.getMessage());
        }
    }

    public AggregationResult aggregate(String deviceUID, String sensorId, AggregateFunction func,
                                       DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                       SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter
    ) throws ProcessorException, IOException {

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deviceUID);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(sensorId)) {
                rowGroupReader.getValueReaders().get(sensorId)
                        .aggreate(func, insertMemoryData, updateTrue, updateFalse, timeFilter, freqFilter, valueFilter);
            }
        }

        // add left insert values
        if (insertMemoryData != null && insertMemoryData.hasInsertData()) {
            func.calculateFromLeftMemoryData(insertMemoryData);
        }
        return func.result;
    }

    /**
     *  This function is used for cross column query.
     *
     * @return
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData getValuesUseTimeValueWithOverflow(String deviceUID, String sensorId, long[] timestamps,
                                                                  DynamicOneColumnData updateTrue, InsertDynamicData insertMemoryData, SingleSeriesFilterExpression deleteFilter)
            throws ProcessorException, IOException {
        TSDataType dataType;
        String deviceType;
        try {
            dataType = MManager.getInstance().getSeriesType(deviceUID + "." + sensorId);
            deviceType = MManager.getInstance().getDeltaObjectTypeByPath(deviceUID);
        } catch (PathErrorException e) {
            throw new ProcessorException(e.getMessage());
        }
        DynamicOneColumnData oldRes = getValuesUseTimestamps(deviceUID, sensorId, timestamps);
        if (oldRes == null) {
            oldRes = new DynamicOneColumnData(dataType, true);
            oldRes.setDeltaObjectType(deviceType);
        }
        DynamicOneColumnData res = new DynamicOneColumnData(dataType, true);
        res.setDeltaObjectType(deviceType);

        // the timestamps of timeData is eventual, its has conclude the value of insertMemory.
        int oldResIdx = 0;

        for (int i = 0; i < timestamps.length; i++) {
            // no need to consider update data, because insertMemoryData has dealed with update data.
            if (oldResIdx < oldRes.timeLength && timestamps[i] == oldRes.getTime(oldResIdx)) {
                if (insertMemoryData != null && insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= timestamps[i]) {
                    res.putTime(insertMemoryData.getCurrentMinTime());
                    putValueUseDataType(res, insertMemoryData);
                    if (insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= timestamps[i]) {
                        oldResIdx++;
                    }
                    insertMemoryData.removeCurrentValue();
                } else {
                    oldResIdx++;
                    res.putTime(timestamps[i]);
                    res.putAValueFromDynamicOneColumnData(oldRes, oldResIdx);
                }
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
    private DynamicOneColumnData getValuesUseTimestamps(String deltaObjectUID, String measurementUId, long[] timestamps)
            throws IOException {
        DynamicOneColumnData res = null;
        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        for (int i = 0; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            if (i == 0) {
                res = rowGroupReader.readValueUseTimestamps(measurementUId, timestamps);
                res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
            } else {
                DynamicOneColumnData tmpRes = rowGroupReader.readValueUseTimestamps(measurementUId, timestamps);
                res.mergeRecord(tmpRes);
            }
        }
        return res;
    }

    /**
     * Put left values in insertMemoryData to res.
     *
     * @param res result answer
     * @param insertMemoryData memory data
     * @param fetchSize read fetch size
     * @return true represents that all the values has been read.
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
     * {NEWFUNC} use {@code RecordReaderFactory} to manage all RecordReader
     *
     * @throws ProcessorException
     */
    public void closeFromFactory() throws ProcessorException {
        RecordReaderFactory.getInstance().closeOneRecordReader(this);
    }

    /**
     * {NEWFUNC} for optimization in recordReaderFactory
     */
    public void reopenIfChanged() {
        // TODO: how to reopen a recordReader
    }

    /**
     * {NEWFUNC} close current RecordReader
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
