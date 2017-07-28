package cn.edu.thu.tsfiledb.query.dataset;

import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.encoding.decoder.Decoder;
import cn.edu.thu.tsfile.encoding.decoder.DeltaBinaryDecoder;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.Digest;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.thu.tsfile.timeseries.read.PageReader;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * A new DynamicOneColumnData which replaces insertTrue and contains unsealed PageList.
 *
 * @author CGF
 */
public class InsertDynamicData extends DynamicOneColumnData {
    private static final Logger LOG = LoggerFactory.getLogger(InsertDynamicData.class);
    public List<ByteArrayInputStream> pageList;
    private int pageIndex = 0;
    private PageReader pageReader = null;
    private CompressionTypeName compressionTypeName;
    private TSDataType dataType;
    private Decoder timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder(), valueDecoder, freDecoder;
    private long currentSatisfiedTime = -1; // timestamp for page list
    public SingleSeriesFilterExpression timeFilter, valueFilter, frequencyFilter;

    private int curSatisfiedIntValue;
    private boolean curSatisfiedBooleanValue;
    private long curSatisfiedLongValue;
    private float curSatisfiedFloatValue;
    private double curSatisfiedDoubleValue;
    private Binary curSatisfiedBinaryValue;
    private int curTimeIndex = -1;
    private long[] timeValues; // time for current read page
    private InputStream page = null; // value inputstream for current read page

    private DigestVisitor digestVisitor = new DigestVisitor();
    private SingleValueVisitor singleValueVisitor;
    private SingleValueVisitor singleTimeVisitor;

    public InsertDynamicData(List<ByteArrayInputStream> pageList, CompressionTypeName compressionName,
                             DynamicOneColumnData insertTrue, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse,
                             SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression frequencyFilter,
                             TSDataType dataType) {
        this.pageList = pageList;
        this.compressionTypeName = compressionName;
        this.insertTrue = insertTrue;
        this.updateTrue = updateTrue;
        this.updateFalse = updateFalse;
        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        this.frequencyFilter = frequencyFilter;
        this.dataType = dataType;
        if (valueFilter != null)
            this.singleValueVisitor = getSingleValueVisitorByDataType(dataType, valueFilter);
        if (timeFilter != null)
            this.singleTimeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
    }

    public void setBufferWritePageList(List<ByteArrayInputStream> pageList) {
        this.pageList = pageList;
    }

    public void setCurrentPageBuffer(DynamicOneColumnData pageBuffer) {
        this.insertTrue = pageBuffer;
    }

    public TSDataType getDataType() {
        return this.dataType;
    }

    public boolean hasInsertData() throws IOException {
        return findNext();
    }

    public long getCurrentMinTime() {
        return currentSatisfiedTime;
    }

    public int getCurrentIntValue() {
        // will not exist: currentSatisfiedTime = -1 (page list has been read all), but insertTrue still has unread timestamp
        if (currentSatisfiedTime == -1) {
            LOG.error("UnReachable!");    
        }
        
        if (insertTrue.insertTrueIndex < insertTrue.valueLength && insertTrue.getTime(insertTrue.insertTrueIndex) <= currentSatisfiedTime) {
            return insertTrue.getInt(insertTrue.insertTrueIndex);
        } else {
            return curSatisfiedIntValue;
        }
    }
    
    public boolean getCurrentBooleanValue() {
        if (insertTrue.insertTrueIndex < insertTrue.valueLength && insertTrue.getTime(insertTrue.insertTrueIndex) <= currentSatisfiedTime) {
            return insertTrue.getBoolean(insertTrue.insertTrueIndex);
        } else {
            return curSatisfiedBooleanValue;
        }
    }

    public long getCurrentLongValue() {
        if (insertTrue.insertTrueIndex < insertTrue.valueLength && insertTrue.getTime(insertTrue.insertTrueIndex) <= currentSatisfiedTime) {
            return insertTrue.getLong(insertTrue.insertTrueIndex);
        } else {
            return curSatisfiedLongValue;
        }
    }

    public float getCurrentFloatValue() {
        if (insertTrue.insertTrueIndex < insertTrue.valueLength && insertTrue.getTime(insertTrue.insertTrueIndex) <= currentSatisfiedTime) {
            return insertTrue.getFloat(insertTrue.insertTrueIndex);
        } else {
            return curSatisfiedFloatValue;
        }
    }

    public double getCurrentDoubleValue() {
        if (insertTrue.insertTrueIndex < insertTrue.valueLength && insertTrue.getTime(insertTrue.insertTrueIndex) <= currentSatisfiedTime) {
            return insertTrue.getDouble(insertTrue.insertTrueIndex);
        } else {
            return curSatisfiedDoubleValue;
        }
    }

    public Binary getCurrentBinaryValue() {
        if (insertTrue.insertTrueIndex < insertTrue.valueLength && insertTrue.getTime(insertTrue.insertTrueIndex) <= currentSatisfiedTime) {
            return insertTrue.getBinary(insertTrue.insertTrueIndex);
        } else {
            return curSatisfiedBinaryValue;
        }
    }


    /**
     * Remove current time and value, to get next time and value satisfied with the filters.
     * Must exist current time and value.
     */
    public void removeCurrentValue() throws IOException {
        if (insertTrue.insertTrueIndex < insertTrue.valueLength && insertTrue.getTime(insertTrue.insertTrueIndex) <= currentSatisfiedTime) {
            if (insertTrue.getTime(insertTrue.insertTrueIndex) < currentSatisfiedTime) {
                insertTrue.insertTrueIndex++;
                return;
            } else {
                insertTrue.insertTrueIndex++;
            }
        }

        // remove page time
        currentSatisfiedTime = -1;
        curTimeIndex++;
        if (timeValues != null && curTimeIndex >= timeValues.length) {
            pageIndex++;
            pageReader = null;
            page = null;
            curTimeIndex = 0;
        }
    }

    /**
     * Only when the current page data has been read completely, this method could be invoked.
     */
    private boolean findNext() throws IOException {
        if (currentSatisfiedTime != -1)
            return true;

        boolean pageFindFlag = false;

        // to get next page which has satisfied data
        while (!pageFindFlag) {
            if (pageList == null || (pageReader == null && pageIndex >= pageList.size()))
                break;

            if (pageReader == null) {
                pageReader = new PageReader(pageList.get(pageIndex), compressionTypeName);
                PageHeader pageHeader = pageReader.getNextPageHeader();
                Digest pageDigest = pageHeader.data_page_header.getDigest();

                // construct value filter digest
                DigestForFilter valueDigest = new DigestForFilter(pageDigest.min, pageDigest.max, dataType);
                // construct time filter digest
                long mint = pageHeader.data_page_header.min_timestamp;
                long maxt = pageHeader.data_page_header.max_timestamp;
                DigestForFilter timeDigest = new DigestForFilter(mint, maxt);
                LOG.debug("Page min time:{}, max time:{}, min value:{}, max value:{}", String.valueOf(mint),
                        String.valueOf(maxt), String.valueOf(pageDigest.bufferForMax()), pageDigest.bufferForMax().toString());

                while (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2+1) < mint) {
                    updateTrue.curIdx ++;
                }

                while (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2+1) < mint) {
                    updateFalse.curIdx ++;
                }

                // not satisfied with time filter.
                if ((timeFilter != null && !digestVisitor.satisfy(timeDigest, timeFilter))) {
                    pageReaderReset();
                    continue;
                } else {
                    // no updateTrue and updateFalse, not satisfied with valueFilter
                    if (updateTrue != null && updateTrue.curIdx >= updateTrue.valueLength && updateFalse != null && updateFalse.curIdx >= updateFalse.valueLength
                                    && valueFilter != null && !digestVisitor.satisfy(valueDigest, valueFilter)) {
                        pageReaderReset();
                        continue;
                    }
                    // has updateTrue, updateTrue not update this page and not satisfied with valueFilter
                    else if (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) >= maxt &&
                            valueFilter != null && !digestVisitor.satisfy(valueDigest, valueFilter)) {
                        pageReaderReset();
                        continue;
                    }
                    // has updateFalse and updateFalse update this page all
                    else if (updateTrue != null && updateFalse != null && updateFalse.curIdx < updateFalse.valueLength &&
                            updateFalse.getTime(updateFalse.curIdx*2) >= mint && updateFalse.getTime(updateFalse.curIdx*2+1) <= maxt) {
                        pageReaderReset();
                        continue;
                    }
                }

                page = pageReader.getNextPage();
                timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);
                curTimeIndex = 0;
                this.valueDecoder = Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType);
            }

            if (pageReader != null) {
                int unValidTimeCount = 0;

                //TODO consider time filter
                while (timeFilter != null && (curTimeIndex<timeValues.length && !singleTimeVisitor.verify(timeValues[curTimeIndex]))) {
                    curTimeIndex++;
                    unValidTimeCount++;
                }

                // all of remain time data are not satisfied with the time filter.
                if (curTimeIndex == timeValues.length) {
                    pageReader = null; // pageReader reset
                    currentSatisfiedTime = -1;
                    pageIndex++;
                    continue;
                }

                int cnt;
                switch (dataType) {
                    case INT32:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedIntValue = valueDecoder.readInt(page);
                                cnt++;
                            }

                            curSatisfiedIntValue = valueDecoder.readInt(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateTrue.curIdx ++;
                                while (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateFalse.curIdx ++;

                                // updateTrue.valueLength*2 - 1
                                if (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = timeValues[curTimeIndex];
                                    curSatisfiedIntValue = updateTrue.getInt(updateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedIntValue, valueFilter)) {
                                        currentSatisfiedTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case INT64:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedLongValue = valueDecoder.readLong(page);
                                cnt++;
                            }

                            curSatisfiedLongValue = valueDecoder.readLong(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateTrue.curIdx ++;
                                while (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateFalse.curIdx ++;

                                // updateTrue.valueLength*2 - 1
                                if (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = timeValues[curTimeIndex];
                                    curSatisfiedLongValue = updateTrue.getLong(updateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedLongValue, valueFilter)) {
                                        currentSatisfiedTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case FLOAT:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedFloatValue = valueDecoder.readFloat(page);
                                cnt++;
                            }

                            curSatisfiedFloatValue = valueDecoder.readFloat(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateTrue.curIdx ++;
                                while (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateFalse.curIdx ++;

                                // updateTrue.valueLength*2 - 1
                                if (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = timeValues[curTimeIndex];
                                    curSatisfiedFloatValue = updateTrue.getFloat(updateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedFloatValue, valueFilter)) {
                                        currentSatisfiedTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case DOUBLE:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedDoubleValue = valueDecoder.readDouble(page);
                                cnt++;
                            }

                            curSatisfiedDoubleValue = valueDecoder.readDouble(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateTrue.curIdx ++;
                                while (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateFalse.curIdx ++;

                                // updateTrue.valueLength*2 - 1
                                if (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = timeValues[curTimeIndex];
                                    curSatisfiedDoubleValue = updateTrue.getDouble(updateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedDoubleValue, valueFilter)) {
                                        currentSatisfiedTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case BOOLEAN:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedBooleanValue = valueDecoder.readBoolean(page);
                                cnt++;
                            }

                            curSatisfiedBooleanValue = valueDecoder.readBoolean(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateTrue.curIdx ++;
                                while (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateFalse.curIdx ++;

                                // updateTrue.valueLength*2 - 1
                                if (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = timeValues[curTimeIndex];
                                    curSatisfiedBooleanValue = updateTrue.getBoolean(updateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedBooleanValue, valueFilter)) {
                                        currentSatisfiedTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case TEXT:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedBinaryValue = valueDecoder.readBinary(page);
                                cnt++;
                            }

                            curSatisfiedBinaryValue = valueDecoder.readBinary(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateTrue.curIdx ++;
                                while (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    updateFalse.curIdx ++;

                                // updateTrue.valueLength*2 - 1
                                if (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = timeValues[curTimeIndex];
                                    curSatisfiedBinaryValue = updateTrue.getBinary(updateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedBinaryValue, valueFilter)) {
                                        currentSatisfiedTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    default:
                            throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
                }
            }
        }

        // insertTrue value already satisfy the time filter
        while (insertTrue != null && insertTrue.insertTrueIndex < insertTrue.valueLength) {
            while (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2+1) < insertTrue.getTime(insertTrue.insertTrueIndex))
                updateTrue.curIdx += 1;
            while (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2+1) < insertTrue.getTime(insertTrue.insertTrueIndex))
                updateFalse.curIdx += 1;

            if (updateTrue != null && updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= insertTrue.getTime(insertTrue.insertTrueIndex)) {
                currentSatisfiedTime = insertTrue.getTime(insertTrue.insertTrueIndex);
                updateNewValue();
                return true;
            } else if (updateFalse != null && updateFalse.curIdx < updateFalse.valueLength && updateFalse.getTime(updateFalse.curIdx*2) <= insertTrue.getTime(insertTrue.insertTrueIndex)) {
                insertTrue.insertTrueIndex ++;
            } else {
                if (valueFilter == null || valueSatisfied()) {
                    if (currentSatisfiedTime == -1)
                        currentSatisfiedTime = insertTrue.getTime(insertTrue.insertTrueIndex);
                    return true;
                } else {
                    insertTrue.insertTrueIndex ++;
                }
            }
        }

        return pageFindFlag;
    }

    private boolean valueSatisfied() {
        switch (dataType) {
            case INT32:
                return singleValueVisitor.satisfyObject(insertTrue.getInt(insertTrue.insertTrueIndex), valueFilter);
            case INT64:
                return singleValueVisitor.satisfyObject(insertTrue.getLong(insertTrue.insertTrueIndex), valueFilter);
            case FLOAT:
                return singleValueVisitor.satisfyObject(insertTrue.getFloat(insertTrue.insertTrueIndex), valueFilter);
            case DOUBLE:
                return singleValueVisitor.satisfyObject(insertTrue.getDouble(insertTrue.insertTrueIndex), valueFilter);
            case TEXT:
                return singleValueVisitor.satisfyObject(insertTrue.getBinary(insertTrue.insertTrueIndex), valueFilter);
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
    }

    public void pageReaderReset() {
        pageIndex++;
        pageReader = null;
        currentSatisfiedTime = -1;
    }

    private void curTimeReset() {
        currentSatisfiedTime = -1;
        curTimeIndex++;
    }

    /**
     * Reset the read status, streaming
     */
    public void readStatusReset() {
        if (pageList != null) {
            for (ByteArrayInputStream stream : pageList) {
                stream.reset();
            }
        }
        if (insertTrue != null)
            insertTrue.insertTrueIndex = 0;
        if (updateTrue != null)
            updateTrue.curIdx = 0;
        if (updateFalse != null)
            updateFalse.curIdx = 0;
        pageIndex = 0;
        pageReader = null;
        curTimeIndex = 0;
        currentSatisfiedTime = -1;
    }

    /**
     * update the value of insertTrue used updateData.
     */
    private void updateNewValue() {
        switch (dataType) {
            case INT32:
                curSatisfiedIntValue = updateTrue.getInt(updateTrue.curIdx);
                insertTrue.setInt(insertTrue.insertTrueIndex, curSatisfiedIntValue);
                break;
            case INT64:
                curSatisfiedLongValue = updateTrue.getLong(updateTrue.curIdx);
                insertTrue.setLong(insertTrue.insertTrueIndex, curSatisfiedLongValue);
                break;
            case FLOAT:
                curSatisfiedFloatValue = updateTrue.getFloat(updateTrue.curIdx);
                insertTrue.setFloat(insertTrue.insertTrueIndex, curSatisfiedFloatValue);
                break;
            case DOUBLE:
                curSatisfiedDoubleValue = updateTrue.getDouble(updateTrue.curIdx);
                insertTrue.setDouble(insertTrue.insertTrueIndex, curSatisfiedDoubleValue);
                break;
            case TEXT:
                curSatisfiedBinaryValue = updateTrue.getBinary(updateTrue.curIdx);
                insertTrue.setBinary(insertTrue.insertTrueIndex, curSatisfiedBinaryValue);
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
    }

    private SingleValueVisitor<?> getSingleValueVisitorByDataType(TSDataType type, SingleSeriesFilterExpression filter) {
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
     * Read time value from the page and return them.
     *
     * @param page
     * @param size
     * @param skip If skip is true, then return long[] which is null.
     * @throws IOException
     */
    private long[] initTimeValue(InputStream page, int size, boolean skip) throws IOException {
        long[] res = null;
        int idx = 0;

        int length = ReadWriteStreamUtils.readUnsignedVarInt(page);
        byte[] buf = new byte[length];
        int readSize = page.read(buf, 0, length);

        if (!skip) {
            ByteArrayInputStream bis = new ByteArrayInputStream(buf);
            res = new long[size];
            while (timeDecoder.hasNext(bis)) {
                res[idx++] = timeDecoder.readLong(bis);
            }
        }

        return res;
    }

    // Below are used for aggregation function
    private long rowNum = 0;
    private long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
    private int minIntValue = Integer.MAX_VALUE, maxIntValue = Integer.MIN_VALUE;
    private long minLongValue = Long.MAX_VALUE, maxLongValue = Long.MIN_VALUE;
    private float minFloatValue = Float.MAX_VALUE, maxFloatValue = Float.MIN_VALUE;
    private double minDoubleValue = Double.MIN_VALUE, maxDoubleValue = Double.MIN_VALUE;
    private Binary minBinaryValue = null, maxBinaryValue = null;

    private void calcIntAggregation() {
        minTime = Math.min(minTime, getCurrentMinTime());
        maxTime = Math.max(maxTime, getCurrentMinTime());
        minIntValue = Math.min(minIntValue, getCurrentIntValue());
        maxIntValue = Math.max(maxIntValue, getCurrentIntValue());
    }

    private void calcLongAggregation() {
        minTime = Math.min(minTime, getCurrentMinTime());
        maxTime = Math.max(maxTime, getCurrentMinTime());
        minLongValue = Math.min(minLongValue, getCurrentLongValue());
        maxLongValue = Math.max(maxLongValue, getCurrentLongValue());
    }

    private void calcFloatAggregation() {
        minTime = Math.min(minTime, getCurrentMinTime());
        maxTime = Math.max(maxTime, getCurrentMinTime());
        minFloatValue = Math.min(minFloatValue, getCurrentFloatValue());
        maxFloatValue = Math.max(maxFloatValue, getCurrentFloatValue());
    }

    private void calcDoubleAggregation() {
        minTime = Math.min(minTime, getCurrentMinTime());
        maxTime = Math.max(maxTime, getCurrentMinTime());
        minDoubleValue = Math.min(minDoubleValue, getCurrentDoubleValue());
        maxDoubleValue = Math.max(maxDoubleValue, getCurrentDoubleValue());
    }

    private void calcTextAggregation() {
        minTime = Math.min(minTime, getCurrentMinTime());
        maxTime = Math.max(maxTime, getCurrentMinTime());
        if (minBinaryValue == null) {
            minBinaryValue = getCurrentBinaryValue();
        }
        if (maxBinaryValue == null) {
            maxBinaryValue = getCurrentBinaryValue();
        }
        if (getCurrentBinaryValue().compareTo(minBinaryValue) < 0) {
            minBinaryValue = getCurrentBinaryValue();
        }
        if (getCurrentBinaryValue().compareTo(maxBinaryValue) > 0) {
            maxBinaryValue = getCurrentBinaryValue();
        }
    }

    public Pair<Long, Object> calcAggregation(String aggType) throws IOException {
        readStatusReset();
        rowNum = 0;
        minTime = Long.MAX_VALUE;
        maxTime = Long.MIN_VALUE;
        minIntValue = Integer.MAX_VALUE;
        maxIntValue = Integer.MIN_VALUE;
        minLongValue = Long.MAX_VALUE;
        maxLongValue = Long.MIN_VALUE;
        minFloatValue = Float.MAX_VALUE;
        maxFloatValue = Float.MIN_VALUE;
        minDoubleValue = Double.MIN_VALUE;
        maxDoubleValue = Double.MIN_VALUE;
        minBinaryValue = null;
        maxBinaryValue = null;

        while (hasInsertData()) {
            switch (dataType) {
                case INT32:
                    rowNum++;
                    calcIntAggregation();
                    removeCurrentValue();
                    break;
                case INT64:
                    rowNum++;
                    calcLongAggregation();
                    removeCurrentValue();
                    break;
                case FLOAT:
                    rowNum++;
                    calcFloatAggregation();
                    removeCurrentValue();
                    break;
                case DOUBLE:
                    rowNum++;
                    calcDoubleAggregation();
                    removeCurrentValue();
                    break;
                case TEXT:
                    rowNum++;
                    calcTextAggregation();
                    removeCurrentValue();
                    break;
                default:
                    LOG.error("Aggregation Error!");
                    throw new UnSupportedDataTypeException("UnSupported" + dataType);
            }
        }

        switch (aggType) {
            case "COUNT":
                return new Pair<>(rowNum, rowNum);
            case "MIN_TIME":
                return new Pair<>(rowNum, minTime);
            case "MAX_TIME":
                return new Pair<>(rowNum, maxTime);
            case "MIN_VALUE":
                switch (dataType) {
                    case INT32:
                        return new Pair<>(rowNum, minIntValue);
                    case INT64:
                        return new Pair<>(rowNum, minLongValue);
                    case FLOAT:
                        return new Pair<>(rowNum, minFloatValue);
                    case DOUBLE:
                        return new Pair<>(rowNum, minDoubleValue);
                    case TEXT:
                        return new Pair<>(rowNum, minBinaryValue);
                    default:
                        LOG.error("Aggregation Error!");
                        throw new UnSupportedDataTypeException("UnSupported datatype: " + dataType);

                }
            case "MAX_VALUE":
                // System.out.println(maxIntValue + ">>>" + maxLongValue + ">>>" + maxFloatValue + ">>>" + maxDoubleValue);
                switch (dataType) {
                    case INT32:
                        return new Pair<>(rowNum, maxIntValue);
                    case INT64:
                        return new Pair<>(rowNum, maxLongValue);
                    case FLOAT:
                        return new Pair<>(rowNum, maxFloatValue);
                    case DOUBLE:
                        return new Pair<>(rowNum, maxDoubleValue);
                    case TEXT:
                        return new Pair<>(rowNum, maxBinaryValue);
                    default:
                        LOG.error("Aggregation Error!");
                        throw new UnSupportedDataTypeException("UnSupported datatype: " + dataType);
                }
            default:
                return null;
        }
    }
}
