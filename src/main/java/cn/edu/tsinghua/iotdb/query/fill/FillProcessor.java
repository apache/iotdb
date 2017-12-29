package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.UnSupportedFillTypeException;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.reader.ReaderUtils;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class contains the Fill process method.
 */
public class FillProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FillProcessor.class);

    /**
     * Return false if we haven't get the correct previous value before queryTime.
     *
     * @param result fill result data
     * @param valueReader stores data of series in a rowgroup
     * @param beforeTime before time
     * @param queryTime query time
     * @param timeFilter time filter
     * @param updateTrue overflow update operation
     * @return false if we haven't get the correct previous value before queryTime.
     * @throws IOException TsFile read error
     */
    public static boolean getPreviousFillResultInFile(DynamicOneColumnData result, ValueReader valueReader,
                                                      long beforeTime, long queryTime, SingleSeriesFilterExpression timeFilter,
                                                      DynamicOneColumnData updateTrue)
            throws IOException {

        if (beforeTime > valueReader.getEndTime()) {
            LOG.debug(String.format("previous fill, current series time digest[%s,%s] is not satisfied with the fill range[%s,%s]",
                    valueReader.getStartTime(), valueReader.getEndTime(), beforeTime, queryTime));
            return false;
        }

        IntervalTimeVisitor intervalTimeVisitor = new IntervalTimeVisitor();
        if (timeFilter != null && !intervalTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            return false;
        }

        TSDataType dataType = valueReader.getDataType();
        CompressionTypeName compressionTypeName = valueReader.compressionTypeName;

        long offset = valueReader.getFileOffset();
        while ((offset - valueReader.getFileOffset()) < valueReader.totalSize) {
            ByteArrayInputStream bis = valueReader.initBAISForOnePage(offset);
            long lastAvailable = bis.available();

            PageReader pageReader = new PageReader(bis, compressionTypeName);
            PageHeader pageHeader = pageReader.getNextPageHeader();

            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;

            if (beforeTime > pageMaxTime) {
                pageReader.skipCurrentPage();
                offset += lastAvailable - bis.available();
                continue;
            }

            InputStream page = pageReader.getNextPage();
            offset += lastAvailable - bis.available();
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));
            long[] timestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            int timeIdx = 0;

            SingleValueVisitor singleValueVisitor = null;
            if (timeFilter != null) {
                singleValueVisitor = new SingleValueVisitor(timeFilter);
            }
            switch (dataType) {
                case INT32:
                    while (valueReader.decoder.hasNext(page)) {
                        long curTime = timestamps[timeIdx];
                        timeIdx++;

                        int v = valueReader.decoder.readInt(page);

                        // this branch need to be covered by test case for overflow delete operation
                        if (timeFilter != null && !singleValueVisitor.verify(curTime)) {
                            continue;
                        }

                        if (curTime >= beforeTime && curTime <= queryTime) {

                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < curTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= curTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= curTime) {
                                v = updateTrue.getInt(updateTrue.curIdx);
                            }

                            if (result.timeLength == 0) {
                                result.putTime(curTime);
                                result.putInt(v);
                            } else {
                                result.setTime(0, curTime);
                                result.setInt(0, v);
                            }

                            if (curTime == queryTime) {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                    break;
                case INT64:
                    while (valueReader.decoder.hasNext(page)) {
                        long curTime = timestamps[timeIdx];
                        timeIdx++;

                        long v = valueReader.decoder.readLong(page);

                        if (timeFilter != null && !singleValueVisitor.verify(curTime)) {
                            continue;
                        }

                        if (curTime >= beforeTime && curTime <= queryTime) {

                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < curTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= curTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= curTime) {
                                v = updateTrue.getLong(updateTrue.curIdx);
                            }

                            if (result.timeLength == 0) {
                                result.putTime(curTime);
                                result.putLong(v);
                            } else {
                                result.setTime(0, curTime);
                                result.setLong(0, v);
                            }

                            if (curTime == queryTime) {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                    break;
                case FLOAT:
                    while (valueReader.decoder.hasNext(page)) {
                        long curTime = timestamps[timeIdx];
                        timeIdx++;

                        float v = valueReader.decoder.readFloat(page);

                        if (timeFilter != null && !singleValueVisitor.verify(curTime)) {
                            continue;
                        }

                        if (curTime >= beforeTime && curTime <= queryTime) {

                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < curTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= curTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= curTime) {
                                v = updateTrue.getFloat(updateTrue.curIdx);
                            }

                            if (result.timeLength == 0) {
                                result.putTime(curTime);
                                result.putFloat(v);
                            } else {
                                result.setTime(0, curTime);
                                result.setFloat(0, v);
                            }

                            if (curTime == queryTime) {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                    break;
                case DOUBLE:
                    while (valueReader.decoder.hasNext(page)) {
                        long curTime = timestamps[timeIdx];
                        timeIdx++;

                        double v = valueReader.decoder.readDouble(page);

                        if (timeFilter != null && !singleValueVisitor.verify(curTime)) {
                            continue;
                        }

                        if (curTime >= beforeTime && curTime <= queryTime) {

                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < curTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= curTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= curTime) {
                                v = updateTrue.getDouble(updateTrue.curIdx);
                            }

                            if (result.timeLength == 0) {
                                result.putTime(curTime);
                                result.putDouble(v);
                            } else {
                                result.setTime(0, curTime);
                                result.setDouble(0, v);
                            }

                            if (curTime == queryTime) {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                    break;
                case BOOLEAN:
                    while (valueReader.decoder.hasNext(page)) {
                        long curTime = timestamps[timeIdx];
                        timeIdx++;

                        boolean v = valueReader.decoder.readBoolean(page);

                        if (timeFilter != null && !singleValueVisitor.verify(curTime)) {
                            continue;
                        }

                        if (curTime >= beforeTime && curTime <= queryTime) {

                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < curTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= curTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= curTime) {
                                v = updateTrue.getBoolean(updateTrue.curIdx);
                            }

                            if (result.timeLength == 0) {
                                result.putTime(curTime);
                                result.putBoolean(v);
                            } else {
                                result.setTime(0, curTime);
                                result.setBoolean(0, v);
                            }

                            if (curTime == queryTime) {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                    break;
                case TEXT:
                    while (valueReader.decoder.hasNext(page)) {
                        long curTime = timestamps[timeIdx];
                        timeIdx++;

                        Binary v = valueReader.decoder.readBinary(page);

                        if (timeFilter != null && !singleValueVisitor.verify(curTime)) {
                            continue;
                        }

                        if (curTime >= beforeTime && curTime <= queryTime) {

                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < curTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= curTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= curTime) {
                                v = updateTrue.getBinary(updateTrue.curIdx);
                            }

                            if (result.timeLength == 0) {
                                result.putTime(curTime);
                                result.putBinary(v);
                            } else {
                                result.setTime(0, curTime);
                                result.setBinary(0, v);
                            }

                            if (curTime == queryTime) {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                    break;
                default:
                    LOG.error("Unsupported previous fill data type : " + result.dataType);
                    throw new UnSupportedFillTypeException("Unsupported previous fill data type : " + result.dataType);
            }

        }

        return false;
    }

    /**
     * Get the previous  fill result using the data in <code>InsertDynamicData</code>
     *
     * @param result previous fill result
     * @param insertMemoryData see <code>InsertDynamicData</code>
     * @param beforeTime fill before time
     * @param queryTime fill query time
     * @throws IOException file stream read error
     */
    public static void getPreviousFillResultInMemory(DynamicOneColumnData result, InsertDynamicData insertMemoryData,
                                                        long beforeTime, long queryTime)
            throws IOException {

        while (insertMemoryData.hasInsertData()) {
            long currentTime = insertMemoryData.getCurrentMinTime();
            if (currentTime > queryTime) {
                break;
            }
            if (currentTime >= beforeTime && currentTime <= queryTime) {
                switch (result.dataType) {
                    case INT32:
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putInt(insertMemoryData.getCurrentIntValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setInt(0, insertMemoryData.getCurrentIntValue());
                            }
                        }
                        break;
                    case INT64:
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putLong(insertMemoryData.getCurrentLongValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setLong(0, insertMemoryData.getCurrentLongValue());
                            }
                        }
                        break;
                    case FLOAT:
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putFloat(insertMemoryData.getCurrentFloatValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setFloat(0, insertMemoryData.getCurrentFloatValue());
                            }
                        }
                        break;
                    case DOUBLE:
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putDouble(insertMemoryData.getCurrentDoubleValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setDouble(0, insertMemoryData.getCurrentDoubleValue());
                            }
                        }
                        break;
                    case BOOLEAN:
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putBoolean(insertMemoryData.getCurrentBooleanValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setBoolean(0, insertMemoryData.getCurrentBooleanValue());
                            }
                        }
                        break;
                    case TEXT:
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putBinary(insertMemoryData.getCurrentBinaryValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setBinary(0, insertMemoryData.getCurrentBinaryValue());
                            }
                        }
                        break;
                    default:
                        LOG.error("Unsupported previous fill data type : " + result.dataType);
                        throw new UnSupportedFillTypeException("Unsupported previous fill data type : " + result.dataType);
                }
            }
            insertMemoryData.removeCurrentValue();
        }
    }

    /**
     * Return true if we has get the necessary linear values, only when one of the situations occur below.
     * 1) got exactly a value equals queryTime,
     * 2) got two values, a value before queryTime and a value after queryTime.
     * 3) the valueReader time range is not satisfied with queryTime, beforeRange and afterRange.
     *
     * <p> If the function returns true, the <code>RowGroupReader</code> traverse invoked in {@code RecordReader.getLinearFillResult}
     * is over.
     *
     * @param result linear fill result
     * @param valueReader stores the data of series in a rowgroup
     * @param beforeTime linear fill begin time
     * @param queryTime linear fill begin time
     * @param afterTime linear fill begin time
     * @param timeFilter time filter
     * @param updateTrue overflow update operation
     * @return see above
     * @throws IOException TsFile read error
     */
    public static boolean getLinearFillResultInFile(DynamicOneColumnData result, ValueReader valueReader,
                                                    long beforeTime, long queryTime, long afterTime, SingleSeriesFilterExpression timeFilter,
                                                    DynamicOneColumnData updateTrue)
            throws IOException {

        if (beforeTime > valueReader.getEndTime()) {
            LOG.debug(String.format("Linear fill, current series time digest[%s,%s] is not satisfied with the fill range[%s,%s]",
                    valueReader.getStartTime(), valueReader.getEndTime(), beforeTime, afterTime));
            return false;
        }
        if (afterTime < valueReader.getStartTime()) {
            return true;
        }
        IntervalTimeVisitor intervalTimeVisitor = new IntervalTimeVisitor();
        if (timeFilter != null && !intervalTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            return false;
        }

        TSDataType dataType = valueReader.getDataType();
        CompressionTypeName compressionTypeName = valueReader.compressionTypeName;

        long offset = valueReader.getFileOffset();
        while ((offset - valueReader.getFileOffset()) < valueReader.totalSize) {
            ByteArrayInputStream bis = valueReader.initBAISForOnePage(offset);
            long lastAvailable = bis.available();

            PageReader pageReader = new PageReader(bis, compressionTypeName);
            PageHeader pageHeader = pageReader.getNextPageHeader();

            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;

            // TODO test case covered
            if (beforeTime > pageMaxTime) {
                pageReader.skipCurrentPage();
                offset += lastAvailable - bis.available();
                continue;
            }
            if (afterTime < pageMinTime) {
                return true;
            }

            InputStream page = pageReader.getNextPage();
            offset += lastAvailable - bis.available();
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));
            long[] timestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            int timeIdx = 0;

            SingleValueVisitor singleValueVisitor = null;
            if (timeFilter != null) {
                singleValueVisitor = new SingleValueVisitor(timeFilter);
            }
            switch (dataType) {
                case INT32:
                    while (valueReader.decoder.hasNext(page)) {
                        long currentTime = timestamps[timeIdx];
                        timeIdx++;

                        int v = valueReader.decoder.readInt(page);

                        // TODO this branch need to be covered by test case for overflow delete operation
                        if (timeFilter != null && !singleValueVisitor.verify(currentTime)) {
                            continue;
                        }

                        if (currentTime >= beforeTime && currentTime <= queryTime) {
                            // TODO this branch need to be covered by test case
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < currentTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= currentTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= currentTime) {
                                v = updateTrue.getInt(updateTrue.curIdx);
                            }

                            if (result.timeLength == 0) {
                                result.putTime(currentTime);
                                result.putInt(v);
                            } else {
                                result.setTime(0, currentTime);
                                result.setInt(0, v);
                            }

                            if (currentTime == queryTime) {
                                return true;
                            }
                        } else if (currentTime > queryTime){
                            // TODO this branch need to be covered by test case
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < currentTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= currentTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= currentTime) {
                                v = updateTrue.getInt(updateTrue.curIdx);
                            }

                            if (result.timeLength <= 1) {
                                result.putTime(currentTime);
                                result.putInt(v);
                            } else  {
                                LOG.error("linear fill unreachable");
                            }
                            return true;
                        }
                    }
                    break;
                case INT64:
                    while (valueReader.decoder.hasNext(page)) {
                        long currentTime = timestamps[timeIdx];
                        timeIdx++;

                        long v = valueReader.decoder.readLong(page);

                        if (timeFilter != null && !singleValueVisitor.verify(currentTime)) {
                            continue;
                        }

                        if (currentTime >= beforeTime && currentTime <= queryTime) {
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < currentTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= currentTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= currentTime) {
                                v = updateTrue.getLong(updateTrue.curIdx);
                            }
                            if (result.timeLength == 0) {
                                result.putTime(currentTime);
                                result.putLong(v);
                            } else {
                                result.setTime(0, currentTime);
                                result.setLong(0, v);
                            }

                            if (currentTime == queryTime) {
                                return true;
                            }
                        } else if (currentTime > queryTime){
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < currentTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= currentTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= currentTime) {
                                v = updateTrue.getLong(updateTrue.curIdx);
                            }

                            if (result.timeLength <= 1) {
                                result.putTime(currentTime);
                                result.putLong(v);
                            } else  {
                                LOG.error("linear fill unreachable");
                            }
                            return true;
                        }
                    }
                    break;
                case FLOAT:
                    while (valueReader.decoder.hasNext(page)) {
                        long currentTime = timestamps[timeIdx];
                        timeIdx++;

                        float v = valueReader.decoder.readFloat(page);

                        if (timeFilter != null && !singleValueVisitor.verify(currentTime)) {
                            continue;
                        }

                        if (currentTime >= beforeTime && currentTime <= queryTime) {
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < currentTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= currentTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= currentTime) {
                                v = updateTrue.getFloat(updateTrue.curIdx);
                            }
                            if (result.timeLength == 0) {
                                result.putTime(currentTime);
                                result.putFloat(v);
                            } else {
                                result.setTime(0, currentTime);
                                result.setFloat(0, v);
                            }

                            if (currentTime == queryTime) {
                                return true;
                            }
                        } else if (currentTime > queryTime){
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < currentTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= currentTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= currentTime) {
                                v = updateTrue.getFloat(updateTrue.curIdx);
                            }

                            if (result.timeLength <= 1) {
                                result.putTime(currentTime);
                                result.putFloat(v);
                            } else  {
                                LOG.error("linear fill unreachable");
                            }
                            return true;
                        }
                    }
                    break;
                case DOUBLE:
                    while (valueReader.decoder.hasNext(page)) {
                        long currentTime = timestamps[timeIdx];
                        timeIdx++;

                        double v = valueReader.decoder.readDouble(page);

                        if (timeFilter != null && !singleValueVisitor.verify(currentTime)) {
                            continue;
                        }

                        if (currentTime >= beforeTime && currentTime <= queryTime) {
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < currentTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= currentTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= currentTime) {
                                v = updateTrue.getDouble(updateTrue.curIdx);
                            }
                            if (result.timeLength == 0) {
                                result.putTime(currentTime);
                                result.putDouble(v);
                            } else {
                                result.setTime(0, currentTime);
                                result.setDouble(0, v);
                            }

                            if (currentTime == queryTime) {
                                return true;
                            }
                        } else if (currentTime > queryTime){
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < currentTime) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= currentTime
                                    && updateTrue.getTime(updateTrue.curIdx*2+1) >= currentTime) {
                                v = updateTrue.getDouble(updateTrue.curIdx);
                            }

                            if (result.timeLength <= 1) {
                                result.putTime(currentTime);
                                result.putDouble(v);
                            } else  {
                                LOG.error("linear fill unreachable");
                            }
                            return true;
                        }
                    }
                    break;
                default:
                    LOG.error("Unsupported linear fill data type : " + dataType);
                    throw new UnSupportedFillTypeException("Unsupported linear fill data type : " + dataType);
            }

        }

        return false;
    }

    /**
     * Get the linear fill result using the data in <code>InsertDynamicData</code>
     *
     * @param result result data
     * @param insertMemoryData see <code>InsertDynamicData</code>
     * @param beforeTime linear fill before time
     * @param queryTime linear fill query time
     * @param afterTime linear fill after time
     * @throws IOException file stream read error
     */
    public static void getLinearFillResultInMemory(DynamicOneColumnData result, InsertDynamicData insertMemoryData,
                                                        long beforeTime, long queryTime, long afterTime)
            throws IOException {

        while (insertMemoryData.hasInsertData()) {
            long currentTime = insertMemoryData.getCurrentMinTime();

            if (currentTime > afterTime) {
                return;
            }

            switch (result.dataType) {
                case INT32:
                    if (currentTime >= beforeTime && currentTime < queryTime) {
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putInt(insertMemoryData.getCurrentIntValue());
                        } else if (result.timeLength == 1){
                            long existTime = result.getTime(0);
                            // TODO existTime == time is a special situation, need test covered
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setInt(0, insertMemoryData.getCurrentIntValue());
                            } else if (existTime > queryTime) {
                                result.putTime(result.getTime(0));
                                result.putInt(result.getInt(0));
                                result.setTime(0, currentTime);
                                result.setInt(0, insertMemoryData.getCurrentIntValue());
                            }
                        } else if (result.timeLength == 2) {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setInt(0, insertMemoryData.getCurrentIntValue());
                            }
                        } else {
                            LOG.error("Linear fill unreachable!");
                        }
                    } else if (currentTime == queryTime) {
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putInt(insertMemoryData.getCurrentIntValue());
                        } else {
                            result.timeLength = result.valueLength = 1;
                            result.setTime(0, currentTime);
                            result.setInt(0, insertMemoryData.getCurrentIntValue());
                        }

                        // get the time equals queryTime, end this function
                        return;
                    } else if (currentTime > queryTime) {
                        if (result.timeLength == 0) {
                            return;
                        } else if (result.timeLength == 1) {
                            long existTime = result.getTime(0);
                            if (existTime < queryTime) {
                                result.putTime(currentTime);
                                result.putInt(insertMemoryData.getCurrentIntValue());
                                return;
                            } else {
                                return;
                            }
                        } else if (result.timeLength == 2) {
                            long existTime = result.getTime(1);

                            if (currentTime <= existTime) {
                                result.setTime(1, existTime);
                                result.setInt(1, insertMemoryData.getCurrentIntValue());
                            } else if (currentTime <= queryTime) {
                                LOG.error("linear fill unreachable");
                            }
                            return;

                        } else {
                            LOG.error("Linear fill unreachable!");
                        }
                        return;
                    }
                    break;
                case INT64:
                    if (currentTime >= beforeTime && currentTime < queryTime) {
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putLong(insertMemoryData.getCurrentLongValue());
                        } else if (result.timeLength == 1){
                            long existTime = result.getTime(0);
                            // TODO existTime == time is a special situation, need test covered
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setLong(0, insertMemoryData.getCurrentLongValue());
                            } else if (existTime > queryTime) {
                                result.putTime(result.getTime(0));
                                result.putLong(result.getLong(0));
                                result.setTime(0, currentTime);
                                result.setLong(0, insertMemoryData.getCurrentLongValue());
                            }
                        } else if (result.timeLength == 2) {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setLong(0, insertMemoryData.getCurrentLongValue());
                            }
                        } else {
                            LOG.error("Linear fill unreachable!");
                        }
                    } else if (currentTime == queryTime) {
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putLong(insertMemoryData.getCurrentLongValue());
                        } else {
                            result.timeLength = result.valueLength = 1;
                            result.setTime(0, currentTime);
                            result.setLong(0, insertMemoryData.getCurrentLongValue());
                        }

                        // get the time equals queryTime, end this function
                        return;
                    } else if (currentTime > queryTime) {
                        if (result.timeLength == 0) {
                            return;
                        } else if (result.timeLength == 1) {
                            long existTime = result.getTime(0);
                            if (existTime < queryTime) {
                                result.putTime(currentTime);
                                result.putLong(insertMemoryData.getCurrentLongValue());
                                return;
                            } else {
                                return;
                            }
                        } else if (result.timeLength == 2) {
                            long existTime = result.getTime(1);

                            if (currentTime <= existTime) {
                                result.setTime(1, existTime);
                                result.setLong(1, insertMemoryData.getCurrentLongValue());
                            } else if (currentTime <= queryTime) {
                                LOG.error("linear fill unreachable");
                            }
                            return;

                        } else {
                            LOG.error("Linear fill unreachable!");
                        }
                        return;
                    }
                    break;
                case FLOAT:
                    if (currentTime >= beforeTime && currentTime < queryTime) {
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putFloat(insertMemoryData.getCurrentFloatValue());
                        } else if (result.timeLength == 1){
                            long existTime = result.getTime(0);
                            // TODO existTime == time is a special situation, need test covered
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setFloat(0, insertMemoryData.getCurrentFloatValue());
                            } else if (existTime > queryTime) {
                                result.putTime(result.getTime(0));
                                result.putFloat(result.getFloat(0));
                                result.setTime(0, currentTime);
                                result.setFloat(0, insertMemoryData.getCurrentFloatValue());
                            }
                        } else if (result.timeLength == 2) {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setFloat(0, insertMemoryData.getCurrentFloatValue());
                            }
                        } else {
                            LOG.error("Linear fill unreachable!");
                        }
                    } else if (currentTime == queryTime) {
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putFloat(insertMemoryData.getCurrentFloatValue());
                        } else {
                            result.timeLength = result.valueLength = 1;
                            result.setTime(0, currentTime);
                            result.setFloat(0, insertMemoryData.getCurrentFloatValue());
                        }

                        // get the time equals queryTime, end this function
                        return;
                    } else if (currentTime > queryTime) {
                        if (result.timeLength == 0) {
                            return;
                        } else if (result.timeLength == 1) {
                            long existTime = result.getTime(0);
                            if (existTime < queryTime) {
                                result.putTime(currentTime);
                                result.putFloat(insertMemoryData.getCurrentFloatValue());
                                return;
                            } else {
                                return;
                            }
                        } else if (result.timeLength == 2) {
                            long existTime = result.getTime(1);

                            if (currentTime <= existTime) {
                                result.setTime(1, existTime);
                                result.setFloat(1, insertMemoryData.getCurrentFloatValue());
                            } else if (currentTime <= queryTime) {
                                LOG.error("linear fill unreachable");
                            }
                            return;

                        } else {
                            LOG.error("Linear fill unreachable!");
                        }
                        return;
                    }
                    break;
                case DOUBLE:
                    if (currentTime >= beforeTime && currentTime < queryTime) {
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putDouble(insertMemoryData.getCurrentDoubleValue());
                        } else if (result.timeLength == 1){
                            long existTime = result.getTime(0);
                            // TODO existTime == time is a special situation, need test covered
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setDouble(0, insertMemoryData.getCurrentDoubleValue());
                            } else if (existTime > queryTime) {
                                result.putTime(result.getTime(0));
                                result.putDouble(result.getDouble(0));
                                result.setTime(0, currentTime);
                                result.setDouble(0, insertMemoryData.getCurrentDoubleValue());
                            }
                        } else if (result.timeLength == 2) {
                            long existTime = result.getTime(0);
                            if (existTime <= currentTime) {
                                result.setTime(0, currentTime);
                                result.setDouble(0, insertMemoryData.getCurrentDoubleValue());
                            }
                        } else {
                            LOG.error("Linear fill unreachable!");
                        }
                    } else if (currentTime == queryTime) {
                        if (result.timeLength == 0) {
                            result.putTime(currentTime);
                            result.putDouble(insertMemoryData.getCurrentDoubleValue());
                        } else {
                            result.timeLength = result.valueLength = 1;
                            result.setTime(0, currentTime);
                            result.setDouble(0, insertMemoryData.getCurrentDoubleValue());
                        }

                        // get the time equals queryTime, end this function
                        return;
                    } else if (currentTime > queryTime) {
                        if (result.timeLength == 0) {
                            return;
                        } else if (result.timeLength == 1) {
                            long existTime = result.getTime(0);
                            if (existTime < queryTime) {
                                result.putTime(currentTime);
                                result.putDouble(insertMemoryData.getCurrentDoubleValue());
                                return;
                            } else {
                                return;
                            }
                        } else if (result.timeLength == 2) {
                            long existTime = result.getTime(1);

                            if (currentTime <= existTime) {
                                result.setTime(1, existTime);
                                result.setDouble(1, insertMemoryData.getCurrentDoubleValue());
                            } else if (currentTime <= queryTime) {
                                LOG.error("linear fill unreachable");
                            }
                            return;

                        } else {
                            LOG.error("Linear fill unreachable!");
                        }
                        return;
                    }
                    break;
                default:
                    LOG.error("Unsupported linear fill data type : " + result.dataType);
                    throw new UnSupportedFillTypeException("Unsupported linear fill data type : " + result.dataType);
            }

            insertMemoryData.removeCurrentValue();
        }
    }
}
