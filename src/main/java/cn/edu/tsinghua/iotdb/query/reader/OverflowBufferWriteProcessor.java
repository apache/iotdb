package cn.edu.tsinghua.iotdb.query.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import cn.edu.tsinghua.iotdb.query.visitorImpl.PageAllSatisfiedVisitor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationResult;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class OverflowBufferWriteProcessor{

    private static final Logger LOG = LoggerFactory.getLogger(OverflowBufferWriteProcessor.class);

    // private
//    OverflowBufferWriteProcessor(long offset, long totalSize, TSDataType dataType, TsDigest digest,
//                                 ITsRandomAccessFileReader raf, List<String> enumValues, CompressionTypeName compressionTypeName,
//                                 long rowNums) {
//        super(offset, totalSize, dataType, digest, raf, enumValues, compressionTypeName, rowNums);
//    }

//    private ByteArrayInputStream initBAISForOnePage(long pageOffset) throws IOException {
//        int length = (int) (this.totalSize - (pageOffset - fileOffset));
//        // int length = (int) (this.totalSize + fileOffset - valueOffset);
//        byte[] buf = new byte[length]; // warning
//        int readSize = 0;
//        raf.seek(pageOffset);
//        readSize = raf.read(buf, 0, length);
//        if (readSize != length) {
//            throw new IOException("Expect byte size : " + length + ". Read size : " + readSize);
//        }
//
//        return new ByteArrayInputStream(buf);
//    }

    static DynamicOneColumnData getValuesWithOverFlow(ValueReader valueReader, DynamicOneColumnData updateTrueData, DynamicOneColumnData updateFalseData,
                                               InsertDynamicData insertMemoryData, SingleSeriesFilterExpression timeFilter,
                                               SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                               DynamicOneColumnData res, int fetchSize) throws IOException {

        TSDataType dataType = valueReader.getDataType();
        CompressionTypeName compressionTypeName = valueReader.compressionTypeName;

        if (res == null) {
            res = new DynamicOneColumnData(dataType, true);
            res.pageOffset = valueReader.getFileOffset();
            res.leftSize = valueReader.getTotalSize();
            res.insertTrueIndex = 0;
        }

        // IMPORTANT!!
        if (res.pageOffset == -1) {
            res.pageOffset = valueReader.getFileOffset();
        }

        TsDigest digest = valueReader.getDigest();
        DigestForFilter digestFF = new DigestForFilter(digest.min, digest.max, dataType);
        LOG.debug("read one series normally, digest min and max is: " + digestFF.getMinValue() + " --- " + digestFF.getMaxValue());
        DigestVisitor digestVisitor = new DigestVisitor();
        // If not satisfied, return res with size equal to 0

        // TODO: optimize
        updateTrueData = (updateTrueData == null ? new DynamicOneColumnData(dataType, true) : updateTrueData);
        updateFalseData = (updateFalseData == null ? new DynamicOneColumnData(dataType, true) : updateFalseData);

        if (updateTrueData.valueLength == 0 && !insertMemoryData.hasInsertData() && valueFilter != null
                && !digestVisitor.satisfy(digestFF, valueFilter)) {
            return res;
        }

        DynamicOneColumnData[] updateData = new DynamicOneColumnData[2];
        updateData[0] = updateTrueData;
        updateData[1] = updateFalseData;
        int[] updateIdx = new int[]{updateTrueData.curIdx, updateFalseData.curIdx};

        int mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateTrueData, updateFalseData);

        // initial one page from file
        ByteArrayInputStream bis = valueReader.initBAISForOnePage(res.pageOffset);
        PageReader pageReader = new PageReader(bis, compressionTypeName);
        int pageCount = 0;
        // let resCount be the sum of records in last read
        // in BatchReadRecordGenerator, The ResCount needed equals to (res.valueLength - res.curIdx)??
        int resCount = res.valueLength - res.curIdx;

        // some variables for frequency calculation with overflow
        boolean hasOverflowDataInThisPage = false;

        while ((res.pageOffset - valueReader.fileOffset) < valueReader.totalSize && resCount < fetchSize) {
            // To help to record byte size in this process of read.
            int lastAvailable = bis.available();
            pageCount++;
            LOG.debug("read page {}, offset : {}", pageCount, res.pageOffset);
            PageHeader pageHeader = pageReader.getNextPageHeader();

            // construct valueFilter
            // System.out.println(res.pageOffset + "|" + fileOffset + "|" + totalSize);
            Digest pageDigest = pageHeader.data_page_header.getDigest();
            DigestForFilter valueDigestFF = new DigestForFilter(pageDigest.min, pageDigest.max, dataType);

            // construct timeFilter
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);

            // find first interval , skip some intervals that not available
            while (mode != -1 && updateData[mode].getTime(updateIdx[mode] + 1) < mint) {
                updateIdx[mode] += 2;
                mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateTrueData, updateFalseData);
            }

            if (mode == -1 && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }
            if (mode == 0 && updateData[0].getTime(updateIdx[0]) > maxt
                    && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }
            if (mode == 1 && ((updateData[1].getTime(updateIdx[1]) <= mint && updateData[1].getTime(updateIdx[1] + 1) >= maxt)
                    || ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter))))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }

            // start traverse the hole page
            InputStream page = pageReader.getNextPage();
            // update current res's pageOffset to the start of next page.
            res.pageOffset += lastAvailable - bis.available();

            hasOverflowDataInThisPage = checkDataChanged(mint, maxt, updateTrueData, updateIdx[0], updateFalseData, updateIdx[1],
                    insertMemoryData, timeFilter);

            long[] timeValues = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);

            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));

            // record the length of this res before the new records in this page
            // were put in.
            int resPreviousLength = res.valueLength;

            SingleValueVisitor<?> timeVisitor = null;
            if (timeFilter != null) {
                timeVisitor = valueReader.getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
            }
            SingleValueVisitor<?> valueVisitor = null;
            if (valueFilter != null) {
                valueVisitor = valueReader.getSingleValueVisitorByDataType(dataType, valueFilter);
            }

            try {
                int timeIdx = 0;
                switch (dataType) {
                    case INT32:
                        while (valueReader.decoder.hasNext(page)) {
                            // put insert points that less than or equals to current
                            // timestamp in page.
                            while (insertMemoryData.hasInsertData() && timeIdx < timeValues.length
                                    && insertMemoryData.getCurrentMinTime() <= timeValues[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putInt(insertMemoryData.getCurrentIntValue());
                                res.insertTrueIndex++;
                                resCount++;

                                if (insertMemoryData.getCurrentMinTime() == timeValues[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                    valueReader.decoder.readInt(page);
                                    if (!valueReader.decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (!valueReader.decoder.hasNext(page)) {
                                break;
                            }
                            int v = valueReader.decoder.readInt(page);
                            if (mode == -1) {
                                if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putInt(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 0) {
                                if (updateData[0].getTime(updateIdx[0]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[0].getTime(updateIdx[0] + 1)) {
                                    // update the value
                                    if (timeFilter == null
                                            || timeVisitor.verify(timeValues[timeIdx])) {
                                        res.putInt(updateData[0].getInt(updateIdx[0] / 2));
                                        res.putTime(timeValues[timeIdx]);
                                        resCount++;
                                    }
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putInt(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 1) {
                                if (updateData[1].getTime(updateIdx[1]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[1].getTime(updateIdx[1] + 1)) {
                                    // do nothing
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putInt(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            // set the interval to next position that current time
                            // in page maybe be included.
                            while (mode != -1 && timeIdx < timeValues.length
                                    && timeValues[timeIdx] > updateData[mode].getTime(updateIdx[mode] + 1)) {
                                updateIdx[mode] += 2;
                                mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case BOOLEAN:
                        while (valueReader.decoder.hasNext(page)) {
                            // put insert points
                            while (insertMemoryData.hasInsertData() && timeIdx < timeValues.length
                                    && insertMemoryData.getCurrentMinTime() <= timeValues[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putBoolean(insertMemoryData.getCurrentBooleanValue());
                                res.insertTrueIndex++;
                                resCount++;

                                if (insertMemoryData.getCurrentMinTime() == timeValues[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                    valueReader.decoder.readBoolean(page);
                                    if (!valueReader.decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (!valueReader.decoder.hasNext(page)) {
                                break;
                            }
                            boolean v = valueReader.decoder.readBoolean(page);
                            if (mode == -1) {
                                if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.satisfyObject(v, valueFilter))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.satisfyObject(v, valueFilter)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putBoolean(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 0) {
                                if (updateData[0].getTime(updateIdx[0]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[0].getTime(updateIdx[0] + 1)) {
                                    // update the value
                                    if (timeFilter == null
                                            || timeVisitor.verify(timeValues[timeIdx])) {
                                        res.putInt(updateData[0].getInt(updateIdx[0] / 2));
                                        res.putTime(timeValues[timeIdx]);
                                        resCount++;
                                    }
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.satisfyObject(v, valueFilter))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.satisfyObject(v, valueFilter)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putBoolean(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 1) {
                                if (updateData[1].getTime(updateIdx[1]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[1].getTime(updateIdx[1] + 1)) {
                                    // do nothing
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.satisfyObject(v, valueFilter))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.satisfyObject(v, valueFilter)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putBoolean(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            // set the interval to next position that current time
                            // in page maybe be included.
                            while (mode != -1 && timeIdx < timeValues.length
                                    && timeValues[timeIdx] > updateData[mode].getTime(updateIdx[mode] + 1)) {
                                updateIdx[mode] += 2;
                                mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case INT64:
                        while (valueReader.decoder.hasNext(page)) {
                            // put insert points
                            while (insertMemoryData.hasInsertData() && timeIdx < timeValues.length
                                    && insertMemoryData.getCurrentMinTime() <= timeValues[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putLong(insertMemoryData.getCurrentLongValue());
                                res.insertTrueIndex++;
                                resCount++;

                                if (insertMemoryData.getCurrentMinTime() == timeValues[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                    valueReader.decoder.readLong(page);
                                    if (!valueReader.decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (!valueReader.decoder.hasNext(page)) {
                                break;
                            }
                            long v = valueReader.decoder.readLong(page);
                            if (mode == -1) {
                                if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putLong(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 0) {
                                if (updateData[0].getTime(updateIdx[0]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[0].getTime(updateIdx[0] + 1)) {
                                    //TODO update the value, need discuss the logic with gaofei
                                    if (timeFilter == null
                                            || timeVisitor.verify(timeValues[timeIdx])) {
                                        res.putLong(updateData[0].getLong(updateIdx[0] / 2));
                                        res.putTime(timeValues[timeIdx]);
                                        resCount++;
                                    }
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putLong(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 1) {
                                if (updateData[1].getTime(updateIdx[1]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[1].getTime(updateIdx[1] + 1)) {
                                    // do nothing
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putLong(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            while (mode != -1 && timeIdx < timeValues.length
                                    && timeValues[timeIdx] > updateData[mode].getTime(updateIdx[mode] + 1)) {
                                updateIdx[mode] += 2;
                                mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case FLOAT:
                        while (valueReader.decoder.hasNext(page)) {
                            // put insert points
                            while (insertMemoryData.hasInsertData() && timeIdx < timeValues.length
                                    && insertMemoryData.getCurrentMinTime() <= timeValues[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putFloat(insertMemoryData.getCurrentFloatValue());
                                res.insertTrueIndex++;
                                resCount++;

                                if (insertMemoryData.getCurrentMinTime() == timeValues[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                    valueReader.decoder.readFloat(page);
                                    if (!valueReader.decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (!valueReader.decoder.hasNext(page)) {
                                break;
                            }
                            float v = valueReader.decoder.readFloat(page);
                            if (mode == -1) {
                                if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putFloat(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 0) {
                                if (updateData[0].getTime(updateIdx[0]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[0].getTime(updateIdx[0] + 1)) {
                                    // update the value
                                    if (timeFilter == null
                                            || timeVisitor.verify(timeValues[timeIdx])) {
                                        res.putFloat(updateData[0].getFloat(updateIdx[0] / 2));
                                        res.putTime(timeValues[timeIdx]);
                                        resCount++;
                                    }
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putFloat(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 1) {
                                if (updateData[1].getTime(updateIdx[1]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[1].getTime(updateIdx[1] + 1)) {
                                    // do nothing
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putFloat(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            while (mode != -1 && timeIdx < timeValues.length
                                    && timeValues[timeIdx] > updateData[mode].getTime(updateIdx[mode] + 1)) {
                                updateIdx[mode] += 2;
                                mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case DOUBLE:
                        while (valueReader.decoder.hasNext(page)) {
                            // put insert points
                            while (insertMemoryData.hasInsertData() && timeIdx < timeValues.length
                                    && insertMemoryData.getCurrentMinTime() <= timeValues[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putDouble(insertMemoryData.getCurrentDoubleValue());
                                res.insertTrueIndex++;
                                resCount++;

                                if (insertMemoryData.getCurrentMinTime() == timeValues[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                    valueReader.decoder.readDouble(page);
                                    if (!valueReader.decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (!valueReader.decoder.hasNext(page)) {
                                break;
                            }
                            double v = valueReader.decoder.readDouble(page);
                            if (mode == -1) {
                                if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putDouble(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 0) {
                                if (updateData[0].getTime(updateIdx[0]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[0].getTime(updateIdx[0] + 1)) {
                                    // update the value
                                    if (timeFilter == null
                                            || timeVisitor.verify(timeValues[timeIdx])) {
                                        res.putDouble(updateData[0].getDouble(updateIdx[0] / 2));
                                        res.putTime(timeValues[timeIdx]);
                                        resCount++;
                                    }
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putDouble(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 1) {
                                if (updateData[1].getTime(updateIdx[1]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[1].getTime(updateIdx[1] + 1)) {
                                    // do nothing
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.verify(v))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.verify(v)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putDouble(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            while (mode != -1 && timeIdx < timeValues.length
                                    && timeValues[timeIdx] > updateData[mode].getTime(updateIdx[mode] + 1)) {
                                updateIdx[mode] += 2;
                                mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case TEXT:
                        while (valueReader.decoder.hasNext(page)) {
                            // put insert points
                            while (insertMemoryData.hasInsertData() && timeIdx < timeValues.length
                                    && insertMemoryData.getCurrentMinTime() <= timeValues[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putBinary(insertMemoryData.getCurrentBinaryValue());
                                res.insertTrueIndex++;
                                resCount++;

                                if (insertMemoryData.getCurrentMinTime() == timeValues[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                    valueReader.decoder.readBinary(page);
                                    if (!valueReader.decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (!valueReader.decoder.hasNext(page)) {
                                break;
                            }
                            Binary v = valueReader.decoder.readBinary(page);
                            if (mode == -1) {
                                if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.satisfyObject(v, valueFilter))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.satisfyObject(v, valueFilter)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putBinary(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 0) {
                                if (updateData[0].getTime(updateIdx[0]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[0].getTime(updateIdx[0] + 1)) {
                                    // update the value
                                    if (timeFilter == null
                                            || timeVisitor.verify(timeValues[timeIdx])) {
                                        res.putBinary(updateData[0].getBinary(updateIdx[0] / 2));
                                        res.putTime(timeValues[timeIdx]);
                                        resCount++;
                                    }
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.satisfyObject(v, valueFilter))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.satisfyObject(v, valueFilter)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putBinary(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            if (mode == 1) {
                                if (updateData[1].getTime(updateIdx[1]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[1].getTime(updateIdx[1] + 1)) {
                                    // do nothing
                                } else if ((valueFilter == null && timeFilter == null)
                                        || (valueFilter != null && timeFilter == null
                                        && valueVisitor.satisfyObject(v, valueFilter))
                                        || (valueFilter == null && timeFilter != null
                                        && timeVisitor.verify(timeValues[timeIdx]))
                                        || (valueFilter != null && timeFilter != null
                                        && valueVisitor.satisfyObject(v, valueFilter)
                                        && timeVisitor.verify(timeValues[timeIdx]))) {
                                    res.putBinary(v);
                                    res.putTime(timeValues[timeIdx]);
                                    resCount++;
                                }
                                timeIdx++;
                            }

                            while (mode != -1 && timeIdx < timeValues.length
                                    && timeValues[timeIdx] > updateData[mode].getTime(updateIdx[mode] + 1)) {
                                updateIdx[mode] += 2;
                                mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    default:
                        throw new IOException("Data type not support. " + dataType);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // check where new records were put into res and whether the
            // frequency need to be recalculated.
            int resCurrentLength = res.valueLength;
            if (hasOverflowDataInThisPage && freqFilter != null) {
                boolean satisfied = true;
                if (!satisfied) {
                    res.rollBack(resCurrentLength - resPreviousLength);
                    resCount -= (resCurrentLength - resPreviousLength);
                }
            }
        }
        // represents current column has been read all.
        if ((res.pageOffset - valueReader.fileOffset) >= valueReader.totalSize) {
            res.plusRowGroupIndexAndInitPageOffset();
        }

        // save curIdx for batch read
        updateTrueData.curIdx = updateIdx[0];
        updateFalseData.curIdx = updateIdx[1];
        return res;
    }

    static AggregationResult aggregate(ValueReader valueReader, AggregateFunction func, InsertDynamicData insertMemoryData,
                                DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, SingleSeriesFilterExpression timeFilter,
                                SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter)
            throws IOException, ProcessorException {

        TSDataType dataType = valueReader.dataType;

        DynamicOneColumnData res = new DynamicOneColumnData(dataType, true);
        res.pageOffset = valueReader.fileOffset;

        // get column digest
        TsDigest digest = valueReader.getDigest();
        DigestForFilter digestFF = new DigestForFilter(digest.min, digest.max, dataType);
        LOG.debug("Aggretation : Column Digest min and max is: " + digestFF.getMinValue() + " --- " + digestFF.getMaxValue());
        DigestVisitor digestVisitor = new DigestVisitor();

        // to ensure that updateTrue and updateFalse is not null
        updateTrue = (updateTrue == null ? new DynamicOneColumnData(dataType, true) : updateTrue);
        updateFalse = (updateFalse == null ? new DynamicOneColumnData(dataType, true) : updateFalse);

        // if this column is not satisfied to the filter, then return.
        if (updateTrue.valueLength == 0 && insertMemoryData == null && valueFilter != null
                && !digestVisitor.satisfy(digestFF, valueFilter)) {
            return func.result;
        }

        DynamicOneColumnData[] update = new DynamicOneColumnData[2];
        update[0] = updateTrue;
        update[1] = updateFalse;
        int[] updateIdx = new int[]{updateTrue.curIdx, updateFalse.curIdx};

        ByteArrayInputStream bis = valueReader.initBAISForOnePage(res.pageOffset);
        PageReader pageReader = new PageReader(bis, valueReader.compressionTypeName);
        int pageCount = 0;

        while ((res.pageOffset - valueReader.fileOffset) < valueReader.totalSize) {
            int lastAvailable = bis.available();
            pageCount++;
            LOG.debug("read page {}, offset : {}", pageCount, res.pageOffset);

            PageHeader pageHeader = pageReader.getNextPageHeader();
            // construct value and time digest for this page
            Digest pageDigest = pageHeader.data_page_header.getDigest();
            DigestForFilter valueDigestFF = new DigestForFilter(pageDigest.min, pageDigest.max, dataType);
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);

            int mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateTrue, updateFalse);

            // find first interval , skip some intervals that not available
            while (mode != -1 && update[mode].getTime(updateIdx[mode] + 1) < mint) {
                updateIdx[mode] += 2;
                mode = ReaderUtils.getNextMode(updateIdx[0], updateIdx[1], updateTrue, updateFalse);
            }

            // check whether current page is satisfied to filters.
            if (mode == -1 && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }
            if (mode == 0 && update[0].getTime(updateIdx[0]) > maxt
                    && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }
            if (mode == 1 && ((update[1].getTime(updateIdx[1]) <= mint && update[1].getTime(updateIdx[1] + 1) >= maxt)
                    || ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter))))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }

            // get the InputStream for this page
            InputStream page = pageReader.getNextPage();
            // update current res's pageOffset to the start of next page.
            res.pageOffset += lastAvailable - bis.available();
            boolean hasOverflowDataInThisPage = checkDataChangedForAggregation(mint, maxt, valueDigestFF
                    , updateTrue, updateIdx[0], updateFalse, updateIdx[1], insertMemoryData
                    , timeFilter, freqFilter, valueFilter);
            LOG.debug("Having Overflow info in this page : {}", hasOverflowDataInThisPage);

            // there is no overflow data in this page
            if (!hasOverflowDataInThisPage) {
                func.calculateValueFromPageHeader(pageHeader);
            } else {
                // get all time values in this page
                long[] timeValues = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
                // set Decoder for current page
                valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));

                // clear data in res to make the res only store the data in current page;
                // TODO max, min value could be optimized
                res = ReaderUtils.readOnePage(dataType, timeValues, valueReader.decoder, page, res,
                        timeFilter, freqFilter, valueFilter, insertMemoryData, update, updateIdx);
                func.calculateValueFromDataPage(res);
                res.clearData();
            }
        }

        // record the current index for overflow info
        updateTrue.curIdx = updateIdx[0];
        updateFalse.curIdx = updateIdx[1];

        return func.result;
    }

    /**
     * <p>
     * An aggregation method implementation for the ValueReader aspect.
     * The aggregation will be calculated using the calculated common timestamps.
     *
     * @param func aggregation function
     * @param insertMemoryData bufferwrite memory insert data with overflow operation
     * @param updateTrue overflow update operation which satisfy the filter
     * @param updateFalse overflow update operation which doesn't satisfy the filter
     * @param timeFilter time filter
     * @param freqFilter frequency filter
     * @param timestamps the timestamps which aggregation must satisfy
     * @param lastAggreData last aggregation result, this variable is used for batch read
     * @return an int value, represents the read time index of timestamps
     * @throws IOException TsFile read error
     * @throws ProcessorException get read info error
     */
    static int aggregateUsingTimestamps(ValueReader valueReader, AggregateFunction func, InsertDynamicData insertMemoryData,
                                 DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, SingleSeriesFilterExpression timeFilter,
                                 SingleSeriesFilterExpression freqFilter, List<Long> timestamps,
                                 DynamicOneColumnData lastAggreData) throws IOException, ProcessorException {
        TSDataType dataType = valueReader.dataType;

        // to ensure that updateTrue and updateFalse is not null
        updateTrue = (updateTrue == null ? new DynamicOneColumnData(dataType, true) : updateTrue);
        updateFalse = (updateFalse == null ? new DynamicOneColumnData(dataType, true) : updateFalse);

        DynamicOneColumnData[] update = new DynamicOneColumnData[2];
        update[0] = updateTrue;
        update[1] = updateFalse;
        int[] updateIdx = new int[]{updateTrue.curIdx, updateFalse.curIdx};
        int timeIndex = 0;

        // for batch read
        while (func.maps.containsKey("pageTimeValues")) {
            long[] pageTimeValues = (long[]) func.maps.get("pageTimeValues");
            int pageTimeIndex = (int) func.maps.get("pageTimeIndex");
            InputStream page = (InputStream) func.maps.get("page");
            Pair<DynamicOneColumnData, Integer> ans = ReaderUtils.readOnePage(
                    dataType, pageTimeValues, pageTimeIndex, valueReader.decoder, page,
                    timeFilter, freqFilter, timestamps, 0, insertMemoryData, update, updateIdx, func);
            if (ans.left != null && ans.left.valueLength > 0)
                func.calculateValueFromDataPage(ans.left);
            timeIndex = ans.right;
            if (timeIndex >= timestamps.size()) {
                return timeIndex;
            }
        }

        // value filter is always null
        if (lastAggreData == null) {
            lastAggreData = new DynamicOneColumnData(dataType, true);
        }
        lastAggreData.pageOffset = valueReader.fileOffset;


        // get column digest
        TsDigest digest = valueReader.getDigest();
        DigestForFilter digestFF = new DigestForFilter(digest.min, digest.max, valueReader.getDataType());
        LOG.debug("aggregate using given timestamps, column Digest min and max is: "
                + digestFF.getMinValue() + " --- " + digestFF.getMaxValue());
        DigestVisitor digestVisitor = new DigestVisitor();

        ByteArrayInputStream bis = valueReader.initBAISForOnePage(lastAggreData.pageOffset);
        PageReader pageReader = new PageReader(bis, valueReader.compressionTypeName);
        int pageCount = 0;

        // (lastAggreData.pageOffset - fileOffset) < totalSize : still has unread data
        while ((lastAggreData.pageOffset - valueReader.fileOffset) < valueReader.totalSize) {
            int lastAvailable = bis.available();
            pageCount++;
            LOG.debug("aggregate using given timestamps, read page {}, offset : {}", pageCount, lastAggreData.pageOffset);

            PageHeader pageHeader = pageReader.getNextPageHeader();
            Digest pageDigest = pageHeader.data_page_header.getDigest();
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);

            // the min value of common timestamps is greater than max time in this series
            if (timestamps.get(timeIndex) > maxt) {
                pageReader.skipCurrentPage();
                lastAggreData.pageOffset += lastAvailable - bis.available();
                continue;
            }

            // if the current page doesn't satisfy the time filter
            if (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter))  {
                pageReader.skipCurrentPage();
                lastAggreData.pageOffset += lastAvailable - bis.available();
                // TODO adjust index of timestamps to fit pageReader.skipCurrentPage()
                continue;
            }

            // get the InputStream for this page
            InputStream page = pageReader.getNextPage();
            // update lastAggreData's pageOffset to the start of next page.
            lastAggreData.pageOffset += lastAvailable - bis.available();

            // get all time values in this page
            long[] pageTimeValues = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            // set Decoder for current page
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), valueReader.getDataType()));

            Pair<DynamicOneColumnData, Integer> ans = ReaderUtils.readOnePage(
                    dataType, pageTimeValues, 0, valueReader.decoder, page,
                    timeFilter, freqFilter, timestamps, timeIndex, insertMemoryData, update, updateIdx, func);
            if (ans.left != null && ans.left.valueLength > 0)
                func.calculateValueFromDataPage(ans.left);
            lastAggreData.clearData();
            timeIndex = ans.right;
            if (timeIndex >= timestamps.size())
                break;
        }

        // record the current updateTrue,updateFalse index for overflow info
        updateTrue.curIdx = updateIdx[0];
        updateFalse.curIdx = updateIdx[1];

        return timeIndex;
    }

    // TODO bug: not consider delete operation, maybe no need to consider.
    private static boolean checkDataChanged(long mint, long maxt, DynamicOneColumnData updateTrueData, int updateTrueIdx,
                                     DynamicOneColumnData updateFalseData, int updateFalseIdx, InsertDynamicData insertMemoryData,
                                     SingleSeriesFilterExpression timeFilter) throws IOException {
        // Judge whether updateTrue has value for this page.
        while (updateTrueIdx <= updateTrueData.timeLength - 2) {
            if (!((updateTrueData.getTime(updateTrueIdx + 1) < mint) || (updateTrueData.getTime(updateTrueIdx) > maxt))) {
                return true;
            }
            updateTrueIdx += 2;
        }

        while (updateFalseIdx <= updateFalseData.timeLength - 2) {
            if (!((updateFalseData.getTime(updateFalseIdx + 1) < mint) || (updateFalseData.getTime(updateFalseIdx) > maxt))) {
                return true;
            }
            updateFalseIdx += 2;
        }

        while (insertMemoryData.hasInsertData()) {
            if (mint <= insertMemoryData.getCurrentMinTime() && insertMemoryData.getCurrentMinTime() <= maxt) {
                return true;
            }
            if (maxt < insertMemoryData.getCurrentMinTime()) {
                break;
            }
            if (insertMemoryData.hasInsertData() && mint > insertMemoryData.getCurrentMinTime()) {
                break;
            }
            insertMemoryData.removeCurrentValue();
        }
        return false;
    }

    // TODO bug: not consider delete operation, maybe no need to consider.
    private static boolean checkDataChangedForAggregation(long mint, long maxt, DigestForFilter pageDigest, DynamicOneColumnData updateTrue, int idx0,
                                                   DynamicOneColumnData updateFalse, int idx1, InsertDynamicData insertMemoryData,
                                                   SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {

        if (checkDataChanged(mint, maxt, updateTrue, idx0, updateFalse, idx1, insertMemoryData, timeFilter)) {
            return true;
        }

        DigestForFilter timeDigest = new DigestForFilter(mint, maxt);
        PageAllSatisfiedVisitor visitor = new PageAllSatisfiedVisitor();
        if (timeFilter != null && !visitor.satisfy(timeDigest, timeFilter)) {
            return true;
        }
        if (valueFilter != null && !visitor.satisfy(pageDigest, valueFilter)) {
            return true;
        }
        return false;
    }
}
