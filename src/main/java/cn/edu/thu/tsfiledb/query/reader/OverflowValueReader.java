package cn.edu.thu.tsfiledb.query.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.aggregation.AggregationResult;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import cn.edu.thu.tsfiledb.query.visitorImpl.PageAllSatisfiedVisitor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.TSDigest;
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

public class OverflowValueReader extends ValueReader {

    private static final Logger LOG = LoggerFactory.getLogger(OverflowValueReader.class);
    
    OverflowValueReader(long offset, long totalSize, TSDataType dataType, TSDigest digest,
                               TSRandomAccessFileReader raf, List<String> enumValues, CompressionTypeName compressionTypeName,
                               long rowNums) {
        super(offset, totalSize, dataType, digest, raf, enumValues, compressionTypeName, rowNums);
    }

    private ByteArrayInputStream initBAISForOnePage(long pageOffset) throws IOException {
        int length = (int) (this.totalSize - (pageOffset - fileOffset));
        // int length = (int) (this.totalSize + fileOffset - valueOffset);
        byte[] buf = new byte[length]; // warning
        int readSize = 0;
        raf.seek(pageOffset);
        readSize = raf.read(buf, 0, length);
        if (readSize != length) {
            throw new IOException("Expect byte size : " + length + ". Read size : " + readSize);
        }

        return new ByteArrayInputStream(buf);
    }

    @Deprecated
    public void setFreqDecoderByDataType() {

    }

    @Deprecated
    public void initFrequenceValue(InputStream page) throws IOException {
        //
    }

    @Deprecated
    public boolean frequencySatisfy(SingleSeriesFilterExpression freqFilter) {
        return true;
    }

    /**
     * -1: no updateTrue data, no updateFalse data.
     * 0: updateTrue data < updateFalse data.
     * 1: updateFalse data < updateTrue data.
     *
     * @param updateTrueIdx index of updateTrue DynamicOneColumn
     * @param updateFalseIdx index of updateFalse DynamicOneColumn
     * @param updateTrue updateTrue DynamicOneColumn
     * @param updateFalse updateFalse DynamicOneColumn
     * @return
     */
    private int getNextMode(int updateTrueIdx, int updateFalseIdx, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse) {
        if (updateTrueIdx > updateTrue.timeLength - 2 && updateFalseIdx > updateFalse.timeLength - 2) {
            return -1;
        } else if (updateTrueIdx <= updateTrue.timeLength - 2 && updateFalseIdx > updateFalse.timeLength - 2) {
            return 0;
        } else if (updateTrueIdx > updateTrue.timeLength - 2 && updateFalseIdx <= updateFalse.timeLength - 2) {
            return 1;
        } else {
            long t0 = updateTrue.getTime(updateTrueIdx);
            long t1 = updateFalse.getTime(updateFalseIdx);
            return t0 < t1 ? 0 : 1;
        }
    }

    DynamicOneColumnData getValuesWithOverFlow(DynamicOneColumnData updateTrueData, DynamicOneColumnData updateFalseData,
                                               InsertDynamicData insertMemoryData, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                               SingleSeriesFilterExpression valueFilter, DynamicOneColumnData res, int fetchSize) throws IOException {

        if (res == null) {
            res = new DynamicOneColumnData(getDataType(), true);
            res.pageOffset = this.fileOffset;
            res.leftSize = this.totalSize;
            res.insertTrueIndex = 0;
        }

        // IMPORTANT!!
        if (res.pageOffset == -1) {
            res.pageOffset = this.fileOffset;
        }

        TSDigest digest = getDigest();
        DigestForFilter digestFF = new DigestForFilter(digest.min, digest.max, getDataType());
        LOG.info("OverflowValueReader Column Digest min and max is: " + digestFF.getMinValue() + " --- " + digestFF.getMaxValue());
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

        int mode = getNextMode(updateIdx[0], updateIdx[1], updateTrueData, updateFalseData);

        // initial one page from file
        ByteArrayInputStream bis = initBAISForOnePage(res.pageOffset);
        PageReader pageReader = new PageReader(bis, compressionTypeName);
        int pageCount = 0;
        // let resCount be the sum of records in last read
        // In BatchReadRecordGenerator, The ResCount needed equals to (res.valueLength - res.curIdx)??
        int resCount = res.valueLength - res.curIdx;

        // some variables for frequency calculation with overflow
        boolean hasOverflowDataInThisPage = false;

        while ((res.pageOffset - fileOffset) < totalSize && resCount < fetchSize) {
            // To help to record byte size in this process of read.
            int lastAvailable = bis.available();
            pageCount++;
            LOG.debug("read page {}, offset : {}", pageCount, res.pageOffset);
            PageHeader pageHeader = pageReader.getNextPageHeader();

            // construct valueFilter
            // System.out.println(res.pageOffset + "|" + fileOffset + "|" + totalSize);
            Digest pageDigest = pageHeader.data_page_header.getDigest();
            DigestForFilter valueDigestFF = new DigestForFilter(pageDigest.min, pageDigest.max, getDataType());

            // construct timeFilter
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);

            // find first interval , skip some intervals that not available
            while (mode != -1 && updateData[mode].getTime(updateIdx[mode] + 1) < mint) {
                updateIdx[mode] += 2;
                mode = getNextMode(updateIdx[0], updateIdx[1], updateTrueData, updateFalseData);
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

            initFrequenceValue(page);
            hasOverflowDataInThisPage = checkDataChanged(mint, maxt, updateTrueData, updateIdx[0], updateFalseData, updateIdx[1],
                    insertMemoryData, timeFilter);
//				System.out.println("Overflow: " + hasOverflowDataInThisPage);
            if (!hasOverflowDataInThisPage && !frequencySatisfy(freqFilter)) {
                continue;
            }

            long[] timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);

            setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), getDataType()));

            // Record the length of this res before the new records in this page
            // were put in.
            int resPreviousLength = res.valueLength;

            SingleValueVisitor<?> timeVisitor = null;
            if (timeFilter != null) {
                timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
            }
            SingleValueVisitor<?> valueVisitor = null;
            if (valueFilter != null) {
                valueVisitor = getSingleValueVisitorByDataType(getDataType(), valueFilter);
            }

            try {
                int timeIdx = 0;
                switch (dataType) {
                    case INT32:
                        while (decoder.hasNext(page)) {
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
                                    decoder.readInt(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (!decoder.hasNext(page)) {
                                break;
                            }
                            int v = decoder.readInt(page);
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

                            // Set the interval to next position that current time
                            // in page maybe be included.
                            while (mode != -1 && timeIdx < timeValues.length
                                    && timeValues[timeIdx] > updateData[mode].getTime(updateIdx[mode] + 1)) {
                                updateIdx[mode] += 2;
                                mode = getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case BOOLEAN:
                        while (decoder.hasNext(page)) {
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
                                    decoder.readBoolean(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (mode == -1) {
                                boolean v = decoder.readBoolean(page);
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
                                boolean v = decoder.readBoolean(page);
                                if (updateData[0].getTime(updateIdx[0]) <= timeValues[timeIdx]
                                        && timeValues[timeIdx] <= updateData[0].getTime(updateIdx[0] + 1)) {
                                    // update the value
                                    if (timeFilter == null
                                            || timeVisitor.verify(timeValues[timeIdx])) {
                                        res.putBoolean(updateData[0].getBoolean(updateIdx[0] / 2));
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
                                boolean v = decoder.readBoolean(page);
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

                            while (mode != -1 && timeIdx < timeValues.length
                                    && timeValues[timeIdx] > updateData[mode].getTime(updateIdx[mode] + 1)) {
                                updateIdx[mode] += 2;
                                mode = getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case INT64:
                        while (decoder.hasNext(page)) {
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
                                    decoder.readLong(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (!decoder.hasNext(page)) {
                                break;
                            }
                            long v = decoder.readLong(page);
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
                                mode = getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case FLOAT:
                        while (decoder.hasNext(page)) {
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
                                    decoder.readFloat(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (!decoder.hasNext(page)) {
                                break;
                            }
                            float v = decoder.readFloat(page);
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
                                mode = getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case DOUBLE:
                        while (decoder.hasNext(page)) {
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
                                    decoder.readDouble(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (!decoder.hasNext(page)) {
                                break;
                            }
                            double v = decoder.readDouble(page);
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
                                mode = getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    case TEXT:
                        while (decoder.hasNext(page)) {
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
                                    decoder.readBinary(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }

                            if (!decoder.hasNext(page)) {
                                break;
                            }
                            Binary v = decoder.readBinary(page);
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
                                mode = getNextMode(updateIdx[0], updateIdx[1], updateData[0], updateData[1]);
                            }
                        }
                        break;
                    default:
                        throw new IOException("Data type not support. " + dataType);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Check where new records were put into res and whether the
            // frequency need to be recalculated.
            int resCurrentLength = res.valueLength;
            if (hasOverflowDataInThisPage && freqFilter != null) {
//					boolean satisfied = frequencyCalculator.satisfy(freqFilter);
                boolean satisfied = true;
                if (!satisfied) {
                    res.rollBack(resCurrentLength - resPreviousLength);
                    resCount -= (resCurrentLength - resPreviousLength);
                }
            }
        }
        // Represents current Column has been read all.
        if ((res.pageOffset - fileOffset) >= totalSize) {
            res.plusRowGroupIndexAndInitPageOffset();
        }

        // Important. save curIdx for batch read
        updateTrueData.curIdx = updateIdx[0];
        updateFalseData.curIdx = updateIdx[1];
        return res;
    }

    AggregationResult aggreate(AggregateFunction func, InsertDynamicData insertMemoryData,
                                      DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, SingleSeriesFilterExpression timeFilter,
                                      SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException, ProcessorException {

        // initialize
        DynamicOneColumnData res = new DynamicOneColumnData(dataType, true);
        res.pageOffset = this.fileOffset;

        // Get column digest
        TSDigest digest = getDigest();
        DigestForFilter digestFF = new DigestForFilter(digest.min, digest.max, getDataType());
        LOG.debug("Aggretation : Column Digest min and max is: " + digestFF.getMinValue() + " --- " + digestFF.getMaxValue());
        DigestVisitor digestVisitor = new DigestVisitor();

        // initialize overflow info
        updateTrue = (updateTrue == null ? new DynamicOneColumnData(dataType, true) : updateTrue);
        // insertMemoryData = (insertMemoryData == null ? new DynamicOneColumnData(dataType, true) : insertMemoryData);
        updateFalse = (updateFalse == null ? new DynamicOneColumnData(dataType, true) : updateFalse);

        // if this column is not satisfied to the filter, then return.
        if (updateTrue.valueLength == 0 && insertMemoryData == null && valueFilter != null
                && !digestVisitor.satisfy(digestFF, valueFilter)) {
            return func.result;
        }

        DynamicOneColumnData[] update = new DynamicOneColumnData[2];
        update[0] = updateTrue;
        update[1] = updateFalse;
        int[] idx = new int[]{updateTrue.curIdx, updateFalse.curIdx};

        ByteArrayInputStream bis = initBAISForOnePage(res.pageOffset);
        PageReader pageReader = new PageReader(bis, compressionTypeName);
        int pageCount = 0;

        while ((res.pageOffset - fileOffset) < totalSize) {
            int lastAvailable = bis.available();
            pageCount++;
            LOG.debug("read page {}, offset : {}", pageCount, res.pageOffset);

            PageHeader pageHeader = pageReader.getNextPageHeader();
            // construct value and time digest for this page
            Digest pageDigest = pageHeader.data_page_header.getDigest();
            DigestForFilter valueDigestFF = new DigestForFilter(pageDigest.min, pageDigest.max, getDataType());
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);


            int mode = getNextMode(idx[0], idx[1], updateTrue, updateFalse);
            // find first interval , skip some intervals that not available
            while (mode != -1 && update[mode].getTime(idx[mode] + 1) < mint) {
                idx[mode] += 2;
                mode = getNextMode(idx[0], idx[1], updateTrue, updateFalse);
            }

            //check whether current page is satisfied to filters.
            if (mode == -1 && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }
            if (mode == 0 && update[0].getTime(idx[0]) > maxt
                    && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }
            if (mode == 1 && ((update[1].getTime(idx[1]) <= mint && update[1].getTime(idx[1] + 1) >= maxt)
                    || ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter))))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }

            //Get the InputStream for this page
            InputStream page = pageReader.getNextPage();
            // update current res's pageOffset to the start of next page.
            res.pageOffset += lastAvailable - bis.available();
            initFrequenceValue(page);
            boolean hasOverflowDataInThisPage = checkDataChangedForAggregation(mint, maxt, valueDigestFF
                    , updateTrue, idx[0], updateFalse, idx[1], insertMemoryData
                    , timeFilter, freqFilter, valueFilter);
            LOG.debug("Having Overflow info in this page : {}", hasOverflowDataInThisPage);

            //If there is no overflow data in this page
            boolean needToReadData = true;
            if (!hasOverflowDataInThisPage) {
                needToReadData = !func.couldCalculateFromPageHeader(pageHeader);
            }

            if (needToReadData) {
                //Get all time values in this page
                long[] timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);
                //Set Decoder for current page
                setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), getDataType()));

                // clear data in res to make the res only store the data in current page;
                // TODO max, min value could be optimized
                res = readOnePageWithOverflow(idx, timeValues, page, pageHeader, res,
                            timeFilter, freqFilter, valueFilter, insertMemoryData, update);
                func.calculateFromDataInThisPage(res);
                res.clearData();
            }
        }

        //Record the current index for overflow info
//			insertTrue.curIdx = idx2;
        updateTrue.curIdx = idx[0];
        updateFalse.curIdx = idx[1];

        return func.result;
    }

    // calculate the aggregate using timestamps
    AggregationResult aggregateUseTimestamps(AggregateFunction func, InsertDynamicData insertMemoryData,
                               DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, SingleSeriesFilterExpression timeFilter,
                               SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, List<Long> timestamps) throws IOException, ProcessorException {

        DynamicOneColumnData res = new DynamicOneColumnData(dataType, true);
        res.pageOffset = this.fileOffset;

        // get column digest
        TSDigest digest = getDigest();
        DigestForFilter digestFF = new DigestForFilter(digest.min, digest.max, getDataType());
        LOG.debug("Aggretation calculation using timestamps: column Digest min and max is: " + digestFF.getMinValue() + " --- " + digestFF.getMaxValue());
        DigestVisitor digestVisitor = new DigestVisitor();

        // initialize overflow info
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
        int[] idx = new int[]{updateTrue.curIdx, updateFalse.curIdx};

        ByteArrayInputStream bis = initBAISForOnePage(res.pageOffset);
        PageReader pageReader = new PageReader(bis, compressionTypeName);
        int pageCount = 0;

        while ((res.pageOffset - fileOffset) < totalSize) {
            int lastAvailable = bis.available();
            pageCount++;
            LOG.debug("read page {}, offset : {}", pageCount, res.pageOffset);

            PageHeader pageHeader = pageReader.getNextPageHeader();
//				System.out.println("===== Page: " + pageCount + ". Count: " + pageHeader.data_page_header.num_rows);
            // construct value and time digest for this page
            Digest pageDigest = pageHeader.data_page_header.getDigest();
            DigestForFilter valueDigestFF = new DigestForFilter(pageDigest.min, pageDigest.max, getDataType());
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);


            int mode = getNextMode(idx[0], idx[1], updateTrue, updateFalse);
            // find first interval , skip some intervals that not available
            while (mode != -1 && update[mode].getTime(idx[mode] + 1) < mint) {
                idx[mode] += 2;
                mode = getNextMode(idx[0], idx[1], updateTrue, updateFalse);
            }

            //check whether current page is satisfied to filters.
            if (mode == -1 && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }
            if (mode == 0 && update[0].getTime(idx[0]) > maxt
                    && ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter)))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }
            if (mode == 1 && ((update[1].getTime(idx[1]) <= mint && update[1].getTime(idx[1] + 1) >= maxt)
                    || ((valueFilter != null && !digestVisitor.satisfy(valueDigestFF, valueFilter))
                    || (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter))))) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }

            //Get the InputStream for this page
            InputStream page = pageReader.getNextPage();
            // update current res's pageOffset to the start of next page.
            res.pageOffset += lastAvailable - bis.available();
            initFrequenceValue(page);
            boolean hasOverflowDataInThisPage = checkDataChangedForAggregation(mint, maxt, valueDigestFF
                    , updateTrue, idx[0], updateFalse, idx[1], insertMemoryData
                    , timeFilter, freqFilter, valueFilter);
            LOG.debug("Having Overflow info in this page : {}", hasOverflowDataInThisPage);

            //If there is no overflow data in this page
            boolean needToReadData = true;
            if (!hasOverflowDataInThisPage) {
                needToReadData = !func.couldCalculateFromPageHeader(pageHeader);
            }

            if (needToReadData) {
                //Get all time values in this page
                long[] timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);
                //Set Decoder for current page
                setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), getDataType()));

                // clear data in res to make the res only store the data in current page;
                // TODO max, min value could be optimized
                res = readOnePageWithOverflow(idx, timeValues, page, pageHeader, res,
                        timeFilter, freqFilter, valueFilter, insertMemoryData, update);
                func.calculateFromDataInThisPage(res);
                res.clearData();
            }
        }

        //Record the current index for overflow info
//			insertTrue.curIdx = idx2;
        updateTrue.curIdx = idx[0];
        updateFalse.curIdx = idx[1];

        return func.result;
    }

    /**
     * Only used for aggreate function.
     *
     * @return
     * @throws IOException
     */
    private DynamicOneColumnData readOnePageWithOverflow(int[] idx, long[] timeValues, InputStream page, PageHeader pageHeader, DynamicOneColumnData res,
                                                         SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                         InsertDynamicData insertMemoryData, DynamicOneColumnData[] update) throws IOException {

        // Calculate current mode
        int mode = getNextMode(idx[0], idx[1], update[0], update[1]);

        try {
            SingleValueVisitor<?> timeVisitor = null;
            if (timeFilter != null) {
                timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
            }
            SingleValueVisitor<?> valueVisitor = null;
            if (valueFilter != null) {
                valueVisitor = getSingleValueVisitorByDataType(getDataType(), valueFilter);
            }

            int timeIdx = 0;
            switch (dataType) {
                case INT32:
                    while (decoder.hasNext(page)) {
                        // put insert points that less than or equals to current
                        // Timestamp in page.
                        while (insertMemoryData.hasInsertData() && timeIdx < timeValues.length
                                && insertMemoryData.getCurrentMinTime() <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putInt(insertMemoryData.getCurrentIntValue());
                            res.insertTrueIndex++;

                            if (insertMemoryData.getCurrentMinTime() == timeValues[timeIdx]) {
                                insertMemoryData.removeCurrentValue();
                                timeIdx++;
                                decoder.readInt(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }
                        }

                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        int v = decoder.readInt(page);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putInt(update[0].getInt(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
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
                            }
                            timeIdx++;
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case BOOLEAN:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putBoolean(insertMemoryData.getBoolean(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readBoolean(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }

                        if (mode == -1) {
                            boolean v = decoder.readBoolean(page);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            boolean v = decoder.readBoolean(page);
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putBoolean(update[0].getBoolean(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            boolean v = decoder.readBoolean(page);
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
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
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case INT64:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putLong(insertMemoryData.getLong(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readLong(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }
                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        long v = decoder.readLong(page);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value,需要和高飞再商量一下这个逻辑
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putLong(update[0].getLong(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
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
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case FLOAT:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putFloat(insertMemoryData.getFloat(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readFloat(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }
                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        float v = decoder.readFloat(page);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putFloat(update[0].getFloat(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
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
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case DOUBLE:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putDouble(insertMemoryData.getDouble(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readDouble(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }
                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        double v = decoder.readDouble(page);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putDouble(update[0].getDouble(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
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
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case TEXT:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putBinary(insertMemoryData.getBinary(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readBinary(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }
                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        Binary v = decoder.readBinary(page);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putBinary(update[0].getBinary(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
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
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
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
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                default:
                    throw new IOException("Data type not support : " + dataType);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Don't forget to update the curIdx in updateTrue and updateFalse
        update[0].curIdx = idx[0];
        update[1].curIdx = idx[1];
        return res;
    }

    // TODO bug: not consider delete operation, maybe no need to consider.
    private boolean checkDataChanged(long mint, long maxt, DynamicOneColumnData updateTrueData, int updateTrueIdx,
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
    private boolean checkDataChangedForAggregation(long mint, long maxt, DigestForFilter pageDigest, DynamicOneColumnData updateTrue, int idx0,
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
