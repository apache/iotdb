package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.*;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class AggregateRecordReader extends RecordReader {

    private static final Logger logger = LoggerFactory.getLogger(AggregateRecordReader.class);

    public AggregateRecordReader(GlobalSortedSeriesDataSource globalSortedSeriesDataSource, OverflowSeriesDataSource overflowSeriesDataSource,
                                 String deltaObjectId, String measurementId,
                                 SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter)
            throws PathErrorException, IOException {
        super(globalSortedSeriesDataSource, overflowSeriesDataSource, deltaObjectId, measurementId,
                queryTimeFilter, queryValueFilter);
    }

    /**
     * the RowGroupReader used index
     **/
    private int usedRowGroupReaderIndex;

    /**
     * the ValueReader used index
     **/
    private int usedValueReaderIndex;

    /**
     * the used file stream offset for batch read
     **/
    private long usedPageOffset = -1;

    /**
     * Aggregation calculate function of <code>RecordReader</code> without filter.
     *
     * @param aggregateFunction aggregation function
     * @return aggregation result
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException        TsFile read exception
     */
    public AggregateFunction aggregate(AggregateFunction aggregateFunction) throws ProcessorException, IOException {

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, queryTimeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                aggregate(rowGroupReader.getValueReaders().get(measurementId), aggregateFunction);
            }
        }

        for (ValueReader valueReader : valueReaders) {
            if (valueReader.getDataType().equals(dataType)) {
                aggregate(valueReader, aggregateFunction);
            }
        }

        // consider left insert values
        // all timestamp of these values are greater than timestamp in List<RowGroupReader>
        if (insertMemoryData != null && insertMemoryData.hasNext()) {
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
     * @param aggregateFunction aggregation function
     * @param timestamps        timestamps calculated by the cross filter
     * @return aggregation result and whether still has unread data
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException        TsFile read exception
     */
    public Pair<AggregateFunction, Boolean> aggregateUsingTimestamps(AggregateFunction aggregateFunction,
                                                                     List<Long> timestamps)
            throws ProcessorException, IOException {

        boolean stillHasUnReadData;
        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, queryTimeFilter);
        int commonTimestampsIndex = 0;

        while (usedRowGroupReaderIndex < rowGroupReaderList.size()) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(usedRowGroupReaderIndex);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {

                // TODO commonTimestampsIndex could be saved as a parameter

                commonTimestampsIndex = aggregateUsingTimestamps(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, timestamps);

                // all value of commonTimestampsIndex has been used,
                // the next batch of commonTimestamps should be loaded
                if (commonTimestampsIndex >= timestamps.size()) {
                    return new Pair<>(aggregateFunction, true);
                }
                usedRowGroupReaderIndex++;
            } else {
                usedRowGroupReaderIndex++;
            }
        }

        while (usedValueReaderIndex < valueReaders.size()) {
            if (valueReaders.get(usedValueReaderIndex).getDataType().equals(dataType)) {

                // TODO commonTimestampsIndex could be saved as a parameter

                commonTimestampsIndex = aggregateUsingTimestamps(valueReaders.get(usedValueReaderIndex), aggregateFunction, timestamps);

                // all value of commonTimestampsIndex has been used,
                // the next batch of commonTimestamps should be loaded
                if (commonTimestampsIndex >= timestamps.size()) {
                    return new Pair<>(aggregateFunction, true);
                }
            } else {
                usedValueReaderIndex++;
            }
        }

        // calculate aggregation using unsealed file data and memory data
        if (insertMemoryData.hasNext()) {
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

    private void aggregate(ValueReader valueReader, AggregateFunction func) throws IOException, ProcessorException {

        DynamicOneColumnData result = new DynamicOneColumnData(dataType, true);
        usedPageOffset = valueReader.fileOffset;

        // get series digest
        TsDigest digest = valueReader.getDigest();
        DigestForFilter valueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
        logger.debug(String.format("calculate aggregation without filter : series time range is [%s, %s], value range is [%s, %s]",
                valueReader.getStartTime(), valueReader.getEndTime(), valueDigest.getMinValue(), valueDigest.getMaxValue()));
        DigestVisitor valueDigestVisitor = new DigestVisitor();

        // skip the current series chunk according to time filter
        IntervalTimeVisitor seriesTimeVisitor = new IntervalTimeVisitor();
        if (queryTimeFilter != null && !seriesTimeVisitor.satisfy(queryTimeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            logger.debug(String.format("calculate aggregation without filter, series time does not satisfy time filter, time filter is [%s].",
                    queryTimeFilter.toString()));
            usedPageOffset = -1;
            return;
        }

        // skip the current series chunk according to value filter
        if (queryValueFilter != null && !valueDigestVisitor.satisfy(valueDigest, queryValueFilter)) {
            if ((!overflowOperationReaderCopy.hasNext() || overflowOperationReaderCopy.getCurrentOperation().getLeftBound() > valueReader.getEndTime()) &&
                    (!insertMemoryData.hasNext() || insertMemoryData.getCurrentMinTime() > valueReader.getEndTime())) {
                logger.debug(String.format("calculate aggregation without filter, series value digest does not satisfy value filter, value filter is [%s].",
                        queryValueFilter.toString()));
                usedPageOffset = -1;
                return;
            }
        }

        ByteArrayInputStream bis = valueReader.initBAISForOnePage(usedPageOffset);
        PageReader pageReader = new PageReader(bis, valueReader.compressionTypeName);

        while ((usedPageOffset - valueReader.fileOffset) < valueReader.totalSize) {
            int lastAvailable = bis.available();

            PageHeader pageHeader = pageReader.getNextPageHeader();
            DigestForFilter pageValueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                    digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;

            // skip the current page according to time filter
            if (queryTimeFilter != null && !seriesTimeVisitor.satisfy(queryTimeFilter, pageMinTime, pageMaxTime)) {
                pageReader.skipCurrentPage();
                usedPageOffset += lastAvailable - bis.available();
                continue;
            }

            // skip the current page according to value filter
            if (queryValueFilter != null && !valueDigestVisitor.satisfy(pageValueDigest, queryValueFilter)) {
                if ((!overflowOperationReaderCopy.hasNext() || overflowOperationReaderCopy.getCurrentOperation().getLeftBound() > pageMaxTime) &&
                        (!insertMemoryData.hasNext() || insertMemoryData.getCurrentMinTime() > pageMaxTime)) {
                    pageReader.skipCurrentPage();
                    usedPageOffset += lastAvailable - bis.available();
                    continue;
                }
            }

            InputStream page = pageReader.getNextPage();
            usedPageOffset += lastAvailable - bis.available();

            if (canCalcAggregationUsingHeader(pageMinTime, pageMaxTime, insertMemoryData)) {
                func.calculateValueFromPageHeader(pageHeader);
            } else {
                long[] timestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
                valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));
                result = ReaderUtils.readOnePage(dataType, timestamps, valueReader.decoder, page, result,
                        queryTimeFilter, queryValueFilter, insertMemoryData, overflowOperationReaderCopy);
                func.calculateValueFromDataPage(result);
                result.clearData();
            }
        }
    }

    /**
     * <p> An aggregation method implementation for the ValueReader aspect.
     * The aggregation will be calculated using the calculated common timestamps.
     *
     * @param aggregateFunction     aggregation function
     * @param aggregationTimestamps the timestamps which aggregation must satisfy
     * @return an int value, represents the read time index of timestamps
     * @throws IOException        TsFile read error
     * @throws ProcessorException get read info error
     */
    private int aggregateUsingTimestamps(ValueReader valueReader, AggregateFunction aggregateFunction,
                                         List<Long> aggregationTimestamps)
            throws IOException, ProcessorException {

        // the used count of aggregationTimestamps,
        // if all the time of aggregationTimestamps has been read, timestampsUsedIndex >= aggregationTimestamps.size()
        int timestampsUsedIndex = 0;

        if (usedPageOffset == -1) {
            usedPageOffset = valueReader.fileOffset;
        }

        // get column digest
        TsDigest digest = valueReader.getDigest();
        DigestForFilter digestFF = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
        logger.debug(String.format("calculate aggregation using given common timestamps, series time range is [%s, %s]," +
                "series value range is [%s, %s].", valueReader.getStartTime(), valueReader.getEndTime(),
                digestFF.getMinValue(), digestFF.getMaxValue()));

        if (aggregationTimestamps.size() > 0 && valueReader.getEndTime() < aggregationTimestamps.get(timestampsUsedIndex)) {
            logger.debug("current series does not satisfy the common timestamps");
            return timestampsUsedIndex;
        }

        DigestVisitor digestVisitor = new DigestVisitor();
        ByteArrayInputStream bis = valueReader.initBAISForOnePage(usedPageOffset);
        PageReader pageReader = new PageReader(bis, valueReader.compressionTypeName);

        // still has unread data
        while ((usedPageOffset - valueReader.fileOffset) < valueReader.totalSize) {
            int lastAvailable = bis.available();

            PageHeader pageHeader = pageReader.getNextPageHeader();
            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(pageMinTime, pageMaxTime);

            // the min value of common timestamps is greater than max time in this series
            if (aggregationTimestamps.get(timestampsUsedIndex) > pageMaxTime) {
                pageReader.skipCurrentPage();
                usedPageOffset += lastAvailable - bis.available();
                continue;
            }

            // if the current page doesn't satisfy the time filter
            if (queryTimeFilter != null && !digestVisitor.satisfy(timeDigestFF, queryTimeFilter)) {
                pageReader.skipCurrentPage();
                usedPageOffset += lastAvailable - bis.available();
                continue;
            }

            InputStream page = pageReader.getNextPage();
            long[] pageTimestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), valueReader.getDataType()));

            Pair<DynamicOneColumnData, Integer> pageData = ReaderUtils.readOnePageUsingCommonTime(
                    dataType, pageTimestamps, valueReader.decoder, page,
                    queryTimeFilter, aggregationTimestamps, timestampsUsedIndex, insertMemoryData, overflowOperationReaderCopy);

            if (pageData.left != null && pageData.left.valueLength > 0)
                aggregateFunction.calculateValueFromDataPage(pageData.left);

            timestampsUsedIndex = pageData.right;
            if (timestampsUsedIndex >= aggregationTimestamps.size())
                break;

            // update lastAggregationResult's pageOffset to the start of next page.
            // notice that : when the aggregationTimestamps is used all, but there still have unused page data,
            // in the next read batch process, the current page will be loaded
            usedPageOffset += lastAvailable - bis.available();
        }

        if (timestampsUsedIndex < aggregationTimestamps.size())
            usedPageOffset = -1;

        return timestampsUsedIndex;
    }

    /**
     * Examine whether the page header can be used to calculate the aggregation.
     * <p>
     * Notice that: the process could be optimized, if the query time filer and value filter contain the time and value of this page completely,
     * the aggregation calculation can also use the page header.
     * e.g. time filter is "time > 20 and time < 100", page time min and max is [50, 60], so the calculation can use the page header and does not need
     * to decompress the page.
     *
     * @param pageMinTime
     * @param pageMaxTime
     * @param insertMemoryData
     * @return
     * @throws IOException
     */
    private boolean canCalcAggregationUsingHeader(long pageMinTime, long pageMaxTime, InsertDynamicData insertMemoryData) throws IOException {

        while (overflowOperationReaderCopy.hasNext() && overflowOperationReaderCopy.getCurrentOperation().getRightBound() < pageMinTime)
            overflowOperationReaderCopy.next();

        // this page is changed by overflow update operation
        if (overflowOperationReaderCopy.hasNext() && overflowOperationReaderCopy.getCurrentOperation().getLeftBound() <= pageMaxTime) {
            return false;
        }

        // this page is changed by overflow insert operation
        if (insertMemoryData.hasNext()) {
            if (pageMinTime <= insertMemoryData.getCurrentMinTime() && insertMemoryData.getCurrentMinTime() <= pageMaxTime)
                return false;
            if (insertMemoryData.getCurrentMinTime() < pageMinTime)
                return false;
        }

        // no time filter and value filter
        if (queryTimeFilter == null && queryValueFilter == null)
            return true;

        // represents that whether the time data of this page are satisfied with the time filter
        boolean timeEligible = false;
        if (queryTimeFilter != null) {
            LongInterval timeInterval = (LongInterval) singleTimeVisitor.getInterval();
            for (int i = 0; i < timeInterval.count; i += 2) {

                long startTime = timeInterval.flag[i] ? timeInterval.v[i] : timeInterval.v[i] + 1;
                if (startTime > pageMaxTime)
                    break;
                long endTime = timeInterval.flag[i+1] ? timeInterval.v[i+1] : timeInterval.v[i+1] - 1;
                if (startTime <= pageMinTime && endTime >= pageMaxTime) {
                    return true;
                }
            }
        } else {
            timeEligible = true;
        }

        return timeEligible;
    }
}
