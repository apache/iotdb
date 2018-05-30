package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReaderByTimestamp;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReaderByTimestamp;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReaderByTimeStamp;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.SeriesWithOverflowOpReader;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileByTimestampImpl;

import java.io.IOException;
import java.util.List;
/**
 * A single series data reader witch can return time-value pair in a time point.
 * First use func setCurrentTimestamp() set the time point. Then func hasNext() can judge the existences, and func next() fetch the time-pair pair in that point.
 * It has considered sequence insert data, overflow data, updata and delete operation.
 * */
public class IoTDBQueryWithTimestampsReader implements SeriesReaderByTimeStamp {

    private SeriesWithOverflowOpReader seriesWithOverflowOpReader;

    public IoTDBQueryWithTimestampsReader(QueryDataSource queryDataSource) throws IOException {
        int priority = 1;
        //sequence insert data
        TsFilesReaderWithTimeStamp tsFilesReader = new TsFilesReaderWithTimeStamp(queryDataSource.getSeriesDataSource());
        PriorityTimeValuePairReaderByTimestamp tsFilesReaderWithPriority = new PriorityTimeValuePairReaderByTimestamp(
                tsFilesReader, new PriorityTimeValuePairReader.Priority(priority++));

        //overflow insert data
        OverflowInsertDataReaderByTimeStamp overflowInsertDataReader = SeriesReaderFactory.getInstance().
                createSeriesReaderForOverflowInsertByTimestamp(queryDataSource.getOverflowSeriesDataSource());
        PriorityTimeValuePairReaderByTimestamp overflowInsertDataReaderWithPriority = new PriorityTimeValuePairReaderByTimestamp(
                overflowInsertDataReader, new PriorityTimeValuePairReader.Priority(priority++));

        //operation of update and delete
        OverflowOperationReader overflowOperationReader = queryDataSource.getOverflowSeriesDataSource().getUpdateDeleteInfoOfOneSeries().getOverflowUpdateOperationReaderNewInstance();

        PriorityMergeSortTimeValuePairReaderByTimestamp insertDataReader = new PriorityMergeSortTimeValuePairReaderByTimestamp(tsFilesReaderWithPriority, overflowInsertDataReaderWithPriority);
        seriesWithOverflowOpReader = new SeriesWithOverflowOpReader(insertDataReader, overflowOperationReader);
    }


    @Override
    public boolean hasNext() throws IOException {
        return seriesWithOverflowOpReader.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        return seriesWithOverflowOpReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        seriesWithOverflowOpReader.close();
    }

    @Override
    public void setCurrentTimestamp(long timestamp) {
        seriesWithOverflowOpReader.setCurrentTimeStamp(timestamp);
    }
}
