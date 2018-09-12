package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.SeriesWithOverflowOpReader;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;

/**
 * A single series data reader with seriesFilter, which has considered sequence insert data, overflow data, updata and delete operation.
 * */
public class IoTDBQueryWithFilterReader implements SeriesReader {

    private SeriesWithOverflowOpReader seriesWithOverflowOpReader;

    public IoTDBQueryWithFilterReader(QueryDataSource queryDataSource, SeriesFilter<?> filter) throws IOException {
        int priority = 1;
        //sequence insert data
        TsFilesReaderWithFilter tsFilesReader = new TsFilesReaderWithFilter(queryDataSource.getSeriesDataSource(), filter);
        PriorityTimeValuePairReader tsFilesReaderWithPriority = new PriorityTimeValuePairReader(
                tsFilesReader, new PriorityTimeValuePairReader.Priority(priority++));

        //overflow insert data
        OverflowInsertDataReader overflowInsertDataReader = SeriesReaderFactory.getInstance().
                createSeriesReaderForOverflowInsert(queryDataSource.getOverflowSeriesDataSource(), filter.getFilter());
        PriorityTimeValuePairReader overflowInsertDataReaderWithPriority = new PriorityTimeValuePairReader(
                overflowInsertDataReader, new PriorityTimeValuePairReader.Priority(priority++));

        //operation of update and delete
        OverflowOperationReader overflowOperationReader = queryDataSource.getOverflowSeriesDataSource().getUpdateDeleteInfoOfOneSeries().getOverflowUpdateOperationReaderNewInstance();

        PriorityMergeSortTimeValuePairReader insertDataReader = new PriorityMergeSortTimeValuePairReader(tsFilesReaderWithPriority, overflowInsertDataReaderWithPriority);
        seriesWithOverflowOpReader = new SeriesWithOverflowOpReader(insertDataReader, overflowOperationReader);
    }

    public IoTDBQueryWithFilterReader(QueryDataSource queryDataSource) throws IOException {
        int priority = 1;
        //sequence insert data
        TsFilesReaderWithFilter tsFilesReader = new TsFilesReaderWithFilter(queryDataSource.getSeriesDataSource(), null);
        PriorityTimeValuePairReader tsFilesReaderWithPriority = new PriorityTimeValuePairReader(
                tsFilesReader, new PriorityTimeValuePairReader.Priority(priority++));

        //overflow insert data
        OverflowInsertDataReader overflowInsertDataReader = SeriesReaderFactory.getInstance().
                createSeriesReaderForOverflowInsert(queryDataSource.getOverflowSeriesDataSource());
        PriorityTimeValuePairReader overflowInsertDataReaderWithPriority = new PriorityTimeValuePairReader(
                overflowInsertDataReader, new PriorityTimeValuePairReader.Priority(priority++));

        //operation of update and delete
        OverflowOperationReader overflowOperationReader = queryDataSource.getOverflowSeriesDataSource().getUpdateDeleteInfoOfOneSeries().getOverflowUpdateOperationReaderNewInstance();

        PriorityMergeSortTimeValuePairReader insertDataReader = new PriorityMergeSortTimeValuePairReader(tsFilesReaderWithPriority, overflowInsertDataReaderWithPriority);
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
        seriesWithOverflowOpReader.skipCurrentTimeValuePair();
    }

    @Override
    public void close() throws IOException {
        seriesWithOverflowOpReader.close();
    }
}
