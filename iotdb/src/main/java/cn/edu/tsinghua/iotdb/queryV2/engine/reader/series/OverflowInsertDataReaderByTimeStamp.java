package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReaderByTimestamp;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;

public class OverflowInsertDataReaderByTimeStamp implements SeriesReaderByTimeStamp {

    private Long jobId;
    private PriorityMergeSortTimeValuePairReaderByTimestamp seriesReader;

    public OverflowInsertDataReaderByTimeStamp(Long jobId, PriorityMergeSortTimeValuePairReaderByTimestamp seriesReader){
        this.jobId = jobId;
        this.seriesReader = seriesReader;
    }

    @Override
    public boolean hasNext() throws IOException {
        return seriesReader.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        return seriesReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        seriesReader.skipCurrentTimeValuePair();
    }

    @Override
    public void close() throws IOException {
        seriesReader.close();
    }

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        return seriesReader.getValueInTimestamp(timestamp);
    }
}
