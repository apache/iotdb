package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;

/**
 * Read TimeValuePair for one Series in BufferWrite which concludes all seriesChunks in TsFile and MemTable.
 * Created by zhangjinrui on 2018/1/15.
 */
public class SequentialSeriesReaderImpl implements SeriesReader {
    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return null;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
