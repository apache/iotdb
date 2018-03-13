package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zhangjinrui on 2018/1/23.
 */
public class RawSeriesChunkReaderWithoutFilter implements TimeValuePairReader {

    private RawSeriesChunk rawSeriesChunk;
    private Iterator<TimeValuePair> timeValuePairIterator;

    public RawSeriesChunkReaderWithoutFilter(RawSeriesChunk rawSeriesChunk) {
        this.rawSeriesChunk = rawSeriesChunk;
        timeValuePairIterator = rawSeriesChunk.getIterator();
    }

    @Override
    public boolean hasNext() throws IOException {
        return timeValuePairIterator.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        return timeValuePairIterator.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {

    }
}
