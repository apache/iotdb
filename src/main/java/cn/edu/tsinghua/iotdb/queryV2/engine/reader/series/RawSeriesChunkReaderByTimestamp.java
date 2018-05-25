package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.Iterator;

public class RawSeriesChunkReaderByTimestamp implements SeriesReaderByTimeStamp {
    private Iterator<TimeValuePair> timeValuePairIterator;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;
    private long currentTimeStamp;

    public RawSeriesChunkReaderByTimestamp(RawSeriesChunk rawSeriesChunk) {
        timeValuePairIterator = rawSeriesChunk.getIterator();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() == currentTimeStamp) {
            return true;
        }
        else {
            hasCachedTimeValuePair = false;
        }
        while (timeValuePairIterator.hasNext()) {
            TimeValuePair timeValuePair = timeValuePairIterator.next();
            if (timeValuePair.getTimestamp() == currentTimeStamp) {
                hasCachedTimeValuePair = true;
                cachedTimeValuePair = timeValuePair;
                break;
            }
            else if(timeValuePair.getTimestamp() > currentTimeStamp){
                break;
            }
        }
        return hasCachedTimeValuePair;
    }

    @Override
    public void setCurrentTimestamp(long currentTimeStamp) {
        this.currentTimeStamp = currentTimeStamp;
    }


    @Override
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            return cachedTimeValuePair;
        } else {
            return null;
        }
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {

    }
}
