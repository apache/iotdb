package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.TimeValuePairFilterVisitorImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zhangjinrui on 2018/1/23.
 */
public class RawSeriesChunkReaderWithFilter implements TimeValuePairReader {

    private RawSeriesChunk rawSeriesChunk;
    private Iterator<TimeValuePair> timeValuePairIterator;
    private Filter<?> filter;
    private TimeValuePairFilterVisitorImpl filterVisitor;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    public RawSeriesChunkReaderWithFilter(RawSeriesChunk rawSeriesChunk, Filter<?> filter) {
        this.rawSeriesChunk = rawSeriesChunk;
        timeValuePairIterator = rawSeriesChunk.getIterator();
        this.filter = filter;
        this.filterVisitor = new TimeValuePairFilterVisitorImpl();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedTimeValuePair) {
            return true;
        }
        while (timeValuePairIterator.hasNext()) {
            TimeValuePair timeValuePair = timeValuePairIterator.next();
            if (filterVisitor.satisfy(timeValuePair, filter)) {
                hasCachedTimeValuePair = true;
                cachedTimeValuePair = timeValuePair;
                break;
            }
        }
        return hasCachedTimeValuePair;
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
