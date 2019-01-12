package org.apache.iotdb.db.query.reader.mem;

import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;

import java.io.IOException;
import java.util.Iterator;


public class MemChunkReaderWithFilter implements IReader {

    private Iterator<TimeValuePair> timeValuePairIterator;
    private Filter filter;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    public MemChunkReaderWithFilter(TimeValuePairSorter readableChunk, Filter filter) {
        timeValuePairIterator = readableChunk.getIterator();
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        if (hasCachedTimeValuePair) {
            return true;
        }
        while (timeValuePairIterator.hasNext()) {
            TimeValuePair timeValuePair = timeValuePairIterator.next();
            if (filter.satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
                hasCachedTimeValuePair = true;
                cachedTimeValuePair = timeValuePair;
                break;
            }
        }
        return hasCachedTimeValuePair;
    }

    @Override
    public TimeValuePair next() {
        if (hasCachedTimeValuePair) {
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair;
        } else {
            return timeValuePairIterator.next();
        }
    }

    @Override
    public void skipCurrentTimeValuePair() {
        next();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean hasNextBatch() {
        return false;
    }

    @Override
    public BatchData nextBatch() {
        return null;
    }

    @Override
    public BatchData currentBatch() {
        return null;
    }
}
