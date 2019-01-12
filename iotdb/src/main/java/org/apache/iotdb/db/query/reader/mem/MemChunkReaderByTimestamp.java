package org.apache.iotdb.db.query.reader.mem;

import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;

import java.io.IOException;
import java.util.Iterator;

public class MemChunkReaderByTimestamp implements EngineReaderByTimeStamp {
    private Iterator<TimeValuePair> timeValuePairIterator;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    public MemChunkReaderByTimestamp(TimeValuePairSorter readableChunk) {
        timeValuePairIterator = readableChunk.getIterator();
    }

    @Override
    public boolean hasNext() {
        if (hasCachedTimeValuePair) {
            return true;
        }
        return timeValuePairIterator.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasCachedTimeValuePair) {
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair;
        } else {
            return timeValuePairIterator.next();
        }
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {

    }

    //TODO consider change timeValuePairIterator to List structure, and use binary search instead of sequential search
    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        while (hasNext()) {
            TimeValuePair timeValuePair = next();
            long time = timeValuePair.getTimestamp();
            if (time == timestamp) {
                return timeValuePair.getValue();
            } else if (time > timestamp) {
                hasCachedTimeValuePair = true;
                cachedTimeValuePair = timeValuePair;
                break;
            }
        }
        return null;
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
