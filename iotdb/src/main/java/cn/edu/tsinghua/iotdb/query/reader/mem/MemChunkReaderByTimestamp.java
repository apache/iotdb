package cn.edu.tsinghua.iotdb.query.reader.mem;

import cn.edu.tsinghua.iotdb.engine.memtable.TimeValuePairSorter;
import cn.edu.tsinghua.iotdb.query.reader.merge.EngineReaderByTimeStamp;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

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
