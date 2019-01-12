package org.apache.iotdb.db.query.reader.mem;

import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;

import java.io.IOException;
import java.util.Iterator;

// TODO merge MemChunkReaderWithoutFilter and MemChunkReaderWithFilter to one class
public class MemChunkReaderWithoutFilter implements IReader {

    private Iterator<TimeValuePair> timeValuePairIterator;

    public MemChunkReaderWithoutFilter(TimeValuePairSorter readableChunk) {
        timeValuePairIterator = readableChunk.getIterator();
    }

    @Override
    public boolean hasNext() {
        return timeValuePairIterator.hasNext();
    }

    @Override
    public TimeValuePair next() {
        return timeValuePairIterator.next();
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
