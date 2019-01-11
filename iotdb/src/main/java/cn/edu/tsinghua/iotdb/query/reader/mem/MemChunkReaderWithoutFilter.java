package cn.edu.tsinghua.iotdb.query.reader.mem;

import cn.edu.tsinghua.iotdb.engine.memtable.TimeValuePairSorter;
import cn.edu.tsinghua.iotdb.query.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

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
