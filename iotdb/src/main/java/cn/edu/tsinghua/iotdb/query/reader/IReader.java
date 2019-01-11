package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

import java.io.IOException;

/**
 * <p> Vital read interface.
 * Batch method is used to increase query speed.
 * Get a batch data every time will be faster than get one point every time.
 */
public interface IReader {

    boolean hasNext() throws IOException;

    TimeValuePair next() throws IOException;

    void skipCurrentTimeValuePair() throws IOException;

    void close() throws IOException;

    boolean hasNextBatch();

    BatchData nextBatch();

    BatchData currentBatch();
}

