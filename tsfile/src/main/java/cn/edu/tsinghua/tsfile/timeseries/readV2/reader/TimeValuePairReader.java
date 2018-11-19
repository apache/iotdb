package cn.edu.tsinghua.tsfile.timeseries.readV2.reader;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public interface TimeValuePairReader{

    boolean hasNext() throws IOException;

    TimeValuePair next() throws IOException;

    void skipCurrentTimeValuePair() throws IOException;

    void close() throws IOException;
}

