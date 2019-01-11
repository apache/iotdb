package cn.edu.tsinghua.iotdb.query.externalsort.serialize;

import cn.edu.tsinghua.iotdb.utils.TimeValuePair;

import java.io.IOException;


public interface TimeValuePairDeserializer {

    boolean hasNext() throws IOException;

    TimeValuePair next() throws IOException;

    /**
     * Close current deserializer
     * @throws IOException
     */
    void close() throws IOException;
}
