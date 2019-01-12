package org.apache.iotdb.db.query.externalsort.serialize;

import org.apache.iotdb.db.utils.TimeValuePair;

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
