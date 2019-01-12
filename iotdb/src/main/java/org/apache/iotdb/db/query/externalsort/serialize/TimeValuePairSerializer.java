package org.apache.iotdb.db.query.externalsort.serialize;


import org.apache.iotdb.db.utils.TimeValuePair;
import java.io.IOException;


public interface TimeValuePairSerializer {

    void write(TimeValuePair timeValuePair) throws IOException;

    void close() throws IOException;
}
