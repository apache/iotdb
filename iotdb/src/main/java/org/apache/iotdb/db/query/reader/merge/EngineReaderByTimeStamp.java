package org.apache.iotdb.db.query.reader.merge;

import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TsPrimitiveType;

import java.io.IOException;

public interface EngineReaderByTimeStamp extends IReader {

    /**
     * Given a timestamp, the reader is supposed to return the corresponding value in the
     * timestamp. If no value in this timestamp, null will be returned.
     */
    TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException;
}
