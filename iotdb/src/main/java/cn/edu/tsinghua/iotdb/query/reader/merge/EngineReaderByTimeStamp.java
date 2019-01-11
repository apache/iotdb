package cn.edu.tsinghua.iotdb.query.reader.merge;

import cn.edu.tsinghua.iotdb.query.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;

import java.io.IOException;

public interface EngineReaderByTimeStamp extends IReader {

    /**
     * Given a timestamp, the reader is supposed to return the corresponding value in the
     * timestamp. If no value in this timestamp, null will be returned.
     */
    TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException;
}
