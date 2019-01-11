package cn.edu.tsinghua.iotdb.query.reader.merge;

import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;

import java.io.IOException;

/**
 * TODO the process of PriorityMergeReaderByTimestamp can be optimized.
 */
public class PriorityMergeReaderByTimestamp extends PriorityMergeReader implements EngineReaderByTimeStamp {

    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {

        if (hasCachedTimeValuePair) {
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                hasCachedTimeValuePair = false;
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                return null;
            }
        }

        while (hasNext()) {
            cachedTimeValuePair = next();
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                hasCachedTimeValuePair = false;
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                hasCachedTimeValuePair = true;
                return null;
            }
        }

        return null;
    }
}
