package cn.edu.tsinghua.tsfile.write.record.datapoint;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.write.chunk.IChunkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * a subclass for Long data type extends DataPoint
 *
 * @author kangrong
 * @see DataPoint DataPoint
 */
public class LongDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(LongDataPoint.class);
    /** actual value **/
    private long value;

    /**
     * constructor of LongDataPoint, the value type will be set automatically
     */
    public LongDataPoint(String measurementId, long v) {
        super(TSDataType.INT64, measurementId);
        this.value = v;
    }

    @Override
    public void writeTo(long time, IChunkWriter writer) throws IOException {
        if (writer == null) {
            LOG.warn("given IChunkWriter is null, do nothing and return");
            return;
        }
        writer.write(time, value);

    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setLong(long value) {
        this.value = value;
    }
}
