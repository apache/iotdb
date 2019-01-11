package cn.edu.tsinghua.tsfile.write.record.datapoint;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.write.chunk.IChunkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * a subclass for Boolean data type extends DataPoint
 *
 * @author kangrong
 * @see DataPoint DataPoint
 */
public class BooleanDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(BooleanDataPoint.class);
    /** actual value **/
    private boolean value;

    /**
     * constructor of BooleanDataPoint, the value type will be set automatically
     */
    public BooleanDataPoint(String measurementId, boolean v) {
        super(TSDataType.BOOLEAN, measurementId);
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
    public void setBoolean(boolean value) {
        this.value = value;
    }
}
