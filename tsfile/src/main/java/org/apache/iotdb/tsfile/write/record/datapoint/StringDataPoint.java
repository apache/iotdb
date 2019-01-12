package org.apache.iotdb.tsfile.write.record.datapoint;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * a subclass for Integer data type extends DataPoint
 *
 * @author kangrong
 * @see DataPoint DataPoint
 */
public class StringDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(StringDataPoint.class);
    /** actual value **/
    private Binary value;

    /**
     * constructor of StringDataPoint, the value type will be set automatically
     */
    public StringDataPoint(String measurementId, Binary v) {
        super(TSDataType.TEXT, measurementId);
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
    public void setString(Binary value) {
        this.value = value;
    }
}
