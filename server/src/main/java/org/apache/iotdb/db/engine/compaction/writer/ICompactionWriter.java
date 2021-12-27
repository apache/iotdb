package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;

public interface ICompactionWriter {
    void startChunkGroup(String deviceId,boolean isAlign) throws IOException;

    void endChunkGroup() throws IOException;

    void startMeasurement(List<IMeasurementSchema> measurementSchemaList);

    void endMeasurement() throws IOException;

    void write(long timestamp,Object value) throws IOException;

    void write(long[] timestamps,Object values);

    void endFile() throws IOException;

    void close() throws IOException;

}
