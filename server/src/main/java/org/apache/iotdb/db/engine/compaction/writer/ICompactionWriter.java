package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;

public interface ICompactionWriter {

  default void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    throw new RuntimeException("Does not support this method");
  }

  default void startChunkGroup(int targetFileIndex, String deviceId, boolean isAlign) {
    throw new RuntimeException("Does not support this method");
  }

  void endChunkGroup() throws IOException;

  void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId);

  void endMeasurement(int subTaskId) throws IOException;
}
