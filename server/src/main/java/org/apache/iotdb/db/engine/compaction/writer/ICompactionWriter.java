package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;

public interface ICompactionWriter extends AutoCloseable {
  int subTaskNum = IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  // Each sub task has its own chunk writer.
  // The index of the array corresponds to subTaskId.
  IChunkWriter[] chunkWriters = new IChunkWriter[subTaskNum];

  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  int[] measurementPointCountArray = new int[subTaskNum];

  void startChunkGroup(String deviceId, boolean isAlign) throws IOException;

  void endChunkGroup() throws IOException;

  void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId);

  void endMeasurement(int subTaskId) throws IOException;

  void write(long timestamp, Object value, int subTaskId) throws IOException;

  void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException;

  void endFile() throws IOException;

  List<TsFileIOWriter> getFileIOWriter();

  public default void checkAndMayFlushChunkMetadata() throws IOException {
    List<TsFileIOWriter> writers = this.getFileIOWriter();
    for (TsFileIOWriter writer : writers) {
      writer.checkMetadataSizeAndMayFlush();
    }
  }

  void updateStartTimeAndEndTime(String device, long time, int subTaskId);
}
