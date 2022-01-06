package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;

public class InnerSpaceCompactionWriter implements ICompactionWriter {
  private TsFileIOWriter fileWriter;
  private IChunkWriter chunkWriter;
  private boolean isAlign;
  private String deviceId;

  public InnerSpaceCompactionWriter(TsFileResource targetFileResource) throws IOException {
    fileWriter = new RestorableTsFileIOWriter(targetFileResource.getTsFile());
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    fileWriter.startChunkGroup(deviceId);
    this.isAlign = isAlign;
    this.deviceId = deviceId;
  }

  @Override
  public void endChunkGroup() throws IOException {
    fileWriter.endChunkGroup();
  }

  @Override
  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList) {
    if (isAlign) {
      chunkWriter = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriter = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
  }

  @Override
  public void endMeasurement() throws IOException {
    writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
    chunkWriter.writeToFileWriter(fileWriter);
    chunkWriter = null;
  }

  @Override
  public void write(long timestamp, Object value) throws IOException {
    if (!isAlign) {
      ChunkWriterImpl chunkWriter = (ChunkWriterImpl) this.chunkWriter;
      switch (chunkWriter.getDataType()) {
        case TEXT:
          chunkWriter.write(timestamp, (Binary) value);
          break;
        case DOUBLE:
          chunkWriter.write(timestamp, (Double) value);
          break;
        case BOOLEAN:
          chunkWriter.write(timestamp, (Boolean) value);
          break;
        case INT64:
          chunkWriter.write(timestamp, (Long) value);
          break;
        case INT32:
          chunkWriter.write(timestamp, (Integer) value);
          break;
        case FLOAT:
          chunkWriter.write(timestamp, (Float) value);
          break;
        default:
          throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
      }
    } else {
      AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriter;
      for (TsPrimitiveType val : (TsPrimitiveType[]) value) {
        Object v = val == null ? null : val.getValue();
        // if val is null, then give it a random type
        TSDataType tsDataType = val == null ? TSDataType.TEXT : val.getDataType();
        boolean isNull = v == null;
        switch (tsDataType) {
          case TEXT:
            chunkWriter.write(timestamp, (Binary) v, isNull);
            break;
          case DOUBLE:
            chunkWriter.write(timestamp, (Double) v, isNull);
            break;
          case BOOLEAN:
            chunkWriter.write(timestamp, (Boolean) v, isNull);
            break;
          case INT64:
            chunkWriter.write(timestamp, (Long) v, isNull);
            break;
          case INT32:
            chunkWriter.write(timestamp, (Integer) v, isNull);
            break;
          case FLOAT:
            chunkWriter.write(timestamp, (Float) v, isNull);
            break;
          default:
            throw new UnsupportedOperationException("Unknown data type " + tsDataType);
        }
      }
      chunkWriter.write(timestamp);
    }
    if (chunkWriter.estimateMaxSeriesMemSize() > 2 * 1024) { // Todo:
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriter);
      fileWriter.endChunkGroup();
      fileWriter.startChunkGroup(deviceId);
    }
  }

  @Override
  public void write(long[] timestamps, Object values) {}

  @Override
  public void endFile() throws IOException {
    fileWriter.endFile();
  }

  @Override
  public void close() throws IOException {
    if (fileWriter != null && fileWriter.canWrite()) {
      fileWriter.close();
    }
    chunkWriter = null;
    fileWriter = null;
  }

  private static void writeRateLimit(long bytesLength) {
    MergeManager.mergeRateLimiterAcquire(
        MergeManager.getINSTANCE().getMergeWriteRateLimiter(), bytesLength);
  }

  public TsFileIOWriter getFileWriter() {
    return this.fileWriter;
  }
}
