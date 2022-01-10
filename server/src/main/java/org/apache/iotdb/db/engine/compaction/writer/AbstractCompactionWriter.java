package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;

public abstract class AbstractCompactionWriter {
  protected IChunkWriter chunkWriter;

  protected boolean isAlign;

  protected String deviceId;

  public abstract void startChunkGroup(String deviceId, boolean isAlign) throws IOException;

  public abstract void endChunkGroup() throws IOException;

  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList) {
    if (isAlign) {
      chunkWriter = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriter = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
  }

  public abstract void endMeasurement() throws IOException;

  public abstract void write(long timestamp, Object value) throws IOException;

  public abstract void write(long[] timestamps, Object values);

  public abstract void endFile() throws IOException;

  public abstract void close() throws IOException;

  protected void writeDataPoint(Long timestamp, Object value) {
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
  }

  protected boolean checkChunkSizeAndMayOpenANewChunk() { // Todo:
    if (chunkWriter instanceof AlignedChunkWriterImpl) {
      if (((AlignedChunkWriterImpl) chunkWriter).getTimeChunkWriter().estimateMaxSeriesMemSize()
          > 2 * 1024) {
        return true;
      }
      for (ValueChunkWriter valueChunkWriter :
          ((AlignedChunkWriterImpl) chunkWriter).getValueChunkWriterList()) {
        if (valueChunkWriter.estimateMaxSeriesMemSize() > 2 * 1024) {
          return true;
        }
      }
      return false;
    } else {
      return chunkWriter.estimateMaxSeriesMemSize() > 2 * 1024;
    }
  }

  protected void writeRateLimit(long bytesLength) {
    MergeManager.mergeRateLimiterAcquire(
        MergeManager.getINSTANCE().getMergeWriteRateLimiter(), bytesLength);
  }
}
