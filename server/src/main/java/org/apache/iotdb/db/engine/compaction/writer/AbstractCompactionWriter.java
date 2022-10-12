package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.constant.ProcessChunkType;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsRecorder;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;

public abstract class AbstractCompactionWriter implements AutoCloseable {
  protected static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  // Each sub task has its own chunk writer.
  // The index of the array corresponds to subTaskId.
  protected IChunkWriter[] chunkWriters = new IChunkWriter[subTaskNum];

  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  protected int[] chunkPointNumArray = new int[subTaskNum];

  // used to control the target chunk size
  public static final long targetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

  // used to control the point num of target chunk
  public static final long targetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();

  // if unsealed chunk size is lower then this, then deserialize next chunk no matter it is
  // overlapped or not
  public static final long chunkSizeLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();

  // if point num of unsealed chunk is lower then this, then deserialize next chunk no matter it is
  // overlapped or not
  public static final long chunkPointNumLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();

  // if unsealed page size is lower then this, then deserialize next page no matter it is
  // overlapped or not
  public static final long pageSizeLowerBoundInCompaction = chunkSizeLowerBoundInCompaction / 10;

  // if point num of unsealed page is lower then this, then deserialize next page no matter it is
  // overlapped or not
  public static final long pagePointNumLowerBoundInCompaction =
      chunkPointNumLowerBoundInCompaction / 10;

  // When num of points writing into target files reaches check point, then check chunk size
  public static final long checkPoint =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum() / 10;

  private long lastCheckIndex = 0;

  private static final boolean enableMetrics =
      MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric();

  protected boolean isAlign;

  protected String deviceId;

  public abstract void startChunkGroup(String deviceId, boolean isAlign) throws IOException;

  public abstract void endChunkGroup() throws IOException;

  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
    chunkPointNumArray[subTaskId] = 0;
    if (isAlign) {
      chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
  }

  public abstract void endMeasurement(int subTaskId) throws IOException;

  public abstract void write(long timestamp, Object value, int subTaskId) throws IOException;

  public abstract void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException;

  public abstract void endFile() throws IOException;

  public abstract void checkAndMayFlushChunkMetadata() throws IOException;

  protected void writeDataPoint(Long timestamp, Object value, IChunkWriter iChunkWriter) {
    if (iChunkWriter instanceof ChunkWriterImpl) {
      ChunkWriterImpl chunkWriter = (ChunkWriterImpl) iChunkWriter;
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
      AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) iChunkWriter;
      for (TsPrimitiveType val : (TsPrimitiveType[]) value) {
        if (val == null) {
          chunkWriter.write(timestamp, null, true);
        } else {
          TSDataType tsDataType = chunkWriter.getCurrentValueChunkType();
          switch (tsDataType) {
            case TEXT:
              chunkWriter.write(timestamp, val.getBinary(), false);
              break;
            case DOUBLE:
              chunkWriter.write(timestamp, val.getDouble(), false);
              break;
            case BOOLEAN:
              chunkWriter.write(timestamp, val.getBoolean(), false);
              break;
            case INT64:
              chunkWriter.write(timestamp, val.getLong(), false);
              break;
            case INT32:
              chunkWriter.write(timestamp, val.getInt(), false);
              break;
            case FLOAT:
              chunkWriter.write(timestamp, val.getFloat(), false);
              break;
            default:
              throw new UnsupportedOperationException("Unknown data type " + tsDataType);
          }
        }
      }
      chunkWriter.write(timestamp);
    }
  }

  protected void flushChunkToFileWriter(TsFileIOWriter targetWriter, IChunkWriter iChunkWriter)
      throws IOException {
    writeRateLimit(iChunkWriter.estimateMaxSeriesMemSize());
    synchronized (targetWriter) {
      iChunkWriter.writeToFileWriter(targetWriter);
    }
  }

  private void writeRateLimit(long bytesLength) {
    CompactionTaskManager.mergeRateLimiterAcquire(
        CompactionTaskManager.getInstance().getMergeWriteRateLimiter(), bytesLength);
  }

  protected void checkChunkSizeAndMayOpenANewChunk(
      TsFileIOWriter fileWriter, IChunkWriter iChunkWriter, int subTaskId, boolean isCrossSpace)
      throws IOException {
    if (chunkPointNumArray[subTaskId] >= (lastCheckIndex + 1) * checkPoint) {
      // if chunk point num reaches the check point, then check if the chunk size over threshold
      lastCheckIndex = chunkPointNumArray[subTaskId] / checkPoint;
      if (iChunkWriter.checkIsChunkSizeOverThreshold(targetChunkSize, targetChunkPointNum)) {
        flushChunkToFileWriter(fileWriter, iChunkWriter);
        chunkPointNumArray[subTaskId] = 0;
        lastCheckIndex = 0;
        if (enableMetrics) {
          CompactionMetricsRecorder.recordWriteInfo(
              isCrossSpace
                  ? CompactionType.CROSS_COMPACTION
                  : CompactionType.INNER_UNSEQ_COMPACTION,
              ProcessChunkType.DESERIALIZE_CHUNK,
              isAlign,
              iChunkWriter.estimateMaxSeriesMemSize());
        }
      }
    }
  }
}
