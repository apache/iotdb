package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.constant.ProcessChunkType;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsRecorder;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;

public class CompactionWriterUtils {
  private static final long targetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

  // When num of points writing into target files reaches, then check chunk size
  public static final long checkPoint =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum() / 10;

  private static final boolean enableMetrics =
      MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric();

  public static void writeDataPoint(
      Long timestamp,
      Object value,
      boolean isAlign,
      IChunkWriter iChunkWriter,
      TsFileIOWriter tsFileIOWriter,
      boolean isCrossSpace)
      throws IOException {
    if (!isAlign) {
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

    // check chunk size and may open a new chunk
    if (tsFileIOWriter != null) {
      checkChunkSizeAndMayOpenANewChunk(tsFileIOWriter, iChunkWriter, isCrossSpace);
    }
  }

  public static void writeTVPair(
      TimeValuePair timeValuePair,
      IChunkWriter iChunkWriter,
      TsFileIOWriter tsFileIOWriter,
      boolean isCrossSpace)
      throws IOException {
    ChunkWriterImpl chunkWriter = (ChunkWriterImpl) iChunkWriter;
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case DOUBLE:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case BOOLEAN:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }

    // check chunk size and may open a new chunk
    if (tsFileIOWriter != null) {
      checkChunkSizeAndMayOpenANewChunk(tsFileIOWriter, iChunkWriter, isCrossSpace);
    }
  }

  public static void flushChunkToFileWriter(TsFileIOWriter targetWriter, IChunkWriter iChunkWriter)
      throws IOException {
    writeRateLimit(iChunkWriter.estimateMaxSeriesMemSize());
    synchronized (targetWriter) {
      iChunkWriter.writeToFileWriter(targetWriter);
    }
  }

  private static void writeRateLimit(long bytesLength) {
    CompactionTaskManager.mergeRateLimiterAcquire(
        CompactionTaskManager.getInstance().getMergeWriteRateLimiter(), bytesLength);
  }

  public static void checkChunkSizeAndMayOpenANewChunk(
      TsFileIOWriter fileWriter, IChunkWriter iChunkWriter, boolean isCrossSpace)
      throws IOException {
    if (checkChunkSize(iChunkWriter)) {
      flushChunkToFileWriter(fileWriter, iChunkWriter);
      if (enableMetrics) {
        boolean isAlign = iChunkWriter instanceof AlignedChunkWriterImpl;
        CompactionMetricsRecorder.recordWriteInfo(
            isCrossSpace ? CompactionType.CROSS_COMPACTION : CompactionType.INNER_UNSEQ_COMPACTION,
            ProcessChunkType.DESERIALIZE_CHUNK,
            isAlign,
            iChunkWriter.estimateMaxSeriesMemSize());
      }
    }
  }

  private static boolean checkChunkSize(IChunkWriter iChunkWriter) {
    if (iChunkWriter instanceof AlignedChunkWriterImpl) {
      return ((AlignedChunkWriterImpl) iChunkWriter).checkIsChunkSizeOverThreshold(targetChunkSize);
    } else {
      return iChunkWriter.estimateMaxSeriesMemSize() > targetChunkSize;
    }
  }
}
