/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionMetricsManager;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.constant.ProcessChunkType;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractCompactionWriter implements AutoCloseable {
  protected static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  // subTaskId -> IChunkWriter
  protected Map<Integer, IChunkWriter> chunkWriterMap = new ConcurrentHashMap<>();

  protected boolean isAlign;

  protected String deviceId;
  private final long targetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private final boolean enableMetrics =
      MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric();

  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  protected int[] measurementPointCountArray = new int[subTaskNum];

  public abstract void startChunkGroup(String deviceId, boolean isAlign) throws IOException;

  public abstract void endChunkGroup() throws IOException;

  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
    measurementPointCountArray[subTaskId] = 0;
    if (isAlign) {
      chunkWriterMap.put(subTaskId, new AlignedChunkWriterImpl(measurementSchemaList));
    } else {
      chunkWriterMap.put(subTaskId, new ChunkWriterImpl(measurementSchemaList.get(0), true));
    }
  }

  public abstract void endMeasurement(int subTaskId) throws IOException;

  public abstract void write(long timestamp, Object value, int subTaskId) throws IOException;

  public abstract void write(long[] timestamps, Object values);

  public abstract void endFile() throws IOException;

  public abstract void close() throws IOException;

  protected void writeDataPoint(Long timestamp, Object value, int subTaskId) {
    if (!isAlign) {
      ChunkWriterImpl chunkWriter = (ChunkWriterImpl) this.chunkWriterMap.get(subTaskId);
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
      AlignedChunkWriterImpl chunkWriter =
          (AlignedChunkWriterImpl) this.chunkWriterMap.get(subTaskId);
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
    measurementPointCountArray[subTaskId] += 1;
  }

  protected void checkChunkSizeAndMayOpenANewChunk(TsFileIOWriter fileWriter, int subTaskId)
      throws IOException {
    if (measurementPointCountArray[subTaskId] % 10 == 0 && checkChunkSize(subTaskId)) {
      writeRateLimit(chunkWriterMap.get(subTaskId).estimateMaxSeriesMemSize());
      CompactionMetricsManager.recordWriteInfo(
          this instanceof CrossSpaceCompactionWriter
              ? CompactionType.CROSS_COMPACTION
              : CompactionType.INNER_UNSEQ_COMPACTION,
          ProcessChunkType.DESERIALIZE_CHUNK,
          this.isAlign,
          chunkWriterMap.get(subTaskId).estimateMaxSeriesMemSize());
      synchronized (fileWriter) {
        chunkWriterMap.get(subTaskId).writeToFileWriter(fileWriter);
      }
    }
  }

  protected boolean checkChunkSize(int subTaskId) {
    if (chunkWriterMap.get(subTaskId) instanceof AlignedChunkWriterImpl) {
      return ((AlignedChunkWriterImpl) chunkWriterMap.get(subTaskId))
          .checkIsChunkSizeOverThreshold(targetChunkSize);
    } else {
      return chunkWriterMap.get(subTaskId).estimateMaxSeriesMemSize() > targetChunkSize;
    }
  }

  protected void writeRateLimit(long bytesLength) {
    CompactionTaskManager.mergeRateLimiterAcquire(
        CompactionTaskManager.getInstance().getMergeWriteRateLimiter(), bytesLength);
  }

  public abstract List<TsFileIOWriter> getFileIOWriter();
}
