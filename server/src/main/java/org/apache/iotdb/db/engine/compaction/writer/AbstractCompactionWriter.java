/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.constant.CompactionType;
import org.apache.iotdb.db.engine.compaction.constant.ProcessChunkType;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsRecorder;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;

public abstract class AbstractCompactionWriter implements AutoCloseable {
  protected int subTaskNum = IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  // Each sub task has its own chunk writer.
  // The index of the array corresponds to subTaskId.
  protected IChunkWriter[] chunkWriters = new IChunkWriter[subTaskNum];

  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  protected int[] chunkPointNumArray = new int[subTaskNum];

  // used to control the target chunk size
  public long targetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

  // used to control the point num of target chunk
  public long targetChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();

  // if unsealed chunk size is lower then this, then deserialize next chunk no matter it is
  // overlapped or not
  public long chunkSizeLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkSizeLowerBoundInCompaction();

  // if point num of unsealed chunk is lower then this, then deserialize next chunk no matter it is
  // overlapped or not
  public long chunkPointNumLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();

  // if unsealed page size is lower then this, then deserialize next page no matter it is
  // overlapped or not
  public long pageSizeLowerBoundInCompaction = chunkSizeLowerBoundInCompaction / 10;

  // if point num of unsealed page is lower then this, then deserialize next page no matter it is
  // overlapped or not
  public long pagePointNumLowerBoundInCompaction = chunkPointNumLowerBoundInCompaction / 10;

  // When num of points writing into target files reaches check point, then check chunk size
  public long checkPoint = IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum() / 10;

  private long lastCheckIndex = 0;

  private boolean enableMetrics =
      MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric();

  protected boolean isAlign;

  protected String deviceId;

  public abstract void startChunkGroup(String deviceId, boolean isAlign) throws IOException;

  public abstract void endChunkGroup() throws IOException;

  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
    chunkPointNumArray[subTaskId] = 0;
    lastCheckIndex = 0;
    if (isAlign) {
      chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
  }

  public abstract void endMeasurement(int subTaskId) throws IOException;

  public abstract void write(TimeValuePair timeValuePair, int subTaskId) throws IOException;

  public abstract void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException;

  public abstract void endFile() throws IOException;

  /**
   * Update startTime and endTime of the current device in each target resources, and check whether
   * to flush chunk metadatas or not.
   */
  public abstract void checkAndMayFlushChunkMetadata() throws IOException;

  protected void writeDataPoint(Long timestamp, TsPrimitiveType value, IChunkWriter iChunkWriter) {
    if (iChunkWriter instanceof ChunkWriterImpl) {
      ChunkWriterImpl chunkWriter = (ChunkWriterImpl) iChunkWriter;
      switch (chunkWriter.getDataType()) {
        case TEXT:
          chunkWriter.write(timestamp, value.getBinary());
          break;
        case DOUBLE:
          chunkWriter.write(timestamp, value.getDouble());
          break;
        case BOOLEAN:
          chunkWriter.write(timestamp, value.getBoolean());
          break;
        case INT64:
          chunkWriter.write(timestamp, value.getLong());
          break;
        case INT32:
          chunkWriter.write(timestamp, value.getInt());
          break;
        case FLOAT:
          chunkWriter.write(timestamp, value.getFloat());
          break;
        default:
          throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
      }
    } else {
      AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) iChunkWriter;
      for (TsPrimitiveType val : value.getVector()) {
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
