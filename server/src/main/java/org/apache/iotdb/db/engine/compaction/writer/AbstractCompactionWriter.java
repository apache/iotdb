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
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;

/**
 * This writer is used for compaction to write data into target file. Notice that, In a cross space
 * compaction task, each seq source file has its corresponding target file. In an inner space
 * compaction task, there is only a target file.
 */
public abstract class AbstractCompactionWriter implements AutoCloseable {
  protected IChunkWriter chunkWriter;

  protected boolean isAlign;

  protected String deviceId;

  private long targetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

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

  protected void checkChunkSizeAndMayOpenANewChunk(TsFileIOWriter fileWriter) throws IOException {
    if (checkChunkSize()) {
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriter);
    }
  }

  private boolean checkChunkSize() {
    if (chunkWriter instanceof AlignedChunkWriterImpl) {
      if (((AlignedChunkWriterImpl) chunkWriter).getTimeChunkWriter().estimateMaxSeriesMemSize()
          > targetChunkSize) {
        return true;
      }
      for (ValueChunkWriter valueChunkWriter :
          ((AlignedChunkWriterImpl) chunkWriter).getValueChunkWriterList()) {
        if (valueChunkWriter.estimateMaxSeriesMemSize() > targetChunkSize) {
          return true;
        }
      }
      return false;
    } else {
      return chunkWriter.estimateMaxSeriesMemSize() > targetChunkSize;
    }
  }

  protected void writeRateLimit(long bytesLength) {
    MergeManager.mergeRateLimiterAcquire(
        MergeManager.getINSTANCE().getMergeWriteRateLimiter(), bytesLength);
  }
}
