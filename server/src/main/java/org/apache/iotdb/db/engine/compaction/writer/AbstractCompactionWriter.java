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
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
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

  protected boolean isAlign;

  protected String deviceId;

  private final long targetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();

  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  protected int[] measurementPointCountArray = new int[subTaskNum];

  public abstract void startChunkGroup(String deviceId, boolean isAlign) throws IOException;

  public abstract void endChunkGroup() throws IOException;

  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
    measurementPointCountArray[subTaskId] = 0;
    if (isAlign) {
      chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
  }

  public abstract void endMeasurement(int subTaskId) throws IOException;

  public abstract void write(long timestamp, Object value, int subTaskId) throws IOException;

  public abstract void write(
      TimeColumn timestamps, Column[] columns, String device, int subTaskId, int batchSize)
      throws IOException;

  public abstract void endFile() throws IOException;

  public abstract void close() throws IOException;

  public abstract List<TsFileIOWriter> getFileIOWriter();

  public abstract void updateStartTimeAndEndTime(String device, long time, int subTaskId);

  public void checkAndMayFlushChunkMetadata() throws IOException {
    List<TsFileIOWriter> writers = this.getFileIOWriter();
    for (TsFileIOWriter writer : writers) {
      writer.checkMetadataSizeAndMayFlush();
    }
  }
}
