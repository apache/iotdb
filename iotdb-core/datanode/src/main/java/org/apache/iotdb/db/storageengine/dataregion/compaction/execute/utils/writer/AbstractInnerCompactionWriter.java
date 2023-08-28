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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.io.IOException;

public abstract class AbstractInnerCompactionWriter extends AbstractCompactionWriter {
  protected CompactionTsFileWriter fileWriter;

  protected boolean isEmptyFile;

  protected TsFileResource targetResource;

  protected long targetPageSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();

  protected long targetPagePointNum =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  protected AbstractInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    long sizeForFileWriter =
        (long)
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion());
    boolean enableMemoryControl = IoTDBDescriptor.getInstance().getConfig().isEnableMemControl();
    this.fileWriter =
        new CompactionTsFileWriter(
            targetFileResource.getTsFile(),
            enableMemoryControl,
            sizeForFileWriter,
            CompactionType.INNER_UNSEQ_COMPACTION);
    this.targetResource = targetFileResource;
    isEmptyFile = true;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    fileWriter.startChunkGroup(deviceId);
    this.isAlign = isAlign;
    this.deviceId = deviceId;
  }

  @Override
  public void endChunkGroup() throws IOException {
    CompactionUtils.updateResource(targetResource, fileWriter, deviceId);
    fileWriter.endChunkGroup();
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    sealChunk(fileWriter, chunkWriters[subTaskId], subTaskId);
  }

  @Override
  public void write(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    writeDataPoint(timeValuePair.getTimestamp(), timeValuePair.getValue(), chunkWriters[subTaskId]);
    chunkPointNumArray[subTaskId]++;
    checkChunkSizeAndMayOpenANewChunk(fileWriter, chunkWriters[subTaskId], subTaskId);
    isEmptyFile = false;
  }

  @Override
  public abstract void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException;

  @Override
  public void endFile() throws IOException {
    fileWriter.endFile();
    if (isEmptyFile) {
      targetResource.forceMarkDeleted();
    }
  }

  @Override
  public void close() throws Exception {
    if (fileWriter != null && fileWriter.canWrite()) {
      fileWriter.close();
    }
    fileWriter = null;
  }

  @Override
  public void checkAndMayFlushChunkMetadata() throws IOException {
    fileWriter.checkMetadataSizeAndMayFlush();
  }

  @Override
  public long getWriterSize() throws IOException {
    return fileWriter.getPos();
  }
}
