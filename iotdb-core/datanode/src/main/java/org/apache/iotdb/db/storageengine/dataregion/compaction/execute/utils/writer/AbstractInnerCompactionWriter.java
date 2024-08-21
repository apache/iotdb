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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionTableSchemaCollector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.schema.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class AbstractInnerCompactionWriter extends AbstractCompactionWriter {
  protected CompactionTsFileWriter fileWriter;
  protected List<TsFileResource> targetResources;
  protected int currentFileIndex;
  protected long endedFileSize = 0;
  protected List<Schema> schemas;

  protected final long memoryBudgetForFileWriter =
      (long)
          ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
              / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
              * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion());

  protected AbstractInnerCompactionWriter(TsFileResource targetFileResource) {
    this(Collections.singletonList(targetFileResource));
  }

  protected AbstractInnerCompactionWriter(List<TsFileResource> targetFileResources) {
    this.targetResources = targetFileResources;
  }

  @Override
  public void startChunkGroup(IDeviceID deviceId, boolean isAlign) throws IOException {
    fileWriter = getAvailableWriter();
    fileWriter.startChunkGroup(deviceId);
    this.isAlign = isAlign;
    this.deviceId = deviceId;
  }

  private CompactionTsFileWriter getAvailableWriter() throws IOException {
    if (fileWriter == null) {
      useNewWriter();
      return fileWriter;
    }
    boolean shouldSwitchToNextWriter =
        fileWriter.getPos()
                >= IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize()
            && (currentFileIndex != targetResources.size() - 1);
    if (shouldSwitchToNextWriter) {
      rollCompactionFileWriter();
    }
    return fileWriter;
  }

  private void rollCompactionFileWriter() throws IOException {
    fileWriter.endFile();
    endedFileSize += fileWriter.getFile().length();
    if (fileWriter.isEmptyTargetFile()) {
      targetResources.get(currentFileIndex).forceMarkDeleted();
    }
    fileWriter = null;

    currentFileIndex++;
    useNewWriter();
  }

  private void useNewWriter() throws IOException {
    fileWriter =
        new CompactionTsFileWriter(
            targetResources.get(currentFileIndex).getTsFile(),
            memoryBudgetForFileWriter,
            targetResources.get(currentFileIndex).isSeq()
                ? CompactionType.INNER_SEQ_COMPACTION
                : CompactionType.INNER_UNSEQ_COMPACTION);
    fileWriter.setSchema(CompactionTableSchemaCollector.copySchema(schemas.get(0)));
  }

  @Override
  public void endChunkGroup() throws IOException {
    CompactionUtils.updateResource(targetResources.get(currentFileIndex), fileWriter, deviceId);
    fileWriter.endChunkGroup();
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    sealChunk(fileWriter, chunkWriters[subTaskId], subTaskId);
  }

  @Override
  public void write(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    checkPreviousTimestamp(timeValuePair.getTimestamp(), subTaskId);
    writeDataPoint(timeValuePair.getTimestamp(), timeValuePair.getValue(), chunkWriters[subTaskId]);
    chunkPointNumArray[subTaskId]++;
    checkChunkSizeAndMayOpenANewChunk(fileWriter, chunkWriters[subTaskId], subTaskId);
    lastTime[subTaskId] = timeValuePair.getTimestamp();
  }

  @Override
  public abstract void write(TsBlock tsBlock, int subTaskId) throws IOException;

  @Override
  public void endFile() throws IOException {
    for (int i = currentFileIndex + 1; i < targetResources.size(); i++) {
      targetResources.get(i).forceMarkDeleted();
    }
    if (fileWriter == null || fileWriter.isEmptyTargetFile()) {
      targetResources.get(currentFileIndex).forceMarkDeleted();
      return;
    }
    fileWriter.endFile();
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
  public void setSchemaForAllTargetFile(List<Schema> schemas) {
    this.schemas = schemas;
  }

  @Override
  public long getWriterSize() throws IOException {
    return endedFileSize + (fileWriter == null ? 0 : fileWriter.getPos());
  }
}
