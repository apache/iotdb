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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractCrossCompactionWriter extends AbstractCompactionWriter {

  // target fileIOWriters
  protected List<CompactionTsFileWriter> targetFileWriters = new ArrayList<>();

  // source tsfiles
  private List<TsFileResource> seqTsFileResources;

  // Each sub task has its corresponding seq file index.
  // The index of the array corresponds to subTaskId.
  protected int[] seqFileIndexArray = new int[subTaskNum];

  // device end time in each source seq file
  protected final long[] currentDeviceEndTime;

  // whether each target file is empty or not
  protected final boolean[] isEmptyFile;

  // whether each target file has device data or not
  protected final boolean[] isDeviceExistedInTargetFiles;

  // current chunk group header size
  private int chunkGroupHeaderSize;

  protected List<TsFileResource> targetResources;

  protected AbstractCrossCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    currentDeviceEndTime = new long[seqFileResources.size()];
    isEmptyFile = new boolean[seqFileResources.size()];
    isDeviceExistedInTargetFiles = new boolean[targetResources.size()];
    long memorySizeForEachWriter =
        (long)
            ((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount()
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataSizeProportion()
                / targetResources.size());
    for (int i = 0; i < targetResources.size(); i++) {
      this.targetFileWriters.add(
          new CompactionTsFileWriter(
              targetResources.get(i).getTsFile(),
              memorySizeForEachWriter,
              CompactionType.CROSS_COMPACTION));
      isEmptyFile[i] = true;
    }
    this.seqTsFileResources = seqFileResources;
    this.targetResources = targetResources;
  }

  @Override
  public void startChunkGroup(IDeviceID deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    this.seqFileIndexArray = new int[subTaskNum];
    checkIsDeviceExistAndGetDeviceEndTime();
    for (CompactionTsFileWriter targetFileWriter : targetFileWriters) {
      chunkGroupHeaderSize = targetFileWriter.startChunkGroup(deviceId);
    }
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (int i = 0; i < seqTsFileResources.size(); i++) {
      CompactionTsFileWriter targetFileWriter = targetFileWriters.get(i);
      if (isDeviceExistedInTargetFiles[i]) {
        // update resource
        CompactionUtils.updateResource(targetResources.get(i), targetFileWriter, deviceId);
        targetFileWriter.endChunkGroup();
      } else {
        targetFileWriter.truncate(targetFileWriter.getPos() - chunkGroupHeaderSize);
      }
      isDeviceExistedInTargetFiles[i] = false;
    }
    seqFileIndexArray = null;
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    sealChunk(
        targetFileWriters.get(seqFileIndexArray[subTaskId]), chunkWriters[subTaskId], subTaskId);
    seqFileIndexArray[subTaskId] = 0;
  }

  @Override
  public void write(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    long timestamp = timeValuePair.getTimestamp();
    TsPrimitiveType value = timeValuePair.getValue();

    checkTimeAndMayFlushChunkToCurrentFile(timestamp, subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    writeDataPoint(timestamp, value, chunkWriters[subTaskId]);
    chunkPointNumArray[subTaskId]++;
    checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), chunkWriters[subTaskId], subTaskId);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    lastTime[subTaskId] = timestamp;
  }

  /** Write data in batch, only used for aligned device. */
  @Override
  public abstract void write(TsBlock tsBlock, int subTaskId) throws IOException;

  @Override
  public void endFile() throws IOException {
    for (int i = 0; i < isEmptyFile.length; i++) {
      targetFileWriters.get(i).endFile();
      // set empty target file to DELETED
      if (isEmptyFile[i]) {
        targetResources.get(i).forceMarkDeleted();
      }
    }
  }

  @Override
  public void close() throws IOException {
    for (CompactionTsFileWriter targetWriter : targetFileWriters) {
      if (targetWriter != null && targetWriter.canWrite()) {
        targetWriter.close();
      }
    }
    targetFileWriters = null;
    seqTsFileResources = null;
  }

  @Override
  public void checkAndMayFlushChunkMetadata() throws IOException {
    for (CompactionTsFileWriter fileIoWriter : targetFileWriters) {
      fileIoWriter.checkMetadataSizeAndMayFlush();
    }
  }

  /**
   * Find the index of the target file to be inserted according to the data time. Notice: unsealed
   * chunk should be flushed to current file before moving target file index.<br>
   * If the seq file is deleted for various reasons, the following two situations may occur when
   * selecting the source files: (1) unseq files may have some devices or measurements which are not
   * exist in seq files. (2) timestamp of one timeseries in unseq files may later than any seq
   * files. Then write these data into the last target file.
   *
   * @throws IOException if io errors occurred
   * @throws RuntimeException if timeStamp is sooner than the last time
   */
  protected void checkTimeAndMayFlushChunkToCurrentFile(long timestamp, int subTaskId)
      throws IOException {
    checkPreviousTimestamp(timestamp, subTaskId);

    int fileIndex = seqFileIndexArray[subTaskId];
    boolean hasFlushedCurrentChunk = false;
    // If timestamp is later than the current source seq tsfile, then flush chunk writer and move to
    // next file
    while (timestamp > currentDeviceEndTime[fileIndex]
        && fileIndex != seqTsFileResources.size() - 1) {
      if (!hasFlushedCurrentChunk) {
        // Flush chunk to current file before moving target file index
        sealChunk(targetFileWriters.get(fileIndex), chunkWriters[subTaskId], subTaskId);
        hasFlushedCurrentChunk = true;
      }
      seqFileIndexArray[subTaskId] = ++fileIndex;
    }
  }

  private void checkIsDeviceExistAndGetDeviceEndTime() throws IOException {
    int fileIndex = 0;
    while (fileIndex < seqTsFileResources.size()) {
      if (seqTsFileResources.get(fileIndex).getTimeIndexType() == 1) {
        // the timeIndexType of resource is deviceTimeIndex
        currentDeviceEndTime[fileIndex] = seqTsFileResources.get(fileIndex).getEndTime(deviceId);
      } else {
        long endTime = Long.MIN_VALUE;
        // Fast compaction get reader from cache map, while read point compaction get reader from
        // FileReaderManager
        Map<String, TimeseriesMetadata> deviceMetadataMap =
            getFileReader(seqTsFileResources.get(fileIndex)).readDeviceMetadata(deviceId);
        for (Map.Entry<String, TimeseriesMetadata> entry : deviceMetadataMap.entrySet()) {
          long tmpStartTime = entry.getValue().getStatistics().getStartTime();
          long tmpEndTime = entry.getValue().getStatistics().getEndTime();
          if (tmpEndTime >= tmpStartTime && endTime < tmpEndTime) {
            endTime = tmpEndTime;
          }
        }
        currentDeviceEndTime[fileIndex] = endTime;
      }

      fileIndex++;
    }
  }

  @Override
  public long getWriterSize() throws IOException {
    long totalSize = 0;
    for (CompactionTsFileWriter writer : targetFileWriters) {
      totalSize += writer.getPos();
    }
    return totalSize;
  }

  @Override
  public void setSchemaForAllTargetFile(List<Schema> schemas) {
    for (int i = 0; i < targetFileWriters.size(); i++) {
      targetFileWriters.get(i).setSchema(schemas.get(i));
    }
  }

  protected abstract TsFileSequenceReader getFileReader(TsFileResource resource) throws IOException;
}
