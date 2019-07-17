/**
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

package org.apache.iotdb.db.engine.merge.task;

import static org.apache.iotdb.db.utils.MergeUtils.writeChunkWithoutUnseq;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.ForceAppendTsFileWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeFileTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeFileTask.class);

  private String taskName;
  private Map<TsFileResource, Integer> mergedChunkCnt;
  private Map<TsFileResource, Integer> unmergedChunkCnt;
  private Map<TsFileResource, Map<Path, List<Long>>> unmergedChunkStartTimes;
  private MergeLogger mergeLogger;
  private MergeResource resource;
  private List<TsFileResource> unmergedFiles;

  MergeFileTask(String taskName,
      Map<TsFileResource, Integer> mergedChunkCnt,
      Map<TsFileResource, Integer> unmergedChunkCnt,
      Map<TsFileResource, Map<Path, List<Long>>> unmergedChunkStartTimes,
      MergeLogger mergeLogger, MergeResource resource, List<TsFileResource> unmergedSeqFiles) {
    this.taskName = taskName;
    this.mergedChunkCnt = mergedChunkCnt;
    this.unmergedChunkCnt = unmergedChunkCnt;
    this.unmergedChunkStartTimes = unmergedChunkStartTimes;
    this.mergeLogger = mergeLogger;
    this.resource = resource;
    this.unmergedFiles = unmergedSeqFiles;
  }

  void mergeFiles() throws IOException {
    // decide whether to write the unmerged chunks to the merge files or to move the merged data
    // back to the origin seqFile's
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} files", taskName, unmergedFiles.size());
    }
    long startTime = System.currentTimeMillis();
    int cnt = 0;
    for (TsFileResource seqFile : unmergedFiles) {
      int mergedChunkNum = mergedChunkCnt.getOrDefault(seqFile, 0);
      int unmergedChunkNum = unmergedChunkCnt.getOrDefault(seqFile, 0);
      if (mergedChunkNum >= unmergedChunkNum) {
        // move the unmerged data to the new file
        if (logger.isInfoEnabled()) {
          logger.info("{} moving unmerged data of {} to the merged file, {} merged chunks, {} "
              + "unmerged chunks", taskName, seqFile.getFile().getName(), mergedChunkNum, unmergedChunkNum);
        }
        moveUnmergedToNew(seqFile);
      } else {
        // move the merged data to the old file
        if (logger.isInfoEnabled()) {
          logger.info("{} moving merged data of {} to the old file {} merged chunks, {} "
              + "unmerged chunks", taskName, seqFile.getFile().getName(), mergedChunkNum, unmergedChunkNum);
        }
        moveMergedToOld(seqFile);
      }
      cnt ++;
      if (logger.isInfoEnabled()) {
        logger.debug("{} has merged {}/{} files", taskName, cnt, unmergedFiles.size());
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} has merged all files after {}ms", taskName, System.currentTimeMillis() - startTime);
    }
    mergeLogger.logMergeEnd();
  }

  private void moveMergedToOld(TsFileResource seqFile) throws IOException {
    int mergedChunkNum = mergedChunkCnt.getOrDefault(seqFile, 0);
    if (mergedChunkNum == 0) {
      resource.removeFileWriter(seqFile);
      return;
    }

    seqFile.getMergeQueryLock().writeLock().lock();
    try {
      TsFileIOWriter oldFileWriter;
      try {
        oldFileWriter = new ForceAppendTsFileWriter(seqFile.getFile());
        mergeLogger.logFileMergeStart(seqFile.getFile(), ((ForceAppendTsFileWriter) oldFileWriter).getTruncatePosition());
        logger.debug("{} moving merged chunks of {} to the old file", taskName, seqFile);
        ((ForceAppendTsFileWriter) oldFileWriter).doTruncate();
      } catch (TsFileNotCompleteException e) {
        // this file may already be truncated if this merge is a system reboot merge
        oldFileWriter = new RestorableTsFileIOWriter(seqFile.getFile());
      }

      oldFileWriter.filterChunks(unmergedChunkStartTimes.get(seqFile));

      RestorableTsFileIOWriter newFileWriter = resource.getMergeFileWriter(seqFile);
      newFileWriter.close();
      try ( TsFileSequenceReader newFileReader =
          new TsFileSequenceReader(newFileWriter.getFile().getPath())) {
        ChunkLoader chunkLoader = new ChunkLoaderImpl(newFileReader);
        List<ChunkGroupMetaData> chunkGroupMetadataList = newFileWriter.getChunkGroupMetaDatas();
        if (logger.isDebugEnabled()) {
          logger.debug("{} find {} merged chunk groups", taskName, chunkGroupMetadataList.size());
        }
        for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetadataList) {
          writeMergedChunkGroup(chunkGroupMetaData, chunkLoader, oldFileWriter);
        }
      }
      oldFileWriter.endFile(new FileSchema(oldFileWriter.getKnownSchema()));

      seqFile.serialize();
      mergeLogger.logFileMergeEnd(seqFile.getFile());
      logger.debug("{} moved merged chunks of {} to the old file", taskName, seqFile);

      newFileWriter.getFile().delete();
    } finally {
      seqFile.getMergeQueryLock().writeLock().unlock();
    }
  }

  private void writeMergedChunkGroup(ChunkGroupMetaData chunkGroupMetaData,
      ChunkLoader chunkLoader, TsFileIOWriter fileWriter)
      throws IOException {
    fileWriter.startChunkGroup(chunkGroupMetaData.getDeviceID());
    long maxVersion = Long.MIN_VALUE;
    for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
      Path path = new Path(chunkGroupMetaData.getDeviceID(), chunkMetaData.getMeasurementUid());
      MeasurementSchema measurementSchema = resource.getSchema(path);
      IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);
      Chunk chunk = chunkLoader.getChunk(chunkMetaData);
      writeChunkWithoutUnseq(chunk, chunkWriter, measurementSchema);
      chunkWriter.writeToFileWriter(fileWriter);
      maxVersion = maxVersion < chunkMetaData.getVersion() ? chunkMetaData.getVersion() :
          maxVersion;
    }
    fileWriter.endChunkGroup(maxVersion + 1);
  }

  private void moveUnmergedToNew(TsFileResource seqFile) throws IOException {
    Map<Path, List<Long>> fileUnmergedChunkStartTimes = this.unmergedChunkStartTimes.get(seqFile);
    RestorableTsFileIOWriter fileWriter = resource.getMergeFileWriter(seqFile);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(resource.getFileReader(seqFile));

    mergeLogger.logFileMergeStart(fileWriter.getFile(), fileWriter.getFile().length());
    logger.debug("{} moving unmerged chunks of {} to the new file", taskName, seqFile);

    int unmergedChunkNum = unmergedChunkCnt.getOrDefault(seqFile, 0);

    if (unmergedChunkNum > 0) {
      for (Entry<Path, List<Long>> entry : fileUnmergedChunkStartTimes.entrySet()) {
        Path path = entry.getKey();
        List<Long> chunkStartTimes = entry.getValue();
        if (chunkStartTimes.isEmpty()) {
          continue;
        }

        List<ChunkMetaData> chunkMetaDataList = resource.queryChunkMetadata(path, seqFile);
        MeasurementSchema measurementSchema = resource.getSchema(path);
        IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);

        if (logger.isDebugEnabled()) {
          logger.debug("{} find {} unmerged chunks", taskName, chunkMetaDataList.size());
        }

        fileWriter.startChunkGroup(path.getDevice());
        long maxVersion = writeUnmergedChunks(chunkStartTimes, chunkMetaDataList, chunkLoader,
            chunkWriter, measurementSchema, fileWriter);
        fileWriter.endChunkGroup(maxVersion + 1);
      }
    }

    fileWriter.endFile(new FileSchema(fileWriter.getKnownSchema()));

    seqFile.serialize();
    mergeLogger.logFileMergeEnd(fileWriter.getFile());
    logger.debug("{} moved unmerged chunks of {} to the new file", taskName, seqFile);

    seqFile.getMergeQueryLock().writeLock().lock();
    try {
      seqFile.getFile().delete();
      FileUtils.moveFile(fileWriter.getFile(), seqFile.getFile());
    } finally {
      seqFile.getMergeQueryLock().writeLock().unlock();
    }
  }

  private long writeUnmergedChunks(List<Long> chunkStartTimes,
      List<ChunkMetaData> chunkMetaDataList, ChunkLoader chunkLoader, IChunkWriter chunkWriter,
      MeasurementSchema measurementSchema, RestorableTsFileIOWriter fileWriter) throws IOException {
    long maxVersion = 0;
    int chunkIdx = 0;
    for (Long startTime : chunkStartTimes) {
      for (; chunkIdx < chunkMetaDataList.size(); chunkIdx ++) {
        ChunkMetaData metaData = chunkMetaDataList.get(chunkIdx);
        if (metaData.getStartTime() == startTime) {
          Chunk chunk = chunkLoader.getChunk(metaData);
          writeChunkWithoutUnseq(chunk, chunkWriter, measurementSchema);
          chunkWriter.writeToFileWriter(fileWriter);
          maxVersion = metaData.getVersion() > maxVersion ? metaData.getVersion() : maxVersion;
          break;
        }
      }
    }
    return maxVersion;
  }

}
