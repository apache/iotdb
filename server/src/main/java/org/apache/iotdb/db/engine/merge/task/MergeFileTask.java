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

package org.apache.iotdb.db.engine.merge.task;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.ForceAppendTsFileWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeFileTask merges the merge temporary files with the seqFiles, either move the merged
 * chunks in the temp files into the seqFiles or move the unmerged chunks into the merge temp
 * files, depending on which one is the majority.
 */
class MergeFileTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeFileTask.class);

  private String taskName;
  private MergeContext context;
  private MergeLogger mergeLogger;
  private MergeResource resource;
  private List<TsFileResource> unmergedFiles;

  MergeFileTask(String taskName, MergeContext context, MergeLogger mergeLogger,
      MergeResource resource, List<TsFileResource> unmergedSeqFiles) {
    this.taskName = taskName;
    this.context = context;
    this.mergeLogger = mergeLogger;
    this.resource = resource;
    this.unmergedFiles = unmergedSeqFiles;
  }

  void mergeFiles() throws IOException {
    // decide whether to write the unmerged chunks to the merge files or to move the merged chunks
    // back to the origin seqFile's
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} files", taskName, unmergedFiles.size());
    }
    long startTime = System.currentTimeMillis();
    int cnt = 0;
    for (TsFileResource seqFile : unmergedFiles) {
      int mergedChunkNum = context.getMergedChunkCnt().getOrDefault(seqFile, 0);
      int unmergedChunkNum = context.getUnmergedChunkCnt().getOrDefault(seqFile, 0);
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
    int mergedChunkNum = context.getMergedChunkCnt().getOrDefault(seqFile, 0);
    if (mergedChunkNum == 0) {
      resource.removeFileAndWriter(seqFile);
      return;
    }

    seqFile.getWriteQueryLock().writeLock().lock();
    try {
      TsFileMetaDataCache.getInstance().remove(seqFile);
      DeviceMetaDataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile);

      resource.removeFileReader(seqFile);
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
      // filter the chunks that have been merged
      oldFileWriter.filterChunks(context.getUnmergedChunkStartTimes().get(seqFile));

      RestorableTsFileIOWriter newFileWriter = resource.getMergeFileWriter(seqFile);
      newFileWriter.close();
      try (TsFileSequenceReader newFileReader =
          new TsFileSequenceReader(newFileWriter.getFile().getPath())) {
        List<ChunkGroupMetaData> chunkGroupMetadataList = newFileWriter.getChunkGroupMetaDatas();
        if (logger.isDebugEnabled()) {
          logger.debug("{} find {} merged chunk groups", taskName, chunkGroupMetadataList.size());
        }
        for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetadataList) {
          writeMergedChunkGroup(chunkGroupMetaData, newFileReader, oldFileWriter);
        }
      }
      oldFileWriter.endFile(new Schema(newFileWriter.getKnownSchema()));

      updateHistoricalVersions(seqFile);
      seqFile.serialize();
      mergeLogger.logFileMergeEnd();
      logger.debug("{} moved merged chunks of {} to the old file", taskName, seqFile);

      newFileWriter.getFile().delete();

      File nextMergeVersionFile = getNextMergeVersionFile(seqFile.getFile());
      FileUtils.moveFile(seqFile.getFile(), nextMergeVersionFile);
      FileUtils
          .moveFile(new File(seqFile.getFile().getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
              new File(nextMergeVersionFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      seqFile.setFile(nextMergeVersionFile);
    } finally {
      seqFile.getWriteQueryLock().writeLock().unlock();
    }
  }

  private void updateHistoricalVersions(TsFileResource seqFile) {
    // as the new file contains data of other files, track their versions in the new file
    // so that we will be able to compare data across different IoTDBs that share the same file
    // generation policy
    // however, since the data of unseq files are mixed together, we won't be able to know
    // which files are exactly contained in the new file, so we have to record all unseq files
    // in the new file
    Set<Long> newHistoricalVersions = new HashSet<>(seqFile.getHistoricalVersions());
    for (TsFileResource unseqFiles : resource.getUnseqFiles()) {
      newHistoricalVersions.addAll(unseqFiles.getHistoricalVersions());
    }
    seqFile.setHistoricalVersions(newHistoricalVersions);
  }

  private void writeMergedChunkGroup(ChunkGroupMetaData chunkGroupMetaData,
      TsFileSequenceReader reader, TsFileIOWriter fileWriter)
      throws IOException {
    fileWriter.startChunkGroup(chunkGroupMetaData.getDeviceID());
    long version = chunkGroupMetaData.getVersion();
    for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      fileWriter.writeChunk(chunk, chunkMetaData);
      context.incTotalPointWritten(chunkMetaData.getNumOfPoints());
    }
    fileWriter.endChunkGroup(version + 1);
  }

  private void moveUnmergedToNew(TsFileResource seqFile) throws IOException {
    Map<Path, List<Long>> fileUnmergedChunkStartTimes =
        context.getUnmergedChunkStartTimes().get(seqFile);
    RestorableTsFileIOWriter fileWriter = resource.getMergeFileWriter(seqFile);

    mergeLogger.logFileMergeStart(fileWriter.getFile(), fileWriter.getFile().length());
    logger.debug("{} moving unmerged chunks of {} to the new file", taskName, seqFile);

    int unmergedChunkNum = context.getUnmergedChunkCnt().getOrDefault(seqFile, 0);

    if (unmergedChunkNum > 0) {
      for (Entry<Path, List<Long>> entry : fileUnmergedChunkStartTimes.entrySet()) {
        Path path = entry.getKey();
        List<Long> chunkStartTimes = entry.getValue();
        if (chunkStartTimes.isEmpty()) {
          continue;
        }

        List<ChunkMetaData> chunkMetaDataList = resource.queryChunkMetadata(path, seqFile);

        if (logger.isDebugEnabled()) {
          logger.debug("{} find {} unmerged chunks", taskName, chunkMetaDataList.size());
        }

        fileWriter.startChunkGroup(path.getDevice());
        long maxVersion = writeUnmergedChunks(chunkStartTimes, chunkMetaDataList,
            resource.getFileReader(seqFile), fileWriter);
        fileWriter.endChunkGroup(maxVersion + 1);
      }
    }

    fileWriter.endFile(new Schema(fileWriter.getKnownSchema()));

    updateHistoricalVersions(seqFile);
    seqFile.serialize();
    mergeLogger.logFileMergeEnd();
    logger.debug("{} moved unmerged chunks of {} to the new file", taskName, seqFile);

    seqFile.getWriteQueryLock().writeLock().lock();
    try {
      resource.removeFileReader(seqFile);
      TsFileMetaDataCache.getInstance().remove(seqFile);
      DeviceMetaDataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile);
      seqFile.getFile().delete();

      File nextMergeVersionFile = getNextMergeVersionFile(seqFile.getFile());
      FileUtils.moveFile(fileWriter.getFile(), nextMergeVersionFile);
      FileUtils
          .moveFile(new File(seqFile.getFile().getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
              new File(nextMergeVersionFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      seqFile.setFile(nextMergeVersionFile);
    } finally {
      seqFile.getWriteQueryLock().writeLock().unlock();
    }
  }

  private File getNextMergeVersionFile(File seqFile) {
    String[] splits = seqFile.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.TSFILE_NAME_SEPARATOR);
    int mergeVersion = Integer.parseInt(splits[2]) + 1;
    return new File(seqFile.getParentFile(),
        splits[0] + IoTDBConstant.TSFILE_NAME_SEPARATOR + splits[1]
            + IoTDBConstant.TSFILE_NAME_SEPARATOR + mergeVersion + TSFILE_SUFFIX);
  }

  private long writeUnmergedChunks(List<Long> chunkStartTimes,
      List<ChunkMetaData> chunkMetaDataList, TsFileSequenceReader reader,
      RestorableTsFileIOWriter fileWriter) throws IOException {
    long maxVersion = 0;
    int chunkIdx = 0;
    for (Long startTime : chunkStartTimes) {
      for (; chunkIdx < chunkMetaDataList.size(); chunkIdx ++) {
        ChunkMetaData metaData = chunkMetaDataList.get(chunkIdx);
        if (metaData.getStartTime() == startTime) {
          Chunk chunk = reader.readMemChunk(metaData);
          fileWriter.writeChunk(chunk, metaData);
          maxVersion = metaData.getVersion() > maxVersion ? metaData.getVersion() : maxVersion;
          context.incTotalPointWritten(metaData.getNumOfPoints());
          break;
        }
      }
    }
    return maxVersion;
  }

}
