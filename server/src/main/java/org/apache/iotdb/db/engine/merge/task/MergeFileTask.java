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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.write.writer.ForceAppendTsFileWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeFileTask merges the merge temporary files with the seqFiles, either move the merged chunks
 * in the temp files into the seqFiles or move the unmerged chunks into the merge temp files,
 * depending on which one is the majority.
 */
class MergeFileTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeFileTask.class);

  private String taskName;
  private MergeContext context;
  private MergeLogger mergeLogger;
  private MergeResource resource;
  private List<TsFileResource> unmergedFiles;

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private int currentMergeIndex;
  private String currMergeFile;

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
    for (int i = 0; i < unmergedFiles.size(); i++) {
      TsFileResource seqFile = unmergedFiles.get(i);
      currentMergeIndex = i;
      currMergeFile = seqFile.getTsFilePath();

      int mergedChunkNum = context.getMergedChunkCnt().getOrDefault(seqFile, 0);
      int unmergedChunkNum = context.getUnmergedChunkCnt().getOrDefault(seqFile, 0);
      if (mergedChunkNum >= unmergedChunkNum) {
        // move the unmerged data to the new file
        if (logger.isInfoEnabled()) {
          logger.info("{} moving unmerged data of {} to the merged file, {} merged chunks, {} "
                  + "unmerged chunks", taskName, seqFile.getTsFile().getName(), mergedChunkNum,
              unmergedChunkNum);
        }
        moveUnmergedToNew(seqFile);
      } else {
        // move the merged data to the old file
        if (logger.isInfoEnabled()) {
          logger.info("{} moving merged data of {} to the old file {} merged chunks, {} "
                  + "unmerged chunks", taskName, seqFile.getTsFile().getName(), mergedChunkNum,
              unmergedChunkNum);
        }
        moveMergedToOld(seqFile);
      }

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }

      logProgress();
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} has merged all files after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }
    mergeLogger.logMergeEnd();
  }

  private void logProgress() {
    if (logger.isInfoEnabled()) {
      logger.debug("{} has merged {}, processed {}/{} files", taskName, currMergeFile,
          currentMergeIndex + 1, unmergedFiles.size());
    }
  }

  public String getProgress() {
    return String.format("Merging %s, processed %d/%d files", currMergeFile,
        currentMergeIndex + 1, unmergedFiles.size());
  }

  private void moveMergedToOld(TsFileResource seqFile) throws IOException {
    int mergedChunkNum = context.getMergedChunkCnt().getOrDefault(seqFile, 0);
    if (mergedChunkNum == 0) {
      resource.removeFileAndWriter(seqFile);
      return;
    }

    seqFile.writeLock();
    try {
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());

      resource.removeFileReader(seqFile);
      TsFileIOWriter oldFileWriter = getOldFileWriter(seqFile);

      // filter the chunks that have been merged
      oldFileWriter.filterChunks(new HashMap<>(context.getUnmergedChunkStartTimes().get(seqFile))
      );

      RestorableTsFileIOWriter newFileWriter = resource.getMergeFileWriter(seqFile);
      newFileWriter.close();
      try (TsFileSequenceReader newFileReader =
          new TsFileSequenceReader(newFileWriter.getFile().getPath())) {
        Map<String, List<ChunkMetadata>> chunkMetadataListInChunkGroups =
            newFileWriter.getDeviceChunkMetadataMap();
        if (logger.isDebugEnabled()) {
          logger.debug("{} find {} merged chunk groups", taskName,
              chunkMetadataListInChunkGroups.size());
        }
        for (Map.Entry<String, List<ChunkMetadata>> entry : chunkMetadataListInChunkGroups
            .entrySet()) {
          String deviceId = entry.getKey();
          List<ChunkMetadata> chunkMetadataList = entry.getValue();
          writeMergedChunkGroup(chunkMetadataList, deviceId, newFileReader, oldFileWriter);

          if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            oldFileWriter.close();
            restoreOldFile(seqFile);
            return;
          }
        }
      }
      oldFileWriter.endFile();

      updateHistoricalVersions(seqFile);
      seqFile.serialize();
      mergeLogger.logFileMergeEnd();
      logger.debug("{} moved merged chunks of {} to the old file", taskName, seqFile);
    } catch (Exception e) {
      restoreOldFile(seqFile);
      throw e;
    } finally {
      seqFile.writeUnlock();
    }
  }

  /**
   * Restore an old seq file which is being written new chunks when exceptions occur or the task is
   * aborted.
   */
  private void restoreOldFile(TsFileResource seqFile) throws IOException {
    RestorableTsFileIOWriter oldFileRecoverWriter = new RestorableTsFileIOWriter(
        seqFile.getTsFile());
    if (oldFileRecoverWriter.hasCrashed() && oldFileRecoverWriter.canWrite()) {
      oldFileRecoverWriter.endFile();
    } else {
      oldFileRecoverWriter.close();
    }
  }

  /**
   * Open an appending writer for an old seq file so we can add new chunks to it.
   */
  private TsFileIOWriter getOldFileWriter(TsFileResource seqFile) throws IOException {
    TsFileIOWriter oldFileWriter;
    try {
      oldFileWriter = new ForceAppendTsFileWriter(seqFile.getTsFile());
      mergeLogger.logFileMergeStart(seqFile.getTsFile(),
          ((ForceAppendTsFileWriter) oldFileWriter).getTruncatePosition());
      logger.debug("{} moving merged chunks of {} to the old file", taskName, seqFile);
      ((ForceAppendTsFileWriter) oldFileWriter).doTruncate();
    } catch (TsFileNotCompleteException e) {
      // this file may already be truncated if this merge is a system reboot merge
      oldFileWriter = new RestorableTsFileIOWriter(seqFile.getTsFile());
    }
    return oldFileWriter;
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

  private void writeMergedChunkGroup(List<ChunkMetadata> chunkMetadataList, String device,
      TsFileSequenceReader reader, TsFileIOWriter fileWriter)
      throws IOException {
    fileWriter.startChunkGroup(device);
    long maxVersion = 0;
    for (ChunkMetadata chunkMetaData : chunkMetadataList) {
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      fileWriter.writeChunk(chunk, chunkMetaData);
      maxVersion =
          chunkMetaData.getVersion() > maxVersion ? chunkMetaData.getVersion() : maxVersion;
      context.incTotalPointWritten(chunkMetaData.getNumOfPoints());
    }
    fileWriter.writeVersion(maxVersion);
    fileWriter.endChunkGroup();
  }

  private void moveUnmergedToNew(TsFileResource seqFile) throws IOException {
    Map<PartialPath, List<Long>> fileUnmergedChunkStartTimes =
        context.getUnmergedChunkStartTimes().get(seqFile);
    RestorableTsFileIOWriter fileWriter = resource.getMergeFileWriter(seqFile);

    mergeLogger.logFileMergeStart(fileWriter.getFile(), fileWriter.getFile().length());
    logger.debug("{} moving unmerged chunks of {} to the new file", taskName, seqFile);

    int unmergedChunkNum = context.getUnmergedChunkCnt().getOrDefault(seqFile, 0);

    if (unmergedChunkNum > 0) {
      for (Entry<PartialPath, List<Long>> entry : fileUnmergedChunkStartTimes.entrySet()) {
        PartialPath path = entry.getKey();
        List<Long> chunkStartTimes = entry.getValue();
        if (chunkStartTimes.isEmpty()) {
          continue;
        }

        List<ChunkMetadata> chunkMetadataList = resource.queryChunkMetadata(path, seqFile);

        if (logger.isDebugEnabled()) {
          logger.debug("{} find {} unmerged chunks", taskName, chunkMetadataList.size());
        }

        fileWriter.startChunkGroup(path.getDevice());
        long maxVersion = writeUnmergedChunks(chunkStartTimes, chunkMetadataList,
            resource.getFileReader(seqFile), fileWriter);

        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        fileWriter.writeVersion(maxVersion + 1);
        fileWriter.endChunkGroup();
      }
    }

    fileWriter.endFile();

    updateHistoricalVersions(seqFile);
    seqFile.serialize();
    mergeLogger.logFileMergeEnd();
    logger.debug("{} moved unmerged chunks of {} to the new file", taskName, seqFile);

    seqFile.writeLock();
    try {
      resource.removeFileReader(seqFile);
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());

      File newMergeFile = seqFile.getTsFile();
      newMergeFile.delete();
      fsFactory.moveFile(fileWriter.getFile(), newMergeFile);
      seqFile.setFile(newMergeFile);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      // clean cache
      if (IoTDBDescriptor.getInstance().getConfig().isMetaDataCacheEnable()) {
        ChunkCache.getInstance().clear();
        ChunkMetadataCache.getInstance().clear();
        TimeSeriesMetadataCache.getInstance().clear();
      }
      seqFile.writeUnlock();
    }
  }

  private long writeUnmergedChunks(List<Long> chunkStartTimes,
      List<ChunkMetadata> chunkMetadataList, TsFileSequenceReader reader,
      RestorableTsFileIOWriter fileWriter) throws IOException {
    long maxVersion = 0;
    int chunkIdx = 0;
    for (Long startTime : chunkStartTimes) {
      for (; chunkIdx < chunkMetadataList.size(); chunkIdx++) {
        ChunkMetadata metaData = chunkMetadataList.get(chunkIdx);
        if (metaData.getStartTime() == startTime) {
          Chunk chunk = reader.readMemChunk(metaData);
          fileWriter.writeChunk(chunk, metaData);
          maxVersion = metaData.getVersion() > maxVersion ? metaData.getVersion() : maxVersion;
          context.incTotalPointWritten(metaData.getNumOfPoints());
          break;
        }
      }

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return maxVersion;
      }
    }
    return maxVersion;
  }

  private File getNextMergeVersionFile(File seqFile) {
    String[] splits = seqFile.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.FILE_NAME_SEPARATOR);
    int mergeVersion = Integer.parseInt(splits[2]) + 1;
    return fsFactory.getFile(seqFile.getParentFile(),
        splits[0] + IoTDBConstant.FILE_NAME_SEPARATOR + splits[1]
            + IoTDBConstant.FILE_NAME_SEPARATOR + mergeVersion + TSFILE_SUFFIX);
  }
}
