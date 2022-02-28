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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.ForceAppendTsFileWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.modifyTsFileNameUnseqMergCnt;

/**
 * MergeFileTask merges the merge temporary files with the seqFiles, either move the merged chunks
 * in the temp files into the seqFiles or move the unmerged chunks into the merge temp files,
 * depending on which one is the majority.
 */
public class MergeFileTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeFileTask.class);

  private String taskName;
  private MergeContext context;
  private MergeLogger mergeLogger;
  private MergeResource resource;
  private List<TsFileResource> unmergedFiles;

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private int currentMergeIndex;
  private String currMergeFile;

  MergeFileTask(
      String taskName,
      MergeContext context,
      MergeLogger mergeLogger,
      MergeResource resource,
      List<TsFileResource> unmergedSeqFiles) {
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

    checkResourceValid();
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
          logger.info(
              "{} moving unmerged data of {} to the merged file, {} merged chunks, {} "
                  + "unmerged chunks",
              taskName,
              seqFile.getTsFile().getName(),
              mergedChunkNum,
              unmergedChunkNum);
        }
        moveUnmergedToNew(seqFile);
      } else {
        // move the merged data to the old file
        if (logger.isInfoEnabled()) {
          logger.info(
              "{} moving merged data of {} to the old file {} merged chunks, {} "
                  + "unmerged chunks",
              taskName,
              seqFile.getTsFile().getName(),
              mergedChunkNum,
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
      logger.info(
          "{} has merged all files after {}ms", taskName, System.currentTimeMillis() - startTime);
    }
  }

  void checkResourceValid() throws IOException {
    // device -> [startTime, endTime]
    Map<String, Long> devicesTimeRangeMap = new HashMap<>();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      TsFileIOWriter fileWriter = resource.getMergeFileWriter(seqFile, false);
      Map<String, List<ChunkMetadata>> deviceChunkMetadataListMap =
          fileWriter.getDeviceChunkMetadataMap();
      for (Entry<String, List<ChunkMetadata>> entry : deviceChunkMetadataListMap.entrySet()) {
        String device = entry.getKey();
        if (!device.equals("root.Baoshan.670107E20.00")) {
          continue;
        }
        List<ChunkMetadata> metadata = entry.getValue();

        long maxEndTimeForThisFile = Long.MIN_VALUE;
        long minStartTimeForThisFile = Long.MAX_VALUE;
        for (ChunkMetadata chunkMetadata : metadata) {
          if (chunkMetadata.getEndTime() > maxEndTimeForThisFile) {
            maxEndTimeForThisFile = chunkMetadata.getEndTime();
          }
          if (chunkMetadata.getStartTime() < minStartTimeForThisFile) {
            minStartTimeForThisFile = chunkMetadata.getStartTime();
          }
        }

        if (devicesTimeRangeMap.containsKey(device)
            && devicesTimeRangeMap.get(device) > minStartTimeForThisFile) {
          logger.error(
              "merge error occurs. seq file is {}, unseq file is {}, device is {}",
              resource.getSeqFiles(),
              resource.getUnseqFiles(),
              device);
          System.exit(-1);
        }
        devicesTimeRangeMap.put(device, maxEndTimeForThisFile);
      }
    }

    Map<String, Long> measurementLatestTimeMap = new HashMap<>();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      String currentDevice = "";
      String currentSeries = "";
      TsFileIOWriter fileWriter = resource.getMergeFileWriter(seqFile, false);
      try (TsFileSequenceReader reader =
          new TsFileSequenceReader(fileWriter.getFile().getAbsolutePath())) {
        reader.readHeadMagic();
        reader.readTailMagic();
        // Sequential reading of one ChunkGroup now follows this order:
        // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
        // Because we do not know how many chunks a ChunkGroup may have, we should read one byte
        // (the
        // marker) ahead and judge accordingly.
        reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
        byte marker;
        while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
          switch (marker) {
            case MetaMarker.CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
              ChunkHeader header = reader.readChunkHeader(marker);
              currentSeries = currentDevice + "." + header.getMeasurementID();
              Decoder defaultTimeDecoder =
                  Decoder.getDecoderByType(
                      TSEncoding.valueOf(
                          TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                      TSDataType.INT64);
              Decoder valueDecoder =
                  Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
              int dataSize = header.getDataSize();
              while (dataSize > 0) {
                valueDecoder.reset();
                PageHeader pageHeader =
                    reader.readPageHeader(
                        header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
                ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                PageReader reader1 =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                BatchData batchData = reader1.getAllSatisfiedPageData();
                while (batchData.hasCurrent()) {
                  if (measurementLatestTimeMap.containsKey(currentSeries)) {
                    if (measurementLatestTimeMap.get(currentSeries) >= batchData.currentTime()) {
                      logger.error(
                          "merge error while checking files. series is {}, file is {}",
                          currentSeries,
                          seqFile);
                      System.exit(-1);
                    }
                  }
                  measurementLatestTimeMap.put(currentSeries, batchData.currentTime());
                  batchData.next();
                }
                dataSize -= pageHeader.getSerializedPageSize();
              }
              break;
            case MetaMarker.CHUNK_GROUP_HEADER:
              ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
              currentDevice = chunkGroupHeader.getDeviceID();
              break;
            case MetaMarker.OPERATION_INDEX_RANGE:
              reader.readPlanIndex();
              break;
            default:
              MetaMarker.handleUnexpectedMarker(marker);
          }
        }
      }
    }
  }

  private void logProgress() {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} has merged {}, processed {}/{} files",
          taskName,
          currMergeFile,
          currentMergeIndex + 1,
          unmergedFiles.size());
    }
  }

  public String getProgress() {
    return String.format(
        "Merging %s, processed %d/%d files",
        currMergeFile, currentMergeIndex + 1, unmergedFiles.size());
  }

  private void moveMergedToOld(TsFileResource seqFile) throws IOException {
    int mergedChunkNum = context.getMergedChunkCnt().getOrDefault(seqFile, 0);
    if (mergedChunkNum == 0) {
      resource.removeFileAndWriter(seqFile);
      return;
    }

    seqFile.writeLock();
    try {
      if (Thread.currentThread().isInterrupted()) {
        return;
      }

      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());

      resource.removeFileReader(seqFile);
      TsFileIOWriter oldFileWriter = getOldFileWriter(seqFile);

      // filter the chunks that have been merged
      oldFileWriter.filterChunks(new HashMap<>(context.getUnmergedChunkStartTimes().get(seqFile)));

      RestorableTsFileIOWriter newFileWriter = resource.getMergeFileWriter(seqFile, false);
      newFileWriter.close();
      try (TsFileSequenceReader newFileReader =
          new TsFileSequenceReader(newFileWriter.getFile().getPath())) {
        Map<String, List<ChunkMetadata>> chunkMetadataListInChunkGroups =
            newFileWriter.getDeviceChunkMetadataMap();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{} find {} merged chunk groups", taskName, chunkMetadataListInChunkGroups.size());
        }
        for (Map.Entry<String, List<ChunkMetadata>> entry :
            chunkMetadataListInChunkGroups.entrySet()) {
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
      updateStartTimeAndEndTime(seqFile, oldFileWriter);
      oldFileWriter.endFile();
      updatePlanIndexes(seqFile);
      seqFile.serialize();
      logger.debug("{} moved merged chunks of {} to the old file", taskName, seqFile);

      if (!newFileWriter.getFile().delete()) {
        logger.warn("fail to delete {}", newFileWriter.getFile());
      }
      // change tsFile name
      File nextMergeVersionFile = modifyTsFileNameUnseqMergCnt(seqFile.getTsFile());
      fsFactory.moveFile(seqFile.getTsFile(), nextMergeVersionFile);
      fsFactory.moveFile(
          fsFactory.getFile(seqFile.getTsFile().getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
          fsFactory.getFile(
              nextMergeVersionFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      seqFile.setFile(nextMergeVersionFile);
    } catch (Exception e) {
      restoreOldFile(seqFile);
      throw e;
    } finally {
      seqFile.writeUnlock();
    }
  }

  private void updateStartTimeAndEndTime(TsFileResource seqFile, TsFileIOWriter fileWriter) {
    // TODO change to get one timeseries block each time
    Map<String, List<ChunkMetadata>> deviceChunkMetadataListMap =
        fileWriter.getDeviceChunkMetadataMap();
    for (Entry<String, List<ChunkMetadata>> deviceChunkMetadataListEntry :
        deviceChunkMetadataListMap.entrySet()) {
      String device = deviceChunkMetadataListEntry.getKey();
      for (ChunkMetadata chunkMetadata : deviceChunkMetadataListEntry.getValue()) {
        resource.updateStartTime(seqFile, device, chunkMetadata.getStartTime());
        resource.updateEndTime(seqFile, device, chunkMetadata.getEndTime());
      }
    }
    // update all device start time and end time of the resource
    Map<String, Pair<Long, Long>> deviceStartEndTimePairMap = resource.getStartEndTime(seqFile);
    for (Entry<String, Pair<Long, Long>> deviceStartEndTimePairEntry :
        deviceStartEndTimePairMap.entrySet()) {
      String device = deviceStartEndTimePairEntry.getKey();
      Pair<Long, Long> startEndTimePair = deviceStartEndTimePairEntry.getValue();
      seqFile.putStartTime(device, startEndTimePair.left);
      seqFile.putEndTime(device, startEndTimePair.right);
    }
  }

  /**
   * Restore an old seq file which is being written new chunks when exceptions occur or the task is
   * aborted.
   */
  private void restoreOldFile(TsFileResource seqFile) throws IOException {
    RestorableTsFileIOWriter oldFileRecoverWriter =
        new RestorableTsFileIOWriter(seqFile.getTsFile());
    if (oldFileRecoverWriter.hasCrashed() && oldFileRecoverWriter.canWrite()) {
      oldFileRecoverWriter.endFile();
    } else {
      oldFileRecoverWriter.close();
    }
  }

  /** Open an appending writer for an old seq file so we can add new chunks to it. */
  private TsFileIOWriter getOldFileWriter(TsFileResource seqFile) throws IOException {
    TsFileIOWriter oldFileWriter;
    try {
      oldFileWriter = new ForceAppendTsFileWriter(seqFile.getTsFile());
      logger.debug("{} moving merged chunks of {} to the old file", taskName, seqFile);
      ((ForceAppendTsFileWriter) oldFileWriter).doTruncate();
    } catch (TsFileNotCompleteException e) {
      // this file may already be truncated if this merge is a system reboot merge
      oldFileWriter = new RestorableTsFileIOWriter(seqFile.getTsFile());
    }
    return oldFileWriter;
  }

  private void updatePlanIndexes(TsFileResource seqFile) {
    // as the new file contains data of other files, track their plan indexes in the new file
    // so that we will be able to compare data across different IoTDBs that share the same index
    // generation policy
    // however, since the data of unseq files are mixed together, we won't be able to know
    // which files are exactly contained in the new file, so we have to record all unseq files
    // in the new file
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      seqFile.updatePlanIndexes(unseqFile);
    }
  }

  private void writeMergedChunkGroup(
      List<ChunkMetadata> chunkMetadataList,
      String device,
      TsFileSequenceReader reader,
      TsFileIOWriter fileWriter)
      throws IOException {
    fileWriter.startChunkGroup(device);
    for (ChunkMetadata chunkMetaData : chunkMetadataList) {
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      fileWriter.writeChunk(chunk, chunkMetaData);
      context.incTotalPointWritten(chunkMetaData.getNumOfPoints());
    }
    fileWriter.endChunkGroup();
  }

  private void moveUnmergedToNew(TsFileResource seqFile) throws IOException {
    Map<PartialPath, List<Long>> fileUnmergedChunkStartTimes =
        context.getUnmergedChunkStartTimes().get(seqFile);
    RestorableTsFileIOWriter fileWriter = resource.getMergeFileWriter(seqFile, false);

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
        writeUnmergedChunks(
            chunkStartTimes, chunkMetadataList, resource.getFileReader(seqFile), fileWriter);

        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        fileWriter.endChunkGroup();
      }
    }
    updateStartTimeAndEndTime(seqFile, fileWriter);
    resource.removeFileReader(seqFile);
    fileWriter.endFile();

    updatePlanIndexes(seqFile);

    seqFile.writeLock();
    try {
      if (Thread.currentThread().isInterrupted()) {
        return;
      }

      seqFile.serialize();
      logger.debug("{} moved unmerged chunks of {} to the new file", taskName, seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());

      // change tsFile name
      if (!seqFile.getTsFile().delete()) {
        logger.warn("fail to delete {}", seqFile.getTsFile());
      }
      fsFactory.deleteIfExists(
          new File(seqFile.getTsFile().getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      File nextMergeVersionFile = modifyTsFileNameUnseqMergCnt(seqFile.getTsFile());
      fsFactory.moveFile(fileWriter.getFile(), nextMergeVersionFile);
      seqFile.setFile(nextMergeVersionFile);
      seqFile.serialize();
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      // clean cache
      if (IoTDBDescriptor.getInstance().getConfig().isMetaDataCacheEnable()) {
        ChunkCache.getInstance().clear();
        TimeSeriesMetadataCache.getInstance().clear();
      }
      seqFile.writeUnlock();
    }
  }

  private long writeUnmergedChunks(
      List<Long> chunkStartTimes,
      List<ChunkMetadata> chunkMetadataList,
      TsFileSequenceReader reader,
      RestorableTsFileIOWriter fileWriter)
      throws IOException {
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
}
