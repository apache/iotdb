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
package org.apache.iotdb.db.engine.compaction.execute.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This tool can be used to perform inner space or cross space compaction of aligned and non aligned
 * timeseries.
 */
public class CompactionUtils {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  /**
   * Update the targetResource. Move tmp target file to target file and serialize
   * xxx.tsfile.resource.
   */
  public static void moveTargetFile(
      List<TsFileResource> targetResources, boolean isInnerSpace, String fullStorageGroupName)
      throws IOException, WriteProcessException {
    String fileSuffix;
    if (isInnerSpace) {
      fileSuffix = IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX;
    } else {
      fileSuffix = IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX;
    }
    for (TsFileResource targetResource : targetResources) {
      if (targetResource != null) {
        moveOneTargetFile(targetResource, fileSuffix, fullStorageGroupName);
      }
    }
  }

  private static void moveOneTargetFile(
      TsFileResource targetResource, String tmpFileSuffix, String fullStorageGroupName)
      throws IOException {
    // move to target file and delete old tmp target file
    if (!targetResource.getTsFile().exists()) {
      logger.info(
          "{} [Compaction] Tmp target tsfile {} may be deleted after compaction.",
          fullStorageGroupName,
          targetResource.getTsFilePath());
      return;
    }
    File newFile =
        new File(
            targetResource.getTsFilePath().replace(tmpFileSuffix, TsFileConstant.TSFILE_SUFFIX));
    if (!newFile.exists()) {
      FSFactoryProducer.getFSFactory().moveFile(targetResource.getTsFile(), newFile);
    }

    // serialize xxx.tsfile.resource
    targetResource.setFile(newFile);
    targetResource.serialize();
    targetResource.closeWithoutSettingStatus();
  }

  /**
   * Collect all the compaction modification files of source files, and combines them as the
   * modification file of target file.
   */
  public static void combineModsInCrossCompaction(
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources,
      List<TsFileResource> targetResources)
      throws IOException {
    Set<Modification> modifications = new HashSet<>();
    // get compaction mods from all source unseq files
    for (TsFileResource unseqFile : unseqResources) {
      modifications.addAll(ModificationFile.getCompactionMods(unseqFile).getModifications());
    }

    // write target mods file
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource targetResource = targetResources.get(i);
      if (targetResource == null) {
        continue;
      }
      Set<Modification> seqModifications =
          new HashSet<>(ModificationFile.getCompactionMods(seqResources.get(i)).getModifications());
      modifications.addAll(seqModifications);
      updateOneTargetMods(targetResource, modifications);
      if (modifications.size() > 0) {
        FileMetrics.getInstance().increaseModFileNum(1);
        FileMetrics.getInstance().increaseModFileSize(targetResource.getModFile().getSize());
      }
      modifications.removeAll(seqModifications);
    }
  }

  /**
   * Collect all the compaction modification files of source files, and combines them as the
   * modification file of target file.
   */
  public static void combineModsInInnerCompaction(
      Collection<TsFileResource> sourceFiles, TsFileResource targetTsFile) throws IOException {
    Set<Modification> modifications = new HashSet<>();
    for (TsFileResource mergeTsFile : sourceFiles) {
      try (ModificationFile sourceCompactionModificationFile =
          ModificationFile.getCompactionMods(mergeTsFile)) {
        modifications.addAll(sourceCompactionModificationFile.getModifications());
      }
    }
    updateOneTargetMods(targetTsFile, modifications);
    if (modifications.size() > 0) {
      FileMetrics.getInstance().increaseModFileNum(1);
      FileMetrics.getInstance().increaseModFileSize(targetTsFile.getModFile().getSize());
    }
  }

  private static void updateOneTargetMods(
      TsFileResource targetFile, Set<Modification> modifications) throws IOException {
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile = ModificationFile.getNormalMods(targetFile)) {
        for (Modification modification : modifications) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          modificationFile.write(modification);
        }
      }
    }
  }

  public static void deleteCompactionModsFile(
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList)
      throws IOException {
    for (TsFileResource seqFile : selectedSeqTsFileResourceList) {
      ModificationFile modificationFile = seqFile.getCompactionModFile();
      if (modificationFile.exists()) {
        modificationFile.remove();
      }
    }
    for (TsFileResource unseqFile : selectedUnSeqTsFileResourceList) {
      ModificationFile modificationFile = unseqFile.getCompactionModFile();
      if (modificationFile.exists()) {
        modificationFile.remove();
      }
    }
  }

  public static boolean deleteTsFilesInDisk(
      Collection<TsFileResource> mergeTsFiles, String storageGroupName) {
    logger.info("{} [Compaction] Compaction starts to delete real file ", storageGroupName);
    boolean result = true;
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      if (!mergeTsFile.remove()) {
        result = false;
      }
      logger.info(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
    return result;
  }

  /** Delete all modification files for source files */
  public static void deleteModificationForSourceFile(
      Collection<TsFileResource> sourceFiles, String storageGroupName) throws IOException {
    logger.info("{} [Compaction] Start to delete modifications of source files", storageGroupName);
    for (TsFileResource tsFileResource : sourceFiles) {
      ModificationFile compactionModificationFile =
          ModificationFile.getCompactionMods(tsFileResource);
      if (compactionModificationFile.exists()) {
        compactionModificationFile.remove();
      }

      ModificationFile normalModification = ModificationFile.getNormalMods(tsFileResource);
      if (normalModification.exists()) {
        FileMetrics.getInstance().decreaseModFileNum(1);
        FileMetrics.getInstance().decreaseModFileSize(tsFileResource.getModFile().getSize());
        normalModification.remove();
      }
    }
  }

  public static void updateResource(
      TsFileResource resource, TsFileIOWriter tsFileIOWriter, String deviceId) {
    List<ChunkMetadata> chunkMetadatasOfCurrentDevice =
        tsFileIOWriter.getChunkMetadataListOfCurrentDeviceInMemory();
    if (chunkMetadatasOfCurrentDevice != null) {
      // this target file contains current device
      for (ChunkMetadata chunkMetadata : chunkMetadatasOfCurrentDevice) {
        if (chunkMetadata.getMask() == TsFileConstant.VALUE_COLUMN_MASK) {
          // value chunk metadata can be skipped
          continue;
        }
        resource.updateStartTime(deviceId, chunkMetadata.getStatistics().getStartTime());
        resource.updateEndTime(deviceId, chunkMetadata.getStatistics().getEndTime());
      }
    }
  }

  public static void updateProgressIndex(
      List<TsFileResource> targetResources,
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources) {
    for (TsFileResource targetResource : targetResources) {
      for (TsFileResource unseqResource : unseqResources) {
        targetResource.updateProgressIndex(unseqResource.getMaxProgressIndexAfterClose());
      }
      for (TsFileResource seqResource : seqResources) {
        targetResource.updateProgressIndex(seqResource.getMaxProgressIndexAfterClose());
      }
    }
  }

  public static void updatePlanIndexes(
      List<TsFileResource> targetResources,
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources) {
    // as the new file contains data of other files, track their plan indexes in the new file
    // so that we will be able to compare data across different IoTDBs that share the same index
    // generation policy
    // however, since the data of unseq files are mixed together, we won't be able to know
    // which files are exactly contained in the new file, so we have to record all unseq files
    // in the new file
    for (TsFileResource targetResource : targetResources) {
      for (TsFileResource unseqResource : unseqResources) {
        targetResource.updatePlanIndexes(unseqResource);
      }
      for (TsFileResource seqResource : seqResources) {
        targetResource.updatePlanIndexes(seqResource);
      }
    }
  }

  public static boolean validateTsFileResources(
      TsFileManager manager, String storageGroupName, long timePartition) throws IOException {
    List<TsFileResource> resources =
        manager.getOrCreateSequenceListByTimePartition(timePartition).getArrayList();

    // deviceID -> <TsFileResource, last end time>
    Map<String, Pair<TsFileResource, Long>> lastEndTimeMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      DeviceTimeIndex timeIndex;
      if (resource.getTimeIndexType() != 1) {
        // if time index is not device time index, then deserialize it from resource file
        timeIndex = resource.buildDeviceTimeIndex();
      } else {
        timeIndex = (DeviceTimeIndex) resource.getTimeIndex();
      }
      Set<String> devices = timeIndex.getDevices();
      for (String device : devices) {
        long currentStartTime = timeIndex.getStartTime(device);
        long currentEndTime = timeIndex.getEndTime(device);
        Pair<TsFileResource, Long> lastDeviceInfo =
            lastEndTimeMap.computeIfAbsent(device, x -> new Pair<>(null, Long.MIN_VALUE));
        long lastEndTime = lastDeviceInfo.right;
        if (lastEndTime >= currentStartTime) {
          logger.error(
              "{} Device {} is overlapped between {} and {}, end time in {} is {}, start time in {} is {}",
              storageGroupName,
              device,
              lastDeviceInfo.left,
              resource,
              lastDeviceInfo.left,
              lastEndTime,
              resource,
              currentStartTime);
          return false;
        }
        lastDeviceInfo.left = resource;
        lastDeviceInfo.right = currentEndTime;
        lastEndTimeMap.put(device, lastDeviceInfo);
      }
    }
    return true;
  }

  /**
   * Validate TsFiles by reading them sequentially. This method should be fast because the read is
   * sequential.
   *
   * @param tsFileResourceList the tsfiles to be checked
   * @return true if all tsfiles are valid, false if any of the tsfiles is invalid
   */
  public static boolean validateTsFiles(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      if (!validateSingleTsFiles(tsFileResource)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("java:S6541") // do not warn about brain method
  public static boolean validateSingleTsFiles(TsFileResource resource) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(resource.getTsFilePath())) {
      reader.readHeadMagic();
      reader.readTailMagic();

      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      List<long[]> timeBatch = new ArrayList<>();
      int pageIndex = 0;
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              // empty value chunk
              break;
            }
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            pageIndex = 0;
            if (header.getDataType() == TSDataType.VECTOR) {
              timeBatch.clear();
            }
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              if ((header.getChunkType() & (byte) TsFileConstant.TIME_COLUMN_MASK)
                  == (byte) TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
                TimePageReader timePageReader =
                    new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                timeBatch.add(timePageReader.getNextTimeBatch());
              } else if ((header.getChunkType() & (byte) TsFileConstant.VALUE_COLUMN_MASK)
                  == (byte) TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
                ValuePageReader valuePageReader =
                    new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                TsPrimitiveType[] valueBatch =
                    valuePageReader.nextValueBatch(timeBatch.get(pageIndex));
              } else { // NonAligned Chunk
                PageReader pageReader =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                BatchData batchData = pageReader.getAllSatisfiedPageData();
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      for (String device : reader.getAllDevices()) {
        Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
      }
    } catch (Exception e) {
      logger.error("Meets error when validating TsFile {}, ", resource.getTsFilePath(), e);
      return false;
    }
    return true;
  }
}
