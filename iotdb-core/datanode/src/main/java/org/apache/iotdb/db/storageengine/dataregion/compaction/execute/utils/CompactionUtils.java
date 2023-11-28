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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This tool can be used to perform inner space or cross space compaction of aligned and non aligned
 * timeseries.
 */
public class CompactionUtils {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final String SYSTEM = "system";

  private CompactionUtils() {}

  /**
   * Update the targetResource. Move tmp target file to target file and serialize
   * xxx.tsfile.resource.
   *
   * @throws IOException if io errors occurred
   */
  public static void moveTargetFile(
      List<TsFileResource> targetResources, boolean isInnerSpace, String fullStorageGroupName)
      throws IOException {
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
   *
   * @throws IOException if io errors occurred
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
      if (!modifications.isEmpty()) {
        FileMetrics.getInstance().increaseModFileNum(1);
        FileMetrics.getInstance().increaseModFileSize(targetResource.getModFile().getSize());
      }
      modifications.removeAll(seqModifications);
    }
  }

  /**
   * Collect all the compaction modification files of source files, and combines them as the
   * modification file of target file.
   *
   * @throws IOException if io errors occurred
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
    if (!modifications.isEmpty()) {
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

  /**
   * Delete all modification files for source files.
   *
   * @throws IOException if io errors occurred
   */
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
      TsFileResource resource, TsFileIOWriter tsFileIoWriter, String deviceId) {
    List<ChunkMetadata> chunkMetadatasOfCurrentDevice =
        tsFileIoWriter.getChunkMetadataListOfCurrentDeviceInMemory();
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

  public static void deleteSourceTsFileAndUpdateFileMetrics(
      List<TsFileResource> sourceSeqResourceList, List<TsFileResource> sourceUnseqResourceList) {
    deleteSourceTsFileAndUpdateFileMetrics(sourceSeqResourceList, true);
    deleteSourceTsFileAndUpdateFileMetrics(sourceUnseqResourceList, false);
  }

  public static void deleteSourceTsFileAndUpdateFileMetrics(
      List<TsFileResource> resources, boolean seq) {
    List<TsFileResource> removeResources = new ArrayList<>();
    for (TsFileResource resource : resources) {
      if (!resource.remove()) {
        logger.warn(
            "[Compaction] delete file failed, file path is {}",
            resource.getTsFile().getAbsolutePath());
      } else {
        logger.info("[Compaction] delete file: {}", resource.getTsFile().getAbsolutePath());
        removeResources.add(resource);
      }
    }
    FileMetrics.getInstance().deleteTsFile(seq, resources);
  }

  public static boolean isDiskHasSpace() {
    return isDiskHasSpace(0d);
  }

  public static boolean isDiskHasSpace(double redundancy) {
    double availableDisk =
        MetricService.getInstance()
            .getAutoGauge(
                SystemMetric.SYS_DISK_AVAILABLE_SPACE.toString(),
                MetricLevel.CORE,
                Tag.NAME.toString(),
                SYSTEM)
            .getValue();
    double totalDisk =
        MetricService.getInstance()
            .getAutoGauge(
                SystemMetric.SYS_DISK_TOTAL_SPACE.toString(),
                MetricLevel.CORE,
                Tag.NAME.toString(),
                SYSTEM)
            .getValue();

    if (availableDisk != 0 && totalDisk != 0) {
      return availableDisk / totalDisk
          > CommonDescriptor.getInstance().getConfig().getDiskSpaceWarningThreshold() + redundancy;
    }
    return true;
  }
}
