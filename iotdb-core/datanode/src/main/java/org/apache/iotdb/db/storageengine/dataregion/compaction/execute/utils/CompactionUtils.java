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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.FullExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModFileManagement;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.SystemMetric;

import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
      List<TsFileResource> targetResources, CompactionTaskType type, String fullStorageGroupName)
      throws IOException {
    String fileSuffix = getTmpFileSuffix(type);
    for (TsFileResource targetResource : targetResources) {
      if (targetResource != null) {
        moveOneTargetFile(targetResource, fileSuffix, fullStorageGroupName);
      }
    }
  }

  public static String getTmpFileSuffix(CompactionTaskType type) {
    switch (type) {
      case INNER_UNSEQ:
      case INNER_SEQ:
      case REPAIR:
        return IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX;
      case CROSS:
        return IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX;
      case SETTLE:
        return IoTDBConstant.SETTLE_SUFFIX;
      default:
        logger.error("Current task type {} does not have tmp file suffix.", type);
        return "";
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
    if (TsFileResource.useSharedModFile) {
      // when using the shared mod file, modifications generated during compaction will be
      // directly written into shared mod file, so there is no need to concern the sources
      return;
    }

    Set<ModEntry> modifications = new HashSet<>();
    // get compaction mods from all source unseq files
    for (TsFileResource unseqFile : unseqResources) {
      modifications.addAll(ModificationFile.readAllCompactionModifications(unseqFile.getTsFile()));
    }

    // write target mods file
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource targetResource = targetResources.get(i);
      if (targetResource == null) {
        continue;
      }
      Set<ModEntry> seqModifications =
          new HashSet<>(
              ModificationFile.readAllCompactionModifications(seqResources.get(i).getTsFile()));
      modifications.addAll(seqModifications);
      updateOneTargetMods(targetResource, modifications);
      modifications.removeAll(seqModifications);
    }
  }

  @SafeVarargs
  public static void prepareCompactionModFiles(
      List<TsFileResource> targets, List<TsFileResource>... sourceLists) throws IOException {
    if (!TsFileResource.useSharedModFile) {
      Set<ModificationFile> compactionModFileSet =
          targets.stream()
              .map(
                  tsFileResource -> {
                    try {
                      return tsFileResource.getModFileForWrite();
                    } catch (IOException e) {
                      logger.error("Can not get mod file of {}", tsFileResource, e);
                      return null;
                    }
                  })
              .filter(Objects::nonNull)
              .collect(Collectors.toSet());
      for (List<TsFileResource> sourceList : sourceLists) {
        for (TsFileResource tsFileResource : sourceList) {
          tsFileResource.getModFileForWrite().setCascadeFile(compactionModFileSet);
        }
      }
      return;
    }
    TsFileResource firstSource = sourceLists[0].get(0);
    TsFileResource firstTarget = targets.get(0);
    ModFileManagement modFileManagement = firstSource.getModFileManagement();
    ModificationFile modificationFile = modFileManagement.allocateFor(firstTarget);
    for (TsFileResource tsFileResource : targets) {
      tsFileResource.setModFileManagement(modFileManagement);
      modFileManagement.addReference(tsFileResource, modificationFile);
      tsFileResource.setSharedModFile(modificationFile, false);
    }
    for (List<TsFileResource> sources : sourceLists) {
      for (TsFileResource tsFileResource : sources) {
        // lock so that the compaction mod file will not be omitted by deletion
        tsFileResource.writeLock();
        tsFileResource.setCompactionModFile(modificationFile);
        tsFileResource.writeUnlock();
      }
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
    if (TsFileResource.useSharedModFile) {
      return;
    }
    Set<ModEntry> modifications = new HashSet<>();
    for (TsFileResource mergeTsFile : sourceFiles) {
      try (ModificationFile sourceCompactionModificationFile = mergeTsFile.getCompactionModFile()) {
        modifications.addAll(sourceCompactionModificationFile.getAllMods());
      }
    }
    updateOneTargetMods(targetTsFile, modifications);
  }

  public static void combineModsInInnerCompaction(
      Collection<TsFileResource> sourceFiles, List<TsFileResource> targetTsFiles)
      throws IOException {
    if (TsFileResource.useSharedModFile) {
      return;
    }
    Set<ModEntry> modifications = new HashSet<>();
    for (TsFileResource mergeTsFile : sourceFiles) {
      try (ModificationFile sourceCompactionModificationFile = mergeTsFile.getCompactionModFile()) {
        modifications.addAll(sourceCompactionModificationFile.getAllMods());
      }
    }
    for (TsFileResource targetTsFile : targetTsFiles) {
      updateOneTargetMods(targetTsFile, modifications);
    }
  }

  public static void addFilesToFileMetrics(TsFileResource resource) {
    FileMetrics.getInstance()
        .addTsFile(
            resource.getDatabaseName(),
            resource.getDataRegionId(),
            resource.getTsFile().length(),
            resource.isSeq(),
            resource.getTsFile().getName());
  }

  private static void updateOneTargetMods(TsFileResource targetFile, Set<ModEntry> modifications)
      throws IOException {
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile = targetFile.getModFileForWrite()) {
        for (ModEntry modification : modifications) {
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
      seqFile.removeCompactionModFile();
    }
    for (TsFileResource unseqFile : selectedUnSeqTsFileResourceList) {
      unseqFile.removeCompactionModFile();
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
  @TestOnly
  public static void deleteModificationForSourceFile(
      Collection<TsFileResource> sourceFiles, String storageGroupName) throws IOException {
    logger.info("{} [Compaction] Start to delete modifications of source files", storageGroupName);
    for (TsFileResource tsFileResource : sourceFiles) {
      tsFileResource.removeModFile();
    }
  }

  public static void updateResource(
      TsFileResource resource, TsFileIOWriter tsFileIoWriter, IDeviceID deviceId) {
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

  public static void updateProgressIndexAndMark(
      List<TsFileResource> targetResources,
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources) {
    for (TsFileResource targetResource : targetResources) {
      // Initial value
      targetResource.setGeneratedByPipe(true);
      targetResource.setGeneratedByPipeConsensus(true);
      for (TsFileResource unseqResource : unseqResources) {
        targetResource.updateProgressIndex(unseqResource.getMaxProgressIndex());
        targetResource.setGeneratedByPipe(
            unseqResource.isGeneratedByPipe() && targetResource.isGeneratedByPipe());
        targetResource.setGeneratedByPipeConsensus(
            unseqResource.isGeneratedByPipeConsensus()
                && targetResource.isGeneratedByPipeConsensus());
      }
      for (TsFileResource seqResource : seqResources) {
        targetResource.updateProgressIndex(seqResource.getMaxProgressIndex());
        targetResource.setGeneratedByPipe(
            seqResource.isGeneratedByPipe() && targetResource.isGeneratedByPipe());
        targetResource.setGeneratedByPipeConsensus(
            seqResource.isGeneratedByPipeConsensus()
                && targetResource.isGeneratedByPipeConsensus());
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
    for (TsFileResource resource : resources) {
      deleteTsFileResourceWithoutLock(resource);
    }
    FileMetrics.getInstance().deleteTsFile(seq, resources);
  }

  public static void deleteTsFileResourceWithoutLock(TsFileResource resource) {
    if (!resource.remove()) {
      logger.warn(
          "[Compaction] delete file failed, file path is {}",
          resource.getTsFile().getAbsolutePath());
    } else {
      logger.info("[Compaction] delete file: {}", resource.getTsFile().getAbsolutePath());
    }
  }

  public static PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>
      buildModEntryPatternTreeMap(TsFileResource resource) {
    PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> patternTreeMap =
        PatternTreeMapFactory.getModsPatternTreeMap();
    TsFileResource.ModIterator modEntryIterator = resource.getModEntryIterator();
    while (modEntryIterator.hasNext()) {
      ModEntry modification = modEntryIterator.next();
      patternTreeMap.append(modification.keyOfPatternTree(), modification);
    }
    return patternTreeMap;
  }

  public static List<ModEntry> getMatchedModifications(
      PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> patternTreeMap,
      IDeviceID deviceID,
      String measurement,
      ModEntry ttlDeletion)
      throws IllegalPathException {
    if ((patternTreeMap == null) || patternTreeMap.isEmpty()) {
      return ttlDeletion == null ? Collections.emptyList() : Collections.singletonList(ttlDeletion);
    }
    PartialPath path = CompactionPathUtils.getPath(deviceID, measurement);
    List<ModEntry> modEntries = patternTreeMap.getOverlapped(path);
    if (ttlDeletion != null) {
      if (!(modEntries instanceof ArrayList)) {
        List<ModEntry> newModEntries = new ArrayList<>(modEntries.size() + 1);
        newModEntries.addAll(modEntries);
        modEntries = newModEntries;
      }
      modEntries.add(ttlDeletion);
    }
    if (path.getIDeviceID().isTableModel()) {
      modEntries =
          modEntries.stream()
              .filter(e -> e.affects(path.getIDeviceID()) && e.affects(path.getMeasurement()))
              .collect(Collectors.toList());
    }
    return ModificationUtils.sortAndMerge(modEntries);
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

  public static ArrayDeviceTimeIndex buildDeviceTimeIndex(TsFileResource resource)
      throws IOException {
    return buildDeviceTimeIndex(resource, IDeviceID.Deserializer.DEFAULT_DESERIALIZER);
  }

  public static ArrayDeviceTimeIndex buildDeviceTimeIndex(
      TsFileResource resource, IDeviceID.Deserializer deserializer) throws IOException {
    long resourceFileSize =
        new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX).length();
    CompactionTaskManager.getInstance().getCompactionReadOperationRateLimiter().acquire(1);
    CompactionMetrics.getInstance().recordDeserializeResourceInfo(resourceFileSize);
    while (resourceFileSize > 0) {
      int readSize = (int) Math.min(resourceFileSize, Integer.MAX_VALUE);
      CompactionTaskManager.getInstance().getCompactionReadRateLimiter().acquire(readSize);
      resourceFileSize -= readSize;
    }
    return resource.buildDeviceTimeIndex();
  }

  public static ModEntry convertTtlToDeletion(IDeviceID deviceID, long timeLowerBound)
      throws IllegalPathException {
    if (!deviceID.isTableModel()) {
      return new TreeDeletionEntry(
          new MeasurementPath(deviceID, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD),
          Long.MIN_VALUE,
          timeLowerBound);
    } else {
      return new TableDeletionEntry(
          new DeletionPredicate(deviceID.getTableName(), new FullExactMatch(deviceID)),
          new TimeRange(Long.MIN_VALUE, timeLowerBound));
    }
  }

  public static void executeTTLCheckObjectFilesForTableModel(
      File regionObjectDir, String databaseName) {
    File[] tableDirs = regionObjectDir.listFiles();
    if (tableDirs == null) {
      return;
    }
    boolean restrictObjectLimit =
        CommonDescriptor.getInstance().getConfig().isRestrictObjectLimit();
    for (File tableDir : tableDirs) {
      if (!tableDir.isDirectory()) {
        continue;
      }
      String tableName = tableDir.getName();
      if (!restrictObjectLimit) {
        try {
          tableName =
              new String(
                  BaseEncoding.base32().omitPadding().decode(tableName), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ignored) {
          continue;
        }
      }
      TsTable tsTable = DataNodeTableCache.getInstance().getTable(databaseName, tableName);
      if (tsTable == null) {
        continue;
      }
      long ttlInMS = CommonDateTimeUtils.convertIoTDBTimeToMillis(tsTable.getCachedTableTTL());
      if (ttlInMS == Long.MAX_VALUE) {
        continue;
      }
      // buffer 60s to avoid concurrent issues with querying
      final long timeLowerBoundInMS = CommonDateTimeUtils.currentTime() - ttlInMS - 60 * 1000;
      try {
        recursiveTTLCheckForTableDir(
            tableDir, 0, tsTable.getTagNum() + 1, !restrictObjectLimit, timeLowerBoundInMS);
      } catch (Exception e) {
        logger.warn(
            "Meet exception when checking for object files for table {}.{} in region {}",
            databaseName,
            tableName,
            regionObjectDir.getName(),
            e);
      }
    }
  }

  // We try to avoid expensive 'stat' system calls by first checking file name and only performing
  // Files.readAttributes when the file may be expired
  private static void recursiveTTLCheckForTableDir(
      File currentFile,
      int depth,
      int maxObjectFileDepth,
      boolean canDistinguishDirectoryByFileName,
      long lowerBoundInMS) {
    canDistinguishDirectoryByFileName |= depth > maxObjectFileDepth;
    String fileName = currentFile.getName();
    boolean maybeObjectFile = fileName.endsWith(".bin");
    if (maybeObjectFile) {
      if (canDistinguishDirectoryByFileName) {
        checkTTLAndDeleteExpiredObjectFile(currentFile, null, lowerBoundInMS);
        return;
      }
      try {
        BasicFileAttributes basicFileAttributes =
            Files.readAttributes(currentFile.toPath(), BasicFileAttributes.class);
        if (!basicFileAttributes.isDirectory()) {
          checkTTLAndDeleteExpiredObjectFile(currentFile, basicFileAttributes, lowerBoundInMS);
          return;
        }
      } catch (IOException ignored) {
      }
    }
    File[] children = currentFile.listFiles();
    if (children == null) {
      return;
    }
    // The rate limit may only work on filesystems like ext4, directory File.length() is
    // block-aligned and reflects allocated directory entry blocks.
    acquireCompactionReadRate(currentFile.length());
    for (File child : children) {
      recursiveTTLCheckForTableDir(
          child, depth + 1, maxObjectFileDepth, canDistinguishDirectoryByFileName, lowerBoundInMS);
    }
  }

  private static void checkTTLAndDeleteExpiredObjectFile(
      File file, @Nullable BasicFileAttributes attributes, long timeLowerBoundInMS) {
    String fileName = file.getName();
    long fileTimestampInMS;
    try {
      fileTimestampInMS = Long.parseLong(fileName.substring(0, fileName.length() - 4));
    } catch (NumberFormatException ignored) {
      return;
    }

    if (fileTimestampInMS >= timeLowerBoundInMS) {
      return;
    }

    try {
      attributes =
          attributes == null
              ? Files.readAttributes(file.toPath(), BasicFileAttributes.class)
              : attributes;
      if (attributes.isDirectory()) {
        return;
      }
      Files.delete(file.toPath());
      FileMetrics.getInstance().decreaseObjectFileNum(1);
      FileMetrics.getInstance().decreaseObjectFileSize(attributes.size());
      logger.info("Remove object file {}, size is {}(byte)", file.getPath(), attributes.size());
    } catch (Exception ignored) {
    }
  }

  private static void acquireCompactionReadRate(long size) {
    if (size <= 0) {
      return;
    }
    CompactionTaskManager.getInstance().getCompactionReadOperationRateLimiter().acquire(1);
    RateLimiter rateLimiter = CompactionTaskManager.getInstance().getCompactionReadRateLimiter();
    while (size >= Integer.MAX_VALUE) {
      size -= Integer.MAX_VALUE;
      rateLimiter.acquire(Integer.MAX_VALUE);
    }
    rateLimiter.acquire((int) size);
  }
}
