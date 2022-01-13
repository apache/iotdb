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

package org.apache.iotdb.db.engine.compaction.inner.utils;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.MaxFileMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.MergeFileStrategy;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class InnerSpaceCompactionUtils {

  private static final Logger logger = LoggerFactory.getLogger("COMPACTION");

  private InnerSpaceCompactionUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static void compact(
      TsFileResource targetResource,
      List<TsFileResource> tsFileResources,
      String storageGroup,
      boolean sequence)
      throws IOException, MetadataException {
    TsFileIOWriter writer = null;
    try (MultiTsFileDeviceIterator deviceIterator =
        new MultiTsFileDeviceIterator(tsFileResources)) {
      writer = new TsFileIOWriter(targetResource.getTsFile());
      Set<String> devices = deviceIterator.getDevices();
      for (String device : devices) {
        writer.startChunkGroup(device);
        // TODO: compact a aligned device
        MultiTsFileDeviceIterator.MeasurementIterator seriesIterator =
            deviceIterator.iterateOneSeries(device);
        while (seriesIterator.hasNextSeries()) {
          // TODO: we can provide a configuration item to enable concurrent between each series
          String currentSeries = seriesIterator.nextSeries();
          LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList =
              seriesIterator.getMetadataListForCurrentSeries();
          SingleSeriesCompactionExecutor compactionExecutorOfCurrentTimeSeries =
              new SingleSeriesCompactionExecutor(
                  device,
                  currentSeries,
                  readerAndChunkMetadataList,
                  writer,
                  targetResource,
                  sequence);
          compactionExecutorOfCurrentTimeSeries.execute();
        }

        writer.endChunkGroup();
      }

      for (TsFileResource tsFileResource : tsFileResources) {
        targetResource.updatePlanIndexes(tsFileResource);
      }
      writer.endFile();
      targetResource.close();
    } finally {
      if (writer != null && writer.canWrite()) {
        writer.close();
      }
    }
  }

  public static boolean deleteTsFilesInDisk(
      Collection<TsFileResource> mergeTsFiles, String storageGroupName) {
    logger.info("{} [Compaction] Compaction starts to delete real file ", storageGroupName);
    boolean result = true;
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      if (!deleteTsFile(mergeTsFile)) {
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
        normalModification.remove();
      }
    }
  }

  /**
   * This method is called to recover modifications while an exception occurs during compaction. It
   * append new modifications of each selected tsfile to its corresponding old mods file and delete
   * the compaction mods file.
   *
   * @param selectedTsFileResources
   * @throws IOException
   */
  public static void appendNewModificationsToOldModsFile(
      List<TsFileResource> selectedTsFileResources) throws IOException {
    for (TsFileResource sourceFile : selectedTsFileResources) {
      // if there are modifications to this seqFile during compaction
      if (sourceFile.getCompactionModFile().exists()) {
        ModificationFile compactionModificationFile =
            ModificationFile.getCompactionMods(sourceFile);
        Collection<Modification> newModification = compactionModificationFile.getModifications();
        compactionModificationFile.close();
        sourceFile.resetModFile();
        // write the new modifications to its old modification file
        try (ModificationFile oldModificationFile = sourceFile.getModFile()) {
          for (Modification modification : newModification) {
            oldModificationFile.write(modification);
          }
        }
        FileUtils.delete(new File(ModificationFile.getCompactionMods(sourceFile).getFilePath()));
      }
    }
  }

  /**
   * Collect all the compaction modification files of source files, and combines them as the
   * modification file of target file.
   */
  public static void combineModsInCompaction(
      Collection<TsFileResource> mergeTsFiles, TsFileResource targetTsFile) throws IOException {
    List<Modification> modifications = new ArrayList<>();
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      try (ModificationFile sourceCompactionModificationFile =
          ModificationFile.getCompactionMods(mergeTsFile)) {
        modifications.addAll(sourceCompactionModificationFile.getModifications());
      }
    }
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile = ModificationFile.getNormalMods(targetTsFile)) {
        for (Modification modification : modifications) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          modificationFile.write(modification);
        }
      }
    }
  }

  public static boolean deleteTsFile(TsFileResource seqFile) {
    try {
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      return false;
    }
    return true;
  }

  public static ICrossSpaceMergeFileSelector getCrossSpaceFileSelector(
      long budget, CrossSpaceMergeResource resource) {
    MergeFileStrategy strategy = IoTDBDescriptor.getInstance().getConfig().getMergeFileStrategy();
    switch (strategy) {
      case MAX_FILE_NUM:
        return new MaxFileMergeFileSelector(resource, budget);
      case MAX_SERIES_NUM:
        return new MaxSeriesMergeFileSelector(resource, budget);
      default:
        throw new UnsupportedOperationException("Unknown CrossSpaceFileStrategy " + strategy);
    }
  }

  public static File[] findInnerSpaceCompactionLogs(String directory) {
    File timePartitionDir = new File(directory);
    if (timePartitionDir.exists()) {
      return timePartitionDir.listFiles(
          (dir, name) -> name.endsWith(SizeTieredCompactionLogger.COMPACTION_LOG_NAME));
    } else {
      return new File[0];
    }
  }

  public static class TsFileNameComparator implements Comparator<TsFileSequenceReader> {

    @Override
    public int compare(TsFileSequenceReader o1, TsFileSequenceReader o2) {
      return TsFileManager.compareFileName(new File(o1.getFileName()), new File(o2.getFileName()));
    }
  }
  /**
   * Update the targetResource. Move xxx.target to xxx.tsfile and serialize xxx.tsfile.resource .
   *
   * @param targetResource the old tsfile to be moved, which is xxx.target
   */
  public static void moveTargetFile(TsFileResource targetResource, String fullStorageGroupName)
      throws IOException {
    if (!targetResource.getTsFilePath().endsWith(IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX)) {
      logger.warn(
          "{} [Compaction] Tmp target tsfile {} should be end with {}",
          fullStorageGroupName,
          targetResource.getTsFilePath(),
          IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX);
      return;
    }
    File oldFile = targetResource.getTsFile();

    // move TsFile and delete old tsfile
    String newFilePath =
        targetResource
            .getTsFilePath()
            .replace(IoTDBConstant.COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX);
    File newFile = new File(newFilePath);
    FSFactoryProducer.getFSFactory().moveFile(oldFile, newFile);

    // serialize xxx.tsfile.resource
    targetResource.setFile(newFile);
    targetResource.serialize();
    targetResource.close();
  }
}
