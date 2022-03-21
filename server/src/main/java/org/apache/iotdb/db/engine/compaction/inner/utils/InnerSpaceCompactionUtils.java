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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.CrossCompactionStrategy;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.manage.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.RewriteCompactionFileSelector;
import org.apache.iotdb.db.engine.compaction.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
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

public class InnerSpaceCompactionUtils {

  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private InnerSpaceCompactionUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static void compact(TsFileResource targetResource, List<TsFileResource> tsFileResources)
      throws IOException, MetadataException, InterruptedException {

    try (MultiTsFileDeviceIterator deviceIterator = new MultiTsFileDeviceIterator(tsFileResources);
        TsFileIOWriter writer = new TsFileIOWriter(targetResource.getTsFile())) {
      while (deviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean aligned = deviceInfo.right;

        writer.startChunkGroup(device);
        if (aligned) {
          compactAlignedSeries(device, targetResource, writer, deviceIterator);
        } else {
          compactNotAlignedSeries(device, targetResource, writer, deviceIterator);
        }
        writer.endChunkGroup();
      }

      for (TsFileResource tsFileResource : tsFileResources) {
        targetResource.updatePlanIndexes(tsFileResource);
      }
      writer.endFile();
      targetResource.close();
    }
  }

  private static void checkThreadInterrupted(TsFileResource tsFileResource)
      throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException(
          String.format(
              "[Compaction] compaction for target file %s abort", tsFileResource.toString()));
    }
  }

  private static void compactNotAlignedSeries(
      String device,
      TsFileResource targetResource,
      TsFileIOWriter writer,
      MultiTsFileDeviceIterator deviceIterator)
      throws IOException, MetadataException, InterruptedException {
    MultiTsFileDeviceIterator.MeasurementIterator seriesIterator =
        deviceIterator.iterateNotAlignedSeries(device, true);
    while (seriesIterator.hasNextSeries()) {
      checkThreadInterrupted(targetResource);
      // TODO: we can provide a configuration item to enable concurrent between each series
      PartialPath p = new PartialPath(device, seriesIterator.nextSeries());
      IMeasurementSchema measurementSchema;
      // TODO: seriesIterator needs to be refactor.
      // This statement must be called before next hasNextSeries() called, or it may be trapped in a
      // dead-loop.
      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList =
          seriesIterator.getMetadataListForCurrentSeries();
      try {
        if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
          measurementSchema =
              IDTableManager.getInstance().getSeriesSchema(device, p.getMeasurement());
        } else {
          measurementSchema = IoTDB.schemaEngine.getSeriesSchema(p);
        }
      } catch (PathNotExistException e) {
        logger.info("A deleted path is skipped: {}", e.getMessage());
        continue;
      }
      SingleSeriesCompactionExecutor compactionExecutorOfCurrentTimeSeries =
          new SingleSeriesCompactionExecutor(
              p, measurementSchema, readerAndChunkMetadataList, writer, targetResource);
      compactionExecutorOfCurrentTimeSeries.execute();
    }
  }

  private static void compactAlignedSeries(
      String device,
      TsFileResource targetResource,
      TsFileIOWriter writer,
      MultiTsFileDeviceIterator deviceIterator)
      throws IOException, InterruptedException {
    checkThreadInterrupted(targetResource);
    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList =
        deviceIterator.getReaderAndChunkMetadataForCurrentAlignedSeries();
    AlignedSeriesCompactionExecutor compactionExecutor =
        new AlignedSeriesCompactionExecutor(
            device, targetResource, readerAndChunkMetadataList, writer);
    compactionExecutor.execute();
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
      long budget, CrossSpaceCompactionResource resource) {
    CrossCompactionStrategy strategy =
        IoTDBDescriptor.getInstance().getConfig().getCrossCompactionStrategy();
    switch (strategy) {
      case REWRITE_COMPACTION:
        return new RewriteCompactionFileSelector(resource, budget);
      default:
        throw new UnsupportedOperationException("Unknown CrossSpaceFileStrategy " + strategy);
    }
  }

  public static File[] findInnerSpaceCompactionLogs(String directory) {
    File timePartitionDir = new File(directory);
    if (timePartitionDir.exists()) {
      return timePartitionDir.listFiles(
          (dir, name) -> name.endsWith(CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX));
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
    if (!targetResource.getTsFilePath().endsWith(IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)) {
      logger.warn(
          "{} [Compaction] Tmp target tsfile {} should be end with {}",
          fullStorageGroupName,
          targetResource.getTsFilePath(),
          IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX);
      return;
    }
    File oldFile = targetResource.getTsFile();

    // move TsFile and delete old tsfile
    String newFilePath =
        targetResource
            .getTsFilePath()
            .replace(IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX);
    File newFile = new File(newFilePath);
    FSFactoryProducer.getFSFactory().moveFile(oldFile, newFile);

    // serialize xxx.tsfile.resource
    targetResource.setFile(newFile);
    targetResource.serialize();
    targetResource.close();
  }
}
