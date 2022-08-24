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
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.constant.CrossCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.ICrossSpaceCompactionFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.RewriteCompactionFileSelector;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

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
    targetResource.close();
  }

  /**
   * Collect all the compaction modification files of source files, and combines them as the
   * modification file of target file.
   */
  // Todo: improve performance
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
      if (!deleteTsFile(mergeTsFile)) {
        result = false;
      }
      logger.info(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
    return result;
  }

  public static boolean deleteTsFile(TsFileResource seqFile) {
    try {
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.remove();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      return false;
    }
    return true;
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

  public static ICrossSpaceCompactionFileSelector getCrossSpaceFileSelector(
      long budget, CrossSpaceCompactionResource resource) {
    CrossCompactionSelector strategy =
        IoTDBDescriptor.getInstance().getConfig().getCrossCompactionSelector();
    switch (strategy) {
      case REWRITE:
        return new RewriteCompactionFileSelector(resource, budget);
      default:
        throw new UnsupportedOperationException("Unknown CrossSpaceFileStrategy " + strategy);
    }
  }

  public static void updateDeviceStartTimeAndEndTime(
      List<TsFileResource> targetResources, List<TsFileIOWriter> targetFileWriters) {
    for (int i = 0; i < targetFileWriters.size(); i++) {
      TsFileIOWriter fileIOWriter = targetFileWriters.get(i);
      TsFileResource fileResource = targetResources.get(i);
      // The tmp target file may does not have any data points written due to the existence of the
      // mods file, and it will be deleted after compaction. So skip the target file that has been
      // deleted.
      if (!fileResource.getTsFile().exists()
          || fileIOWriter.getDeviceTimeseriesMetadataMap() == null) {
        continue;
      }
      for (Map.Entry<String, List<TimeseriesMetadata>> entry :
          fileIOWriter.getDeviceTimeseriesMetadataMap().entrySet()) {
        String device = entry.getKey();
        for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
          fileResource.updateStartTime(device, timeseriesMetadata.getStatistics().getStartTime());
          fileResource.updateEndTime(device, timeseriesMetadata.getStatistics().getEndTime());
        }
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
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource targetResource = targetResources.get(i);
      // remove the target file been deleted from list
      if (!targetResource.getTsFile().exists()) {
        targetResources.remove(i);
        targetResources.add(i, null);
        continue;
      }
      for (TsFileResource unseqResource : unseqResources) {
        targetResource.updatePlanIndexes(unseqResource);
      }
      for (TsFileResource seqResource : seqResources) {
        targetResource.updatePlanIndexes(seqResource);
      }
    }
  }

  public static List<TsFileResource> sortSourceFiles(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    List<TsFileResource> allResources = new LinkedList<>(seqFiles);
    allResources.addAll(unseqFiles);
    // sort the tsfile by version, from the newest to oldest
    allResources.sort(
        (o1, o2) -> {
          try {
            TsFileNameGenerator.TsFileName n1 =
                TsFileNameGenerator.getTsFileName(o1.getTsFile().getName());
            TsFileNameGenerator.TsFileName n2 =
                TsFileNameGenerator.getTsFileName(o2.getTsFile().getName());
            return (int) (n2.getVersion() - n1.getVersion());
          } catch (IOException e) {
            return 0;
          }
        });
    return allResources;
  }

  public static Map<String, MeasurementSchema> getMeasurementSchema(
      String device,
      Set<String> measurements,
      List<TsFileResource> allResources,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap)
      throws IllegalPathException, IOException {
    HashMap<String, MeasurementSchema> schemaMap = new HashMap<>();
    for (String measurement : measurements) {
      for (TsFileResource tsFileResource : allResources) {
        if (!tsFileResource.mayContainsDevice(device)) {
          continue;
        }
        TsFileSequenceReader reader;
        if (readerCacheMap == null) {
          // get reader from fileReaderManager, used by readPointCompactionPerformer
          reader = FileReaderManager.getInstance().get(tsFileResource.getTsFilePath(), true);
        } else {
          reader =
              readerCacheMap.computeIfAbsent(
                  tsFileResource,
                  resource -> {
                    try {
                      return new TsFileSequenceReader(resource.getTsFilePath());
                    } catch (IOException e) {
                      throw new RuntimeException(
                          String.format(
                              "Failed to construct sequence reader for %s", tsFileResource));
                    }
                  });
        }
        MeasurementSchema schema =
            getMeasurementSchemaFromReader(tsFileResource, reader, device, measurement);
        if (schema != null) {
          schemaMap.put(measurement, schema);
          break;
        }
      }
    }
    return schemaMap;
  }

  private static MeasurementSchema getMeasurementSchemaFromReader(
      TsFileResource resource, TsFileSequenceReader reader, String device, String measurement)
      throws IllegalPathException, IOException {
    List<ChunkMetadata> chunkMetadata =
        reader.getChunkMetadataList(new PartialPath(device, measurement));
    if (chunkMetadata.size() > 0) {
      chunkMetadata.get(chunkMetadata.size() - 1).setFilePath(resource.getTsFilePath());
      Chunk chunk = ChunkCache.getInstance().get(chunkMetadata.get(chunkMetadata.size() - 1));
      // Chunk chunk = reader.readMemChunk(chunkMetadata.get(chunkMetadata.size() - 1));
      ChunkHeader header = chunk.getHeader();
      return new MeasurementSchema(
          measurement, header.getDataType(), header.getEncodingType(), header.getCompressionType());
    }
    return null;
  }
}
