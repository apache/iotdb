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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.SubCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.CrossSpaceCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * This tool can be used to perform inner space or cross space compaction of aligned and non aligned
 * timeseries . Currently, we use {@link
 * org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils} to speed up if it is
 * an seq inner space compaction.
 */
public class CompactionUtils {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  public static void compact(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException, MetadataException, StorageEngineException, InterruptedException {
    long queryId = QueryResourceManager.getInstance().assignCompactionQueryId();
    QueryContext queryContext = new QueryContext(queryId);
    QueryDataSource queryDataSource = new QueryDataSource(seqFileResources, unseqFileResources);
    QueryResourceManager.getInstance()
        .getQueryFileManager()
        .addUsedFilesForQuery(queryId, queryDataSource);

    try (AbstractCompactionWriter compactionWriter =
        getCompactionWriter(seqFileResources, unseqFileResources, targetFileResources)) {
      // Do not close device iterator, because tsfile reader is managed by FileReaderManager.
      MultiTsFileDeviceIterator deviceIterator =
          new MultiTsFileDeviceIterator(seqFileResources, unseqFileResources);
      while (deviceIterator.hasNextDevice()) {
        checkThreadInterrupted(targetFileResources);
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;
        QueryUtils.fillOrderIndexes(queryDataSource, device, true);

        if (isAligned) {
          compactAlignedSeries(
              device, deviceIterator, compactionWriter, queryContext, queryDataSource);
        } else {
          compactNonAlignedSeries(
              device, deviceIterator, compactionWriter, queryContext, queryDataSource);
        }
      }

      compactionWriter.endFile();
      updatePlanIndexes(targetFileResources, seqFileResources, unseqFileResources);
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
    }
  }

  private static void compactAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      QueryContext queryContext,
      QueryDataSource queryDataSource)
      throws IOException, MetadataException {
    Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();

    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(schemaMap.values());
    if (measurementSchemas.isEmpty()) {
      return;
    }
    List<String> existedMeasurements =
        measurementSchemas.stream()
            .map(IMeasurementSchema::getMeasurementId)
            .collect(Collectors.toList());
    IBatchReader dataBatchReader =
        constructReader(
            device,
            existedMeasurements,
            measurementSchemas,
            schemaMap.keySet(),
            queryContext,
            queryDataSource,
            true);

    if (dataBatchReader.hasNextBatch()) {
      // chunkgroup is serialized only when at least one timeseries under this device has data
      compactionWriter.startChunkGroup(device, true);
      compactionWriter.startMeasurement(measurementSchemas, 0);
      writeWithReader(compactionWriter, dataBatchReader, 0);
      compactionWriter.endMeasurement(0);
      compactionWriter.endChunkGroup();
    }
    compactionWriter.checkAndMayFlushChunkMetadata();
  }

  private static void compactNonAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      QueryContext queryContext,
      QueryDataSource queryDataSource)
      throws IOException, InterruptedException {
    Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();
    List<String> allMeasurements = new ArrayList<>(schemaMap.keySet());
    allMeasurements.sort((String::compareTo));
    int subTaskNums = Math.min(allMeasurements.size(), subTaskNum);
    // construct sub tasks and start compacting measurements in parallel
    if (subTaskNums > 0) {
      // assign the measurements for each subtask
      List<String>[] measurementListArray = new List[subTaskNums];
      for (int i = 0, size = allMeasurements.size(); i < size; ++i) {
        int index = i % subTaskNums;
        if (measurementListArray[index] == null) {
          measurementListArray[index] = new LinkedList<>();
        }
        measurementListArray[index].add(allMeasurements.get(i));
      }

      // construct sub tasks and start compacting measurements in parallel
      List<Future<Void>> futures = new ArrayList<>();
      compactionWriter.startChunkGroup(device, false);
      for (int i = 0; i < subTaskNums; i++) {
        futures.add(
            CompactionTaskManager.getInstance()
                .submitSubTask(
                    new SubCompactionTask(
                        device,
                        measurementListArray[i],
                        queryContext,
                        queryDataSource,
                        compactionWriter,
                        schemaMap,
                        i)));
      }

      // wait for all sub tasks finish
      for (int i = 0; i < subTaskNums; i++) {
        try {
          futures.get(i).get();
        } catch (InterruptedException | ExecutionException e) {
          logger.error("SubCompactionTask meet errors ", e);
          Thread.interrupted();
          throw new InterruptedException();
        }
      }
    }
    compactionWriter.endChunkGroup();
    compactionWriter.checkAndMayFlushChunkMetadata();
  }

  public static void writeWithReader(
      AbstractCompactionWriter writer, IBatchReader reader, int subTaskId) throws IOException {
    while (reader.hasNextBatch()) {
      BatchData batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        writer.write(batchData.currentTime(), batchData.currentValue(), subTaskId);
        batchData.next();
      }
    }
  }

  /**
   * @param measurementIds if device is aligned, then measurementIds contain all measurements. If
   *     device is not aligned, then measurementIds only contain one measurement.
   */
  public static IBatchReader constructReader(
      String deviceId,
      List<String> measurementIds,
      List<IMeasurementSchema> measurementSchemas,
      Set<String> allSensors,
      QueryContext queryContext,
      QueryDataSource queryDataSource,
      boolean isAlign)
      throws IllegalPathException {
    PartialPath seriesPath;
    TSDataType tsDataType;
    if (isAlign) {
      seriesPath = new AlignedPath(deviceId, measurementIds, measurementSchemas);
      tsDataType = TSDataType.VECTOR;
    } else {
      seriesPath = new MeasurementPath(deviceId, measurementIds.get(0), measurementSchemas.get(0));
      tsDataType = measurementSchemas.get(0).getType();
    }
    return new SeriesRawDataBatchReader(
        seriesPath, allSensors, tsDataType, queryContext, queryDataSource, null, null, null, true);
  }

  private static AbstractCompactionWriter getCompactionWriter(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException {
    if (!seqFileResources.isEmpty() && !unseqFileResources.isEmpty()) {
      // cross space
      return new CrossSpaceCompactionWriter(targetFileResources, seqFileResources);
    } else {
      // inner space
      return new InnerSpaceCompactionWriter(targetFileResources.get(0));
    }
  }

  private static void updatePlanIndexes(
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
      // remove the target file that has been deleted from list
      if (!targetResource.getTsFile().exists()) {
        targetResources.remove(i--);
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
      moveOneTargetFile(targetResource, fileSuffix, fullStorageGroupName);
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
  public static void combineModsInCompaction(
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources,
      List<TsFileResource> targetResources)
      throws IOException {
    // target file may less than source seq files, so we should find each target file with its
    // corresponding source seq file.
    Map<String, TsFileResource> seqFileInfoMap = new HashMap<>();
    for (TsFileResource tsFileResource : seqResources) {
      seqFileInfoMap.put(
          TsFileNameGenerator.increaseCrossCompactionCnt(tsFileResource.getTsFile()).getName(),
          tsFileResource);
    }
    // update each target mods file.
    for (TsFileResource tsFileResource : targetResources) {
      updateOneTargetMods(
          tsFileResource, seqFileInfoMap.get(tsFileResource.getTsFile().getName()), unseqResources);
    }
  }

  private static void updateOneTargetMods(
      TsFileResource targetFile, TsFileResource seqFile, List<TsFileResource> unseqFiles)
      throws IOException {
    // write mods in the seq file
    if (seqFile != null) {
      ModificationFile seqCompactionModificationFile = ModificationFile.getCompactionMods(seqFile);
      for (Modification modification : seqCompactionModificationFile.getModifications()) {
        targetFile.getModFile().write(modification);
      }
    }
    // write mods in all unseq files
    for (TsFileResource unseqFile : unseqFiles) {
      ModificationFile compactionUnseqModificationFile =
          ModificationFile.getCompactionMods(unseqFile);
      for (Modification modification : compactionUnseqModificationFile.getModifications()) {
        targetFile.getModFile().write(modification);
      }
    }
    targetFile.getModFile().close();
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

  private static void checkThreadInterrupted(List<TsFileResource> tsFileResource)
      throws InterruptedException {
    if (Thread.interrupted() || !IoTDB.activated) {
      throw new InterruptedException(
          String.format(
              "[Compaction] compaction for target file %s abort", tsFileResource.toString()));
    }
  }
}
