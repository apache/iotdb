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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ChunkTypeInconsistentException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionLastTimeCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.IllegalCompactionTaskSummaryException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ISeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionPerformerSubTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionSeriesContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionTableSchemaCollector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.FastCrossCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.FastInnerCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractCrossSpaceEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractInnerSpaceEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCompactionInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCrossSpaceCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.StopReadTsFileByInterruptException;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FastCompactionPerformer
    implements ICrossCompactionPerformer, ISeqCompactionPerformer, IUnseqCompactionPerformer {
  @SuppressWarnings("squid:S1068")
  private final Logger logger = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private List<TsFileResource> seqFiles = Collections.emptyList();

  private List<TsFileResource> unseqFiles = Collections.emptyList();

  private List<TsFileResource> sortedSourceFiles = new ArrayList<>();

  private static final int SUB_TASK_NUM =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  private Map<TsFileResource, TsFileSequenceReader> readerCacheMap = new ConcurrentHashMap<>();

  private FastCompactionTaskSummary subTaskSummary;

  private List<TsFileResource> targetFiles;

  // tsFile name -> modifications
  private Map<String, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
      modificationCache = new ConcurrentHashMap<>();

  private final boolean isCrossCompaction;

  private EncryptParameter encryptParameter;

  @TestOnly
  public FastCompactionPerformer(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      List<TsFileResource> targetFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
    this.targetFiles = targetFiles;
    if (seqFiles.isEmpty() || unseqFiles.isEmpty()) {
      // inner space compaction
      isCrossCompaction = false;
    } else {
      isCrossCompaction = true;
    }
    this.encryptParameter =
        new EncryptParameter(
            TSFileDescriptor.getInstance().getConfig().getEncryptType(),
            TSFileDescriptor.getInstance().getConfig().getEncryptKey());
  }

  public FastCompactionPerformer(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      List<TsFileResource> targetFiles,
      EncryptParameter encryptParameter) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
    this.targetFiles = targetFiles;
    if (seqFiles.isEmpty() || unseqFiles.isEmpty()) {
      // inner space compaction
      isCrossCompaction = false;
    } else {
      isCrossCompaction = true;
    }
    this.encryptParameter = encryptParameter;
  }

  @TestOnly
  public FastCompactionPerformer(boolean isCrossCompaction) {
    this.isCrossCompaction = isCrossCompaction;
    this.encryptParameter =
        new EncryptParameter(
            TSFileDescriptor.getInstance().getConfig().getEncryptType(),
            TSFileDescriptor.getInstance().getConfig().getEncryptKey());
  }

  public FastCompactionPerformer(boolean isCrossCompaction, EncryptParameter encryptParameter) {
    this.isCrossCompaction = isCrossCompaction;
    this.encryptParameter = encryptParameter;
  }

  @Override
  public void perform() throws Exception {
    this.subTaskSummary.setTemporalFileNum(targetFiles.size());
    try (MultiTsFileDeviceIterator deviceIterator =
            new MultiTsFileDeviceIterator(seqFiles, unseqFiles, readerCacheMap);
        AbstractCompactionWriter compactionWriter =
            isCrossCompaction
                ? new FastCrossCompactionWriter(
                    targetFiles, seqFiles, readerCacheMap, encryptParameter)
                : new FastInnerCompactionWriter(targetFiles, encryptParameter)) {
      List<Schema> schemas =
          CompactionTableSchemaCollector.collectSchema(
              seqFiles, unseqFiles, readerCacheMap, deviceIterator.getDeprecatedTableSchemaMap());
      compactionWriter.setSchemaForAllTargetFile(schemas);
      readModification(seqFiles);
      readModification(unseqFiles);
      while (deviceIterator.hasNextDevice()) {
        checkThreadInterrupted();
        Pair<IDeviceID, Boolean> deviceInfo = deviceIterator.nextDevice();
        IDeviceID device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;
        // sort the resources by the start time of current device from old to new, and remove
        // resource that does not contain the current device. Notice: when the level of time index
        // is file, there will be a false positive judgment problem, that is, the device does not
        // actually exist but the judgment return device being existed.
        sortedSourceFiles.addAll(seqFiles);
        sortedSourceFiles.addAll(unseqFiles);
        boolean isTreeModel = !isAligned || device.getTableName().startsWith("root.");
        long ttl = deviceIterator.getTTLForCurrentDevice();
        sortedSourceFiles.removeIf(x -> x.definitelyNotContains(device));
        // checked above
        //noinspection OptionalGetWithoutIsPresent
        sortedSourceFiles.sort(Comparator.comparingLong(x -> x.getStartTime(device).get()));
        ModEntry ttlDeletion = null;
        if (ttl != Long.MAX_VALUE) {
          ttlDeletion =
              CompactionUtils.convertTtlToDeletion(
                  device, deviceIterator.getTimeLowerBoundForCurrentDevice());
          for (TsFileResource sourceFile : sortedSourceFiles) {
            modificationCache
                .computeIfAbsent(
                    sourceFile.getTsFile().getName(),
                    k -> PatternTreeMapFactory.getModsPatternTreeMap())
                .append(ttlDeletion.keyOfPatternTree(), ttlDeletion);
          }
        }
        compactionWriter.setTTLDeletion(ttlDeletion);

        if (sortedSourceFiles.isEmpty()) {
          // device is out of dated in all source files
          continue;
        }

        compactionWriter.startChunkGroup(device, isAligned);

        if (isAligned) {
          compactAlignedSeries(device, deviceIterator, compactionWriter, isTreeModel);
        } else {
          compactNonAlignedSeries(device, deviceIterator, compactionWriter);
        }

        compactionWriter.endChunkGroup();
        // check whether to flush chunk metadata or not
        compactionWriter.checkAndMayFlushChunkMetadata();
        // Add temp file metrics
        subTaskSummary.setTemporaryFileSize(compactionWriter.getWriterSize());
        sortedSourceFiles.clear();
      }
      compactionWriter.endFile();
      CompactionUtils.updatePlanIndexes(targetFiles, seqFiles, unseqFiles);
    } finally {
      // readers of source files have been closed in MultiTsFileDeviceIterator
      // clean cache
      sortedSourceFiles = null;
      readerCacheMap = null;
      modificationCache = null;
    }
  }

  private void compactAlignedSeries(
      IDeviceID deviceId,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter fastCrossCompactionWriter,
      boolean ignoreAllNullRows)
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>, including
    // empty value chunk metadata
    Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new LinkedHashMap<>();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

    // Get all value measurements and their schemas of the current device. Also get start offset and
    // end offset of each timeseries metadata, in order to facilitate the reading of chunkMetadata
    // directly by this offset later. Instead of deserializing chunk metadata later, we need to
    // deserialize chunk metadata here to get the schemas of all value measurements, because we
    // should get schemas of all value measurement to startMeasurement() and compaction process is
    // to read a batch of overlapped files each time, and we cannot make sure if the first batch of
    // overlapped tsfiles contain all the value measurements.
    for (Map.Entry<String, Pair<MeasurementSchema, Map<TsFileResource, Pair<Long, Long>>>> entry :
        deviceIterator.getTimeseriesSchemaAndMetadataOffsetOfCurrentDevice().entrySet()) {
      measurementSchemas.add(entry.getValue().left);
      timeseriesMetadataOffsetMap.put(entry.getKey(), entry.getValue().right);
    }
    // current device may be ignored by some conditions
    if (measurementSchemas.isEmpty()) {
      return;
    }

    FastCompactionTaskSummary taskSummary = new FastCompactionTaskSummary();
    new FastCompactionPerformerSubTask(
            fastCrossCompactionWriter,
            timeseriesMetadataOffsetMap,
            readerCacheMap,
            modificationCache,
            sortedSourceFiles,
            measurementSchemas,
            deviceId,
            taskSummary,
            ignoreAllNullRows)
        .call();
    subTaskSummary.increase(taskSummary);
  }

  private void compactNonAlignedSeries(
      IDeviceID deviceID,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter fastCrossCompactionWriter)
      throws IOException, InterruptedException {
    // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
    // Get all measurements of the current device. Also get start offset and end offset of each
    // timeseries metadata, in order to facilitate the reading of chunkMetadata directly by this
    // offset later. Here we don't need to deserialize chunk metadata, we can deserialize them and
    // get their schema later.
    Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new LinkedHashMap<>();

    Map<String, TSDataType> measurementDataTypeMap = new LinkedHashMap<>();

    Map<String, CompactionSeriesContext> compactionSeriesContextMap =
        deviceIterator.getCompactionSeriesContextOfCurrentDevice();

    for (Map.Entry<String, CompactionSeriesContext> entry : compactionSeriesContextMap.entrySet()) {
      timeseriesMetadataOffsetMap.put(
          entry.getKey(), entry.getValue().getFileTimeseriesMetdataOffsetMap());
      measurementDataTypeMap.put(entry.getKey(), entry.getValue().getFinalType());
    }

    List<String> allMeasurements = new ArrayList<>(timeseriesMetadataOffsetMap.keySet());
    allMeasurements.sort((String::compareTo));

    int subTaskNums = Math.min(allMeasurements.size(), SUB_TASK_NUM);

    // assign all measurements to different sub tasks
    List<String>[] measurementsForEachSubTask = new ArrayList[subTaskNums];
    for (int idx = 0; idx < allMeasurements.size(); idx++) {
      if (measurementsForEachSubTask[idx % subTaskNums] == null) {
        measurementsForEachSubTask[idx % subTaskNums] = new ArrayList<>();
      }
      measurementsForEachSubTask[idx % subTaskNums].add(allMeasurements.get(idx));
    }

    // construct sub tasks and start compacting measurements in parallel
    List<Future<Void>> futures = new ArrayList<>();
    List<FastCompactionTaskSummary> taskSummaryList = new ArrayList<>();
    for (int i = 0; i < subTaskNums; i++) {
      FastCompactionTaskSummary taskSummary = new FastCompactionTaskSummary();
      futures.add(
          CompactionTaskManager.getInstance()
              .submitSubTask(
                  new FastCompactionPerformerSubTask(
                      compactionSeriesContextMap,
                      fastCrossCompactionWriter,
                      readerCacheMap,
                      modificationCache,
                      sortedSourceFiles,
                      measurementsForEachSubTask[i],
                      deviceID,
                      taskSummary,
                      i)));
      taskSummaryList.add(taskSummary);
    }

    // wait for all sub tasks to finish
    for (int i = 0; i < subTaskNums; i++) {
      try {
        futures.get(i).get();
        subTaskSummary.increase(taskSummaryList.get(i));
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof CompactionLastTimeCheckFailedException) {
          throw (CompactionLastTimeCheckFailedException) cause;
        } else if (cause instanceof StopReadTsFileByInterruptException) {
          throw (StopReadTsFileByInterruptException) cause;
        } else if (cause instanceof ChunkTypeInconsistentException) {
          throw (ChunkTypeInconsistentException) cause;
        }
        throw new IOException("[Compaction] SubCompactionTask meet errors ", e);
      } catch (InterruptedException e) {
        abortAllSubTasks(futures);
        throw e;
      }
    }
  }

  private void abortAllSubTasks(List<Future<Void>> futures) {
    for (Future<Void> future : futures) {
      future.cancel(true);
    }
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (Exception ignored) {
      }
    }
  }

  @Override
  public void setTargetFiles(List<TsFileResource> targetFiles) {
    this.targetFiles = targetFiles;
  }

  @Override
  public void setSummary(CompactionTaskSummary summary) {
    if (!(summary instanceof FastCompactionTaskSummary)) {
      throw new IllegalCompactionTaskSummaryException(
          "CompactionTaskSummary for FastCompactionPerformer "
              + "should be FastCompactionTaskSummary");
    }
    this.subTaskSummary = (FastCompactionTaskSummary) summary;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  private void checkThreadInterrupted() throws InterruptedException {
    if (Thread.interrupted() || subTaskSummary.isCancel()) {
      throw new InterruptedException(
          String.format(
              "[Compaction] compaction for target file %s abort", targetFiles.toString()));
    }
  }

  public FastCompactionTaskSummary getSubTaskSummary() {
    return subTaskSummary;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> unseqFiles) {
    this.seqFiles = unseqFiles;
  }

  private void readModification(List<TsFileResource> resources) {
    for (TsFileResource resource : resources) {
      if (resource.getTotalModSizeInByte() == 0) {
        continue;
      }
      // read mods
      PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications =
          CompactionUtils.buildModEntryPatternTreeMap(resource);
      modificationCache.put(resource.getTsFile().getName(), modifications);
    }
  }

  public String getDatabaseName() {
    return !seqFiles.isEmpty()
        ? seqFiles.get(0).getDatabaseName()
        : unseqFiles.get(0).getDatabaseName();
  }

  @Override
  public Optional<AbstractInnerSpaceEstimator> getInnerSpaceEstimator() {
    return Optional.of(new FastCompactionInnerCompactionEstimator());
  }

  @Override
  public Optional<AbstractCrossSpaceEstimator> getCrossSpaceEstimator() {
    return Optional.of(new FastCrossSpaceCompactionEstimator());
  }
}
