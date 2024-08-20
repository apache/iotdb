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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionLastTimeCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.IllegalCompactionTaskSummaryException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ISeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionPerformerSubTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionTableSchemaCollector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.FastCrossCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.FastInnerCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

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
  private Map<String, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>>
      modificationCache = new ConcurrentHashMap<>();

  private final boolean isCrossCompaction;

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
  }

  public FastCompactionPerformer(boolean isCrossCompaction) {
    this.isCrossCompaction = isCrossCompaction;
  }

  @Override
  public void perform() throws Exception {
    this.subTaskSummary.setTemporalFileNum(targetFiles.size());
    try (MultiTsFileDeviceIterator deviceIterator =
            new MultiTsFileDeviceIterator(seqFiles, unseqFiles, readerCacheMap);
        AbstractCompactionWriter compactionWriter =
            isCrossCompaction
                ? new FastCrossCompactionWriter(targetFiles, seqFiles, readerCacheMap)
                : new FastInnerCompactionWriter(targetFiles)) {
      List<Schema> schemas =
          CompactionTableSchemaCollector.collectSchema(seqFiles, unseqFiles, readerCacheMap);
      compactionWriter.setSchemaForAllTargetFile(schemas);
      readModification(seqFiles);
      readModification(unseqFiles);
      while (deviceIterator.hasNextDevice()) {
        checkThreadInterrupted();
        Pair<IDeviceID, Boolean> deviceInfo = deviceIterator.nextDevice();
        IDeviceID device = deviceInfo.left;
        // sort the resources by the start time of current device from old to new, and remove
        // resource that does not contain the current device. Notice: when the level of time index
        // is file, there will be a false positive judgment problem, that is, the device does not
        // actually exist but the judgment return device being existed.
        sortedSourceFiles.addAll(seqFiles);
        sortedSourceFiles.addAll(unseqFiles);
        sortedSourceFiles.removeIf(
            x ->
                x.definitelyNotContains(device)
                    || !x.isDeviceAlive(
                        device,
                        DataNodeTTLCache.getInstance()
                            // TODO: remove deviceId conversion
                            .getTTL(device)));
        sortedSourceFiles.sort(Comparator.comparingLong(x -> x.getStartTime(device)));

        if (sortedSourceFiles.isEmpty()) {
          // device is out of dated in all source files
          continue;
        }

        boolean isAligned = deviceInfo.right;
        compactionWriter.startChunkGroup(device, isAligned);

        if (isAligned) {
          compactAlignedSeries(device, deviceIterator, compactionWriter);
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
      AbstractCompactionWriter fastCrossCompactionWriter)
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
    // should get schemas of all value measurement to startMeasruement() and compaction process is
    // to read a batch of overlapped files each time, and we cannot make sure if the first batch of
    // overlapped tsfiles contain all the value measurements.
    for (Map.Entry<String, Pair<MeasurementSchema, Map<TsFileResource, Pair<Long, Long>>>> entry :
        deviceIterator.getTimeseriesSchemaAndMetadataOffsetOfCurrentDevice().entrySet()) {
      measurementSchemas.add(entry.getValue().left);
      timeseriesMetadataOffsetMap.put(entry.getKey(), entry.getValue().right);
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
            taskSummary)
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
        deviceIterator.getTimeseriesMetadataOffsetOfCurrentDevice();

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
                      fastCrossCompactionWriter,
                      timeseriesMetadataOffsetMap,
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
        }
        if (cause instanceof StopReadTsFileByInterruptException) {
          throw (StopReadTsFileByInterruptException) cause;
        }
        throw new IOException("[Compaction] SubCompactionTask meet errors ", e);
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
      if (resource.getModFile() == null || !resource.getModFile().exists()) {
        continue;
      }
      // read mods
      PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer> modifications =
          PatternTreeMapFactory.getModsPatternTreeMap();
      for (Modification modification : resource.getModFile().getModificationsIter()) {
        modifications.append(modification.getPath(), modification);
      }
      modificationCache.put(resource.getTsFile().getName(), modifications);
    }
  }
}
