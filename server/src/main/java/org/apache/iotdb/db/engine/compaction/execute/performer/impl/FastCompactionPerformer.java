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
package org.apache.iotdb.db.engine.compaction.execute.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.TsFileMetricManager;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.ISeqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.task.subtask.FastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.execute.utils.writer.FastCrossCompactionWriter;
import org.apache.iotdb.db.engine.compaction.execute.utils.writer.FastInnerCompactionWriter;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TIME_COLUMN_ID;

public class FastCompactionPerformer
    implements ICrossCompactionPerformer, ISeqCompactionPerformer, IUnseqCompactionPerformer {
  private final Logger LOGGER = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private List<TsFileResource> seqFiles = Collections.emptyList();

  private List<TsFileResource> unseqFiles = Collections.emptyList();

  private List<TsFileResource> sortedSourceFiles = new ArrayList<>();

  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  public Map<TsFileResource, TsFileSequenceReader> readerCacheMap = new ConcurrentHashMap<>();

  private FastCompactionTaskSummary subTaskSummary;

  private List<TsFileResource> targetFiles;

  public Map<TsFileResource, List<Modification>> modificationCache = new ConcurrentHashMap<>();

  private boolean isCrossCompaction;

  private long tempFileSize = 0L;

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
  public void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException {
    TsFileMetricManager.getInstance()
        .addCompactionTempFileNum(!isCrossCompaction, !seqFiles.isEmpty(), targetFiles.size());
    try (MultiTsFileDeviceIterator deviceIterator =
            new MultiTsFileDeviceIterator(seqFiles, unseqFiles, readerCacheMap);
        AbstractCompactionWriter compactionWriter =
            isCrossCompaction
                ? new FastCrossCompactionWriter(targetFiles, seqFiles, readerCacheMap)
                : new FastInnerCompactionWriter(targetFiles.get(0))) {
      while (deviceIterator.hasNextDevice()) {
        checkThreadInterrupted();
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;
        // sort the resources by the start time of current device from old to new, and remove
        // resource that does not contain the current device. Notice: when the level of time index
        // is file, there will be a false positive judgment problem, that is, the device does not
        // actually exist but the judgment return device being existed.
        sortedSourceFiles.addAll(seqFiles);
        sortedSourceFiles.addAll(unseqFiles);
        sortedSourceFiles.removeIf(x -> !x.mayContainsDevice(device));
        sortedSourceFiles.sort(Comparator.comparingLong(x -> x.getStartTime(device)));

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
        long currentTempFileSize = compactionWriter.getWriterSize();
        TsFileMetricManager.getInstance()
            .addCompactionTempFileSize(
                !isCrossCompaction, !seqFiles.isEmpty(), currentTempFileSize - tempFileSize);
        tempFileSize = currentTempFileSize;
        sortedSourceFiles.clear();
      }
      compactionWriter.endFile();
      CompactionUtils.updatePlanIndexes(targetFiles, seqFiles, unseqFiles);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // readers of source files have been closed in MultiTsFileDeviceIterator
      // clean cache
      sortedSourceFiles = null;
      readerCacheMap = null;
      modificationCache = null;
      TsFileMetricManager.getInstance()
          .addCompactionTempFileNum(!isCrossCompaction, !seqFiles.isEmpty(), -targetFiles.size());
      TsFileMetricManager.getInstance()
          .addCompactionTempFileSize(!isCrossCompaction, !seqFiles.isEmpty(), -tempFileSize);
    }
  }

  private void compactAlignedSeries(
      String deviceId,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter fastCrossCompactionWriter)
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
    Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new HashMap<>();
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
      if (!entry.getKey().equals(TIME_COLUMN_ID)) {
        measurementSchemas.add(entry.getValue().left);
      }
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
      String deviceID,
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

    int subTaskNums = Math.min(allMeasurements.size(), subTaskNum);

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
        LOGGER.error("[Compaction] SubCompactionTask meet errors ", e);
        throw new IOException(e);
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
      throw new RuntimeException(
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
}
