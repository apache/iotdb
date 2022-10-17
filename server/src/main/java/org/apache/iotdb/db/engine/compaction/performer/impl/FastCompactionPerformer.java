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
package org.apache.iotdb.db.engine.compaction.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.AlignedFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.NonAlignedFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.writer.FastCrossCompactionWriter;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FastCompactionPerformer implements ICrossCompactionPerformer {
  private final Logger LOGGER = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private List<TsFileResource> seqFiles;

  private List<TsFileResource> unseqFiles;

  private List<TsFileResource> sortedSourceFiles = new ArrayList<>();

  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  public Map<TsFileResource, TsFileSequenceReader> readerCacheMap = new ConcurrentHashMap<>();

  private CompactionTaskSummary summary;

  private List<TsFileResource> targetFiles;

  public Map<TsFileResource, List<Modification>> modificationCache = new ConcurrentHashMap<>();

  public FastCompactionPerformer(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      List<TsFileResource> targetFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
    this.targetFiles = targetFiles;
  }

  public FastCompactionPerformer() {}

  @Override
  public void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException {
    try (MultiTsFileDeviceIterator deviceIterator =
            new MultiTsFileDeviceIterator(seqFiles, unseqFiles, readerCacheMap);
        FastCrossCompactionWriter compactionWriter =
            new FastCrossCompactionWriter(targetFiles, seqFiles)) {
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
        // update resource of the current device and check whether to flush chunk metadata or not
        compactionWriter.checkAndMayFlushChunkMetadata();
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
    }
  }

  private void compactAlignedSeries(
      String deviceId,
      MultiTsFileDeviceIterator deviceIterator,
      FastCrossCompactionWriter fastCrossCompactionWriter)
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
      if (!entry.getKey().equals("")) {
        measurementSchemas.add(entry.getValue().left);
      }
      timeseriesMetadataOffsetMap.put(entry.getKey(), entry.getValue().right);
    }

    new AlignedFastCompactionPerformerSubTask(
            fastCrossCompactionWriter,
            timeseriesMetadataOffsetMap,
            measurementSchemas,
            readerCacheMap,
            modificationCache,
            sortedSourceFiles,
            deviceId,
            0)
        .call();
  }

  private void compactNonAlignedSeries(
      String deviceID,
      MultiTsFileDeviceIterator deviceIterator,
      FastCrossCompactionWriter fastCrossCompactionWriter)
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
    for (int i = 0; i < subTaskNums; i++) {
      futures.add(
          CompactionTaskManager.getInstance()
              .submitSubTask(
                  new NonAlignedFastCompactionPerformerSubTask(
                      fastCrossCompactionWriter,
                      timeseriesMetadataOffsetMap,
                      measurementsForEachSubTask[i],
                      readerCacheMap,
                      modificationCache,
                      sortedSourceFiles,
                      deviceID,
                      i)));
    }

    // wait for all sub tasks finish
    for (int i = 0; i < subTaskNums; i++) {
      try {
        futures.get(i).get();
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
    this.summary = summary;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  private void checkThreadInterrupted() throws InterruptedException {
    if (Thread.interrupted() || summary.isCancel()) {
      throw new InterruptedException(
          String.format(
              "[Compaction] compaction for target file %s abort", targetFiles.toString()));
    }
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }
}
