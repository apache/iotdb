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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.ReadPointPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.IDataBlockReader;
import org.apache.iotdb.db.engine.compaction.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.ReadPointCrossCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.ReadPointInnerCompactionWriter;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ReadPointCompactionPerformer
    implements ICrossCompactionPerformer, IUnseqCompactionPerformer {
  private final Logger LOGGER = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private List<TsFileResource> seqFiles = Collections.emptyList();
  private List<TsFileResource> unseqFiles = Collections.emptyList();

  private List<TsFileResource> sortedSourceFiles;
  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  private CompactionTaskSummary summary;

  private List<TsFileResource> targetFiles = Collections.emptyList();

  public ReadPointCompactionPerformer(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      List<TsFileResource> targetFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
    this.targetFiles = targetFiles;
  }

  public ReadPointCompactionPerformer(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  public ReadPointCompactionPerformer() {}

  @Override
  public void perform() throws Exception {
    long queryId = QueryResourceManager.getInstance().assignCompactionQueryId();
    FragmentInstanceContext fragmentInstanceContext =
        FragmentInstanceContext.createFragmentInstanceContextForCompaction(queryId);
    QueryDataSource queryDataSource = new QueryDataSource(seqFiles, unseqFiles);
    QueryResourceManager.getInstance()
        .getQueryFileManager()
        .addUsedFilesForQuery(queryId, queryDataSource);
    sortedSourceFiles = CompactionUtils.sortSourceFiles(seqFiles, unseqFiles);
    try (AbstractCompactionWriter compactionWriter =
        getCompactionWriter(seqFiles, unseqFiles, targetFiles)) {
      // Do not close device iterator, because tsfile reader is managed by FileReaderManager.
      MultiTsFileDeviceIterator deviceIterator =
          new MultiTsFileDeviceIterator(seqFiles, unseqFiles);
      while (deviceIterator.hasNextDevice()) {
        checkThreadInterrupted();
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;
        QueryUtils.fillOrderIndexes(queryDataSource, device, true);

        if (isAligned) {
          compactAlignedSeries(
              device, deviceIterator, compactionWriter, fragmentInstanceContext, queryDataSource);
        } else {
          compactNonAlignedSeries(
              device, deviceIterator, compactionWriter, fragmentInstanceContext, queryDataSource);
        }
      }

      compactionWriter.endFile();
      CompactionUtils.updatePlanIndexes(targetFiles, seqFiles, unseqFiles);

    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
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

  private void compactAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      FragmentInstanceContext fragmentInstanceContext,
      QueryDataSource queryDataSource)
      throws IOException, MetadataException {
    Map<String, MeasurementSchema> schemaMap =
        deviceIterator.getAllAlignedSchemasAndMetadatasOfCurrentDevice().right;
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(schemaMap.values());
    if (measurementSchemas.isEmpty()) {
      return;
    }
    List<String> existedMeasurements =
        measurementSchemas.stream()
            .map(IMeasurementSchema::getMeasurementId)
            .collect(Collectors.toList());
    IDataBlockReader dataBlockReader =
        constructReader(
            device,
            existedMeasurements,
            measurementSchemas,
            new ArrayList<>(schemaMap.keySet()),
            fragmentInstanceContext,
            queryDataSource,
            true);

    if (dataBlockReader.hasNextBatch()) {
      // chunkgroup is serialized only when at least one timeseries under this device has data
      compactionWriter.startChunkGroup(device, true);
      compactionWriter.startMeasurement(measurementSchemas, 0);
      writeWithReader(compactionWriter, dataBlockReader, device, 0, true);
      compactionWriter.endMeasurement(0);
      compactionWriter.endChunkGroup();
      compactionWriter.checkAndMayFlushChunkMetadata();
    }
  }

  private void compactNonAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      FragmentInstanceContext fragmentInstanceContext,
      QueryDataSource queryDataSource)
      throws IOException, InterruptedException, ExecutionException {
    Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();
    List<String> allMeasurements = new ArrayList<>(schemaMap.keySet());
    allMeasurements.sort((String::compareTo));
    int subTaskNums = Math.min(allMeasurements.size(), subTaskNum);
    // construct sub tasks and start compacting measurements in parallel
    compactionWriter.startChunkGroup(device, false);
    for (int taskCount = 0; taskCount < allMeasurements.size(); ) {
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < subTaskNums && taskCount < allMeasurements.size(); i++) {
        futures.add(
            CompactionTaskManager.getInstance()
                .submitSubTask(
                    new ReadPointPerformerSubTask(
                        device,
                        Collections.singletonList(allMeasurements.get(taskCount++)),
                        fragmentInstanceContext,
                        queryDataSource,
                        compactionWriter,
                        schemaMap,
                        i)));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
      // sync all the subtask, and check the writer chunk metadata size
      compactionWriter.checkAndMayFlushChunkMetadata();
    }

    compactionWriter.endChunkGroup();
  }

  /**
   * @param measurementIds if device is aligned, then measurementIds contain all measurements. If
   *     device is not aligned, then measurementIds only contain one measurement.
   */
  public static IDataBlockReader constructReader(
      String deviceId,
      List<String> measurementIds,
      List<IMeasurementSchema> measurementSchemas,
      List<String> allSensors,
      FragmentInstanceContext fragmentInstanceContext,
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
    return new SeriesDataBlockReader(
        seriesPath,
        new HashSet<>(allSensors),
        tsDataType,
        fragmentInstanceContext,
        queryDataSource,
        true);
  }

  public static void writeWithReader(
      AbstractCompactionWriter writer,
      IDataBlockReader reader,
      String device,
      int subTaskId,
      boolean isAligned)
      throws IOException {
    while (reader.hasNextBatch()) {
      TsBlock tsBlock = reader.nextBatch();
      if (isAligned) {
        writer.write(
            tsBlock.getTimeColumn(),
            tsBlock.getValueColumns(),
            subTaskId,
            tsBlock.getPositionCount());
      } else {
        IPointReader pointReader = tsBlock.getTsBlockSingleColumnIterator();
        while (pointReader.hasNextTimeValuePair()) {
          TimeValuePair timeValuePair = pointReader.nextTimeValuePair();
          writer.write(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getValue(), subTaskId);
          writer.updateStartTimeAndEndTime(device, timeValuePair.getTimestamp(), subTaskId);
        }
      }
    }
  }

  private AbstractCompactionWriter getCompactionWriter(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException {
    if (!seqFileResources.isEmpty() && !unseqFileResources.isEmpty()) {
      // cross space
      return new ReadPointCrossCompactionWriter(targetFileResources, seqFileResources);
    } else {
      // inner space
      return new ReadPointInnerCompactionWriter(targetFileResources.get(0));
    }
  }

  private void checkThreadInterrupted() throws InterruptedException {
    if (Thread.interrupted() || summary.isCancel()) {
      throw new InterruptedException(
          String.format(
              "[Compaction] compaction for target file %s abort", targetFiles.toString()));
    }
  }

  @Override
  public void setSourceFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> unseqFiles) {
    this.unseqFiles = unseqFiles;
  }
}
