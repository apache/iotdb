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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.ReadPointPerformerSubTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionTableSchemaCollector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.ReadPointCrossCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.ReadPointInnerCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.QueryResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ReadPointCompactionPerformer
    implements ICrossCompactionPerformer, IUnseqCompactionPerformer {
  @SuppressWarnings("squid:S1068")
  private final Logger logger = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  protected List<TsFileResource> seqFiles = Collections.emptyList();
  protected List<TsFileResource> unseqFiles = Collections.emptyList();

  private static final int SUB_TASK_NUM =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  private CompactionTaskSummary summary;

  protected List<TsFileResource> targetFiles = Collections.emptyList();

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

  @SuppressWarnings("squid:S2095") // Do not close device iterator
  @Override
  public void perform() throws Exception {
    long queryId = QueryResourceManager.getInstance().assignCompactionQueryId();
    FragmentInstanceContext fragmentInstanceContext =
        FragmentInstanceContext.createFragmentInstanceContextForCompaction(queryId);
    QueryDataSource queryDataSource = new QueryDataSource(seqFiles, unseqFiles);
    QueryResourceManager.getInstance()
        .getQueryFileManager()
        .addUsedFilesForQuery(queryId, queryDataSource);
    summary.setTemporalFileNum(targetFiles.size());
    try (AbstractCompactionWriter compactionWriter =
        getCompactionWriter(seqFiles, unseqFiles, targetFiles)) {
      // Do not close device iterator, because tsfile reader is managed by FileReaderManager.
      MultiTsFileDeviceIterator deviceIterator =
          new MultiTsFileDeviceIterator(seqFiles, unseqFiles);
      List<Schema> schemas =
          CompactionTableSchemaCollector.collectSchema(
              seqFiles, unseqFiles, deviceIterator.getReaderMap());
      compactionWriter.setSchemaForAllTargetFile(schemas);
      while (deviceIterator.hasNextDevice()) {
        checkThreadInterrupted();
        Pair<IDeviceID, Boolean> deviceInfo = deviceIterator.nextDevice();
        IDeviceID device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;
        queryDataSource.fillOrderIndexes(device, true);

        if (isAligned) {
          compactAlignedSeries(
              device, deviceIterator, compactionWriter, fragmentInstanceContext, queryDataSource);
        } else {
          compactNonAlignedSeries(
              device, deviceIterator, compactionWriter, fragmentInstanceContext, queryDataSource);
        }
        summary.setTemporaryFileSize(compactionWriter.getWriterSize());
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
      IDeviceID device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      FragmentInstanceContext fragmentInstanceContext,
      QueryDataSource queryDataSource)
      throws IOException, MetadataException {
    Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();
    IMeasurementSchema timeSchema = schemaMap.remove(TsFileConstant.TIME_COLUMN_ID);
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(schemaMap.values());
    if (measurementSchemas.isEmpty()) {
      return;
    }
    List<String> existedMeasurements =
        measurementSchemas.stream()
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toList());

    fragmentInstanceContext.setIgnoreAllNullRows(device.getTableName().startsWith("root."));
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
      measurementSchemas.add(0, timeSchema);
      compactionWriter.startMeasurement(
          TsFileConstant.TIME_COLUMN_ID,
          new AlignedChunkWriterImpl(measurementSchemas.remove(0), measurementSchemas),
          0);
      writeWithReader(compactionWriter, dataBlockReader, device, 0, true);
      compactionWriter.endMeasurement(0);
      compactionWriter.endChunkGroup();
      // check whether to flush chunk metadata or not
      compactionWriter.checkAndMayFlushChunkMetadata();
    }
  }

  private void compactNonAlignedSeries(
      IDeviceID device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      FragmentInstanceContext fragmentInstanceContext,
      QueryDataSource queryDataSource)
      throws IOException, InterruptedException, ExecutionException {
    Map<String, MeasurementSchema> schemaMap = deviceIterator.getAllSchemasOfCurrentDevice();
    List<String> allMeasurements = new ArrayList<>(schemaMap.keySet());
    allMeasurements.sort((String::compareTo));
    int subTaskNums = Math.min(allMeasurements.size(), SUB_TASK_NUM);
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

      compactionWriter.startChunkGroup(device, false);
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < subTaskNums; ++i) {
        futures.add(
            CompactionTaskManager.getInstance()
                .submitSubTask(
                    new ReadPointPerformerSubTask(
                        device,
                        measurementListArray[i],
                        fragmentInstanceContext,
                        new QueryDataSource(queryDataSource),
                        compactionWriter,
                        schemaMap,
                        i)));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
      compactionWriter.endChunkGroup();
      // check whether to flush chunk metadata or not
      compactionWriter.checkAndMayFlushChunkMetadata();
    }
  }

  /**
   * Construct series data block reader.
   *
   * @param measurementIds if device is aligned, then measurementIds contain all measurements. If
   *     device is not aligned, then measurementIds only contain one measurement.
   */
  public static IDataBlockReader constructReader(
      IDeviceID deviceId,
      List<String> measurementIds,
      List<IMeasurementSchema> measurementSchemas,
      List<String> allSensors,
      FragmentInstanceContext fragmentInstanceContext,
      QueryDataSource queryDataSource,
      boolean isAlign) {
    IFullPath seriesPath;
    if (isAlign) {
      seriesPath = new AlignedFullPath(deviceId, measurementIds, measurementSchemas);
    } else {
      seriesPath = new NonAlignedFullPath(deviceId, measurementSchemas.get(0));
    }
    return new SeriesDataBlockReader(
        seriesPath, new HashSet<>(allSensors), fragmentInstanceContext, queryDataSource, true);
  }

  @SuppressWarnings("squid:S1172")
  public static void writeWithReader(
      AbstractCompactionWriter writer,
      IDataBlockReader reader,
      IDeviceID device,
      int subTaskId,
      boolean isAligned)
      throws IOException {
    while (reader.hasNextBatch()) {
      TsBlock tsBlock = reader.nextBatch();
      if (isAligned) {
        writer.write(tsBlock, subTaskId);
      } else {
        IPointReader pointReader = tsBlock.getTsBlockSingleColumnIterator();
        while (pointReader.hasNextTimeValuePair()) {
          writer.write(pointReader.nextTimeValuePair(), subTaskId);
        }
      }
    }
  }

  protected AbstractCompactionWriter getCompactionWriter(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException {
    if (!seqFileResources.isEmpty() && !unseqFileResources.isEmpty()) {
      // cross space
      return new ReadPointCrossCompactionWriter(targetFileResources, seqFileResources);
    } else {
      // inner space
      return new ReadPointInnerCompactionWriter(targetFileResources);
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
