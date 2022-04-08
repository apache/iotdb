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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.CrossSpaceCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ReadPointCompactionPerformer
    implements ICrossCompactionPerformer, IUnseqCompactionPerformer {
  private Logger LOGGER = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private List<TsFileResource> seqFiles = Collections.emptyList();
  private List<TsFileResource> unseqFiles = Collections.emptyList();

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
  public void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException {
    long queryId = QueryResourceManager.getInstance().assignCompactionQueryId();
    QueryContext queryContext = new QueryContext(queryId);
    QueryDataSource queryDataSource = new QueryDataSource(seqFiles, unseqFiles);
    QueryResourceManager.getInstance()
        .getQueryFileManager()
        .addUsedFilesForQuery(queryId, queryDataSource);

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
              device, deviceIterator, compactionWriter, queryContext, queryDataSource);
        } else {
          compactNonAlignedSeries(
              device, deviceIterator, compactionWriter, queryContext, queryDataSource);
        }
      }

      compactionWriter.endFile();
      updatePlanIndexes(targetFiles, seqFiles, unseqFiles);
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
    }
  }

  @Override
  public void setTargetFiles(List<TsFileResource> targetFiles) {
    this.targetFiles = targetFiles;
  }

  private void compactAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      QueryContext queryContext,
      QueryDataSource queryDataSource)
      throws IOException, MetadataException {
    MultiTsFileDeviceIterator.AlignedMeasurementIterator alignedMeasurementIterator =
        deviceIterator.iterateAlignedSeries(device);
    Set<String> allMeasurements = alignedMeasurementIterator.getAllMeasurements();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurement : allMeasurements) {
      try {
        if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
          measurementSchemas.add(IDTableManager.getInstance().getSeriesSchema(device, measurement));
        } else {
          measurementSchemas.add(
              IoTDB.schemaProcessor.getSeriesSchema(new PartialPath(device, measurement)));
        }
      } catch (PathNotExistException e) {
        LOGGER.info("A deleted path is skipped: {}", e.getMessage());
      }
    }
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
            allMeasurements,
            queryContext,
            queryDataSource,
            true);

    if (dataBatchReader.hasNextBatch()) {
      // chunkgroup is serialized only when at least one timeseries under this device has data
      compactionWriter.startChunkGroup(device, true);
      compactionWriter.startMeasurement(measurementSchemas);
      writeWithReader(compactionWriter, dataBatchReader);
      compactionWriter.endMeasurement();
      compactionWriter.endChunkGroup();
    }
  }

  private void compactNonAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      QueryContext queryContext,
      QueryDataSource queryDataSource)
      throws MetadataException, IOException {
    boolean hasStartChunkGroup = false;
    MultiTsFileDeviceIterator.MeasurementIterator measurementIterator =
        deviceIterator.iterateNotAlignedSeries(device, false);
    Set<String> allMeasurements = measurementIterator.getAllMeasurements();
    for (String measurement : allMeasurements) {
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      try {
        if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
          measurementSchemas.add(IDTableManager.getInstance().getSeriesSchema(device, measurement));
        } else {
          measurementSchemas.add(
              IoTDB.schemaProcessor.getSeriesSchema(new PartialPath(device, measurement)));
        }
      } catch (PathNotExistException e) {
        LOGGER.info("A deleted path is skipped: {}", e.getMessage());
        continue;
      }

      IBatchReader dataBatchReader =
          constructReader(
              device,
              Collections.singletonList(measurement),
              measurementSchemas,
              allMeasurements,
              queryContext,
              queryDataSource,
              false);

      if (dataBatchReader.hasNextBatch()) {
        if (!hasStartChunkGroup) {
          // chunkgroup is serialized only when at least one timeseries under this device has
          // data
          compactionWriter.startChunkGroup(device, false);
          hasStartChunkGroup = true;
        }
        compactionWriter.startMeasurement(measurementSchemas);
        writeWithReader(compactionWriter, dataBatchReader);
        compactionWriter.endMeasurement();
      }
    }

    if (hasStartChunkGroup) {
      compactionWriter.endChunkGroup();
    }
  }

  /**
   * @param measurementIds if device is aligned, then measurementIds contain all measurements. If
   *     device is not aligned, then measurementIds only contain one measurement.
   */
  private IBatchReader constructReader(
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

  private void writeWithReader(AbstractCompactionWriter writer, IBatchReader reader)
      throws IOException {
    while (reader.hasNextBatch()) {
      BatchData batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        writer.write(batchData.currentTime(), batchData.currentValue());
        batchData.next();
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
      // remove the target file been deleted from list
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

  private void checkThreadInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
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
