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
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.CrossSpaceCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CompactionUtils {
  private static final Logger logger = LoggerFactory.getLogger("CompactionUtils");

  public static void compact(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources,
      String fullStorageGroupName)
      throws IOException, MetadataException, StorageEngineException {
    long queryId =
        QueryResourceManager.getInstance()
            .assignCompactionQueryId(1); // Todo:Thread.currentThread().getName()
    QueryContext queryContext = new QueryContext(queryId);
    QueryDataSource queryDataSource = new QueryDataSource(seqFileResources, unseqFileResources);
    QueryResourceManager.getInstance()
        .getQueryFileManager()
        .addUsedFilesForQuery(queryId, queryDataSource);

    List<TsFileResource> allResources = new ArrayList<>();
    allResources.addAll(seqFileResources);
    allResources.addAll(unseqFileResources);
    try (AbstractCompactionWriter compactionWriter =
            getCompactionWriter(seqFileResources, unseqFileResources, targetFileResources);
        MultiTsFileDeviceIterator deviceIterator = new MultiTsFileDeviceIterator(allResources)) {
      Set<String> tsFileDevicesSet = deviceIterator.getDevices();

      for (String device : tsFileDevicesSet) {
        // calculate the read order of unseqResources
        QueryUtils.fillOrderIndexes(queryDataSource, device, true);
        // Todo: use iterator to get measurements
        boolean isAligned =
            ((EntityMNode) MManager.getInstance().getDeviceNode(new PartialPath(device)))
                .isAligned();
        Map<String, TimeseriesMetadata> deviceMeasurementsMap =
            getDeviceMeasurementsMap(device, seqFileResources, unseqFileResources);

        List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
        if (isAligned) {
          //  aligned
          deviceMeasurementsMap.remove("");
          deviceMeasurementsMap.forEach(
              (measurementId, timeseriesMetadata) -> {
                measurementSchemas.add(
                    new MeasurementSchema(measurementId, timeseriesMetadata.getTSDataType()));
              });

          IBatchReader dataBatchReader =
              constructReader(
                  device,
                  new ArrayList<>(deviceMeasurementsMap.keySet()),
                  measurementSchemas,
                  deviceMeasurementsMap.keySet(),
                  queryContext,
                  queryDataSource,
                  isAligned);

          if (dataBatchReader.hasNextBatch()) {
            // chunkgroup is serialized only when at least one timeseries under this device has data
            compactionWriter.startChunkGroup(device, isAligned);
            compactionWriter.startMeasurement(measurementSchemas);
            writeWithReader(compactionWriter, dataBatchReader);
            compactionWriter.endMeasurement();
            compactionWriter.endChunkGroup();
          }
        } else {
          //  nonAligned
          boolean hasStartChunkGroup = false;
          for (Map.Entry<String, TimeseriesMetadata> entry : deviceMeasurementsMap.entrySet()) {
            measurementSchemas.clear();
            measurementSchemas.add(
                IoTDB.metaManager.getSeriesSchema(new PartialPath(device, entry.getKey())));

            IBatchReader dataBatchReader =
                constructReader(
                    device,
                    Collections.singletonList(entry.getKey()),
                    measurementSchemas,
                    deviceMeasurementsMap.keySet(),
                    queryContext,
                    queryDataSource,
                    isAligned);

            if (dataBatchReader.hasNextBatch()) {
              if (!hasStartChunkGroup) {
                // chunkgroup is serialized only when at least one timeseries under this device has
                // data
                compactionWriter.startChunkGroup(device, isAligned);
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
      }
      compactionWriter.endFile();
      updateDeviceStartTimeAndEndTime(targetFileResources, compactionWriter);
      updatePlanIndexes(targetFileResources, seqFileResources, unseqFileResources);
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
    }
  }

  private static void writeWithReader(AbstractCompactionWriter writer, IBatchReader reader)
      throws IOException {
    while (reader.hasNextBatch()) {
      BatchData batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        writer.write(batchData.currentTime(), batchData.currentValue());
        batchData.next();
      }
    }
  }

  private static IBatchReader constructReader(
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

  private static Map<String, TimeseriesMetadata> getDeviceMeasurementsMap(
      String device, List<TsFileResource> seqFileResources, List<TsFileResource> unseqFileResources)
      throws IOException {
    Map<String, TimeseriesMetadata> deviceMeasurementsMap = new ConcurrentHashMap<>();
    for (TsFileResource seqResource : seqFileResources) {
      deviceMeasurementsMap.putAll(
          FileReaderManager.getInstance()
              .get(seqResource.getTsFilePath(), true)
              .readDeviceMetadata(device));
    }
    for (TsFileResource unseqResource : unseqFileResources) {
      deviceMeasurementsMap.putAll(
          FileReaderManager.getInstance()
              .get(unseqResource.getTsFilePath(), true)
              .readDeviceMetadata(device));
    }
    return deviceMeasurementsMap;
  }

  private static void updateDeviceStartTimeAndEndTime(
      List<TsFileResource> targetResources, AbstractCompactionWriter compactionWriter) {
    List<TsFileIOWriter> fileIOWriterList = new ArrayList<>();
    if (compactionWriter instanceof InnerSpaceCompactionWriter) {
      fileIOWriterList.add(((InnerSpaceCompactionWriter) compactionWriter).getFileWriter());
    } else {
      fileIOWriterList.addAll(((CrossSpaceCompactionWriter) compactionWriter).getFileWriters());
    }
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource targetResource = targetResources.get(i);
      Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
          fileIOWriterList.get(i).getDeviceTimeseriesMetadataMap();
      for (Map.Entry<String, List<TimeseriesMetadata>> entry :
          deviceTimeseriesMetadataMap.entrySet()) {
        for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
          targetResource.updateStartTime(
              entry.getKey(), timeseriesMetadata.getStatistics().getStartTime());
          targetResource.updateEndTime(
              entry.getKey(), timeseriesMetadata.getStatistics().getEndTime());
        }
      }
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
    for (TsFileResource targetResource : targetResources) {
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
   *
   * @param targetResources
   * @param isInnerSpace
   * @param fullStorageGroupName
   * @throws IOException
   */
  public static void moveToTargetFile(
      List<TsFileResource> targetResources, boolean isInnerSpace, String fullStorageGroupName)
      throws IOException {
    if (isInnerSpace) {
      TsFileResource targetResource = targetResources.get(0);
      if (!targetResource
          .getTsFilePath()
          .endsWith(IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)) {
        logger.warn(
            "{} [Compaction] Tmp target tsfile {} should be end with {}",
            fullStorageGroupName,
            targetResource.getTsFilePath(),
            IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX);
        return;
      }
      moveOneTargetFile(targetResource, IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX);
    } else {
      for (TsFileResource targetResource : targetResources) {
        if (!targetResource
            .getTsFilePath()
            .endsWith(IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX)) {
          logger.warn(
              "{} [Compaction] Tmp target tsfile {} should be end with {}",
              fullStorageGroupName,
              targetResource.getTsFilePath(),
              IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX);
          return;
        }
      }
      for (TsFileResource targetResource : targetResources) {
        moveOneTargetFile(targetResource, IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX);
      }
    }
  }

  private static void moveOneTargetFile(TsFileResource targetResource, String tmpFileSuffix)
      throws IOException {
    // move to target file and delete old tmp target file
    File newFile =
        new File(
            targetResource.getTsFilePath().replace(tmpFileSuffix, TsFileConstant.TSFILE_SUFFIX));
    FSFactoryProducer.getFSFactory().moveFile(targetResource.getTsFile(), newFile);

    // serialize xxx.tsfile.resource
    targetResource.setFile(newFile);
    targetResource.serialize();
    targetResource.close();
  }
}
