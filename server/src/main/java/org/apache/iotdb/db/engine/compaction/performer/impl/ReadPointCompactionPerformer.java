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
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.ReadPointPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.IDataBlockReader;
import org.apache.iotdb.db.engine.compaction.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.CrossSpaceCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ReadPointCompactionPerformer
    implements ICrossCompactionPerformer, IUnseqCompactionPerformer {
  private Logger LOGGER = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private List<TsFileResource> seqFiles = Collections.emptyList();
  private List<TsFileResource> unseqFiles = Collections.emptyList();
  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();
  private Map<TsFileResource, TsFileSequenceReader> readerCacheMap = new HashMap<>();
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
  public void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException,
          ExecutionException {
    long queryId = QueryResourceManager.getInstance().assignCompactionQueryId();
    FragmentInstanceContext fragmentInstanceContext =
        FragmentInstanceContext.createFragmentInstanceContextForCompaction(queryId);
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
              device, deviceIterator, compactionWriter, fragmentInstanceContext, queryDataSource);
        } else {
          compactNonAlignedSeries(
              device, deviceIterator, compactionWriter, fragmentInstanceContext, queryDataSource);
        }
      }

      compactionWriter.endFile();
      updatePlanIndexes(targetFiles, seqFiles, unseqFiles);
    } finally {
      clearReaderCache();
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
    MultiTsFileDeviceIterator.AlignedMeasurementIterator alignedMeasurementIterator =
        deviceIterator.iterateAlignedSeries(device);
    List<String> allMeasurements =
        new LinkedList<>(alignedMeasurementIterator.getAllMeasurements());
    Map<String, MeasurementSchema> schemaMap = getMeasurementSchema(device, allMeasurements);
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
            allMeasurements,
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
      throws IOException, InterruptedException, IllegalPathException, ExecutionException {
    MultiTsFileDeviceIterator.MeasurementIterator measurementIterator =
        deviceIterator.iterateNotAlignedSeries(device, false);
    List<String> allMeasurements = new ArrayList<>(measurementIterator.getAllMeasurements());
    allMeasurements.sort((String::compareTo));
    int subTaskNums = Math.min(allMeasurements.size(), subTaskNum);
    Map<String, MeasurementSchema> schemaMap = getMeasurementSchema(device, allMeasurements);

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

  private Map<String, MeasurementSchema> getMeasurementSchema(
      String device, List<String> measurements) throws IllegalPathException, IOException {
    HashMap<String, MeasurementSchema> schemaMap = new HashMap<>();
    List<TsFileResource> allResources = new LinkedList<>(seqFiles);
    allResources.addAll(unseqFiles);
    // sort the tsfile by version, so that we can iterate the tsfile from the newest to oldest
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
    for (String measurement : measurements) {
      for (TsFileResource tsFileResource : allResources) {
        if (!tsFileResource.mayContainsDevice(device)) {
          continue;
        }
        MeasurementSchema schema =
            getMeasurementSchemaFromReader(
                tsFileResource,
                readerCacheMap.computeIfAbsent(
                    tsFileResource,
                    x -> {
                      try {
                        FileReaderManager.getInstance().increaseFileReaderReference(x, true);
                        return FileReaderManager.getInstance().get(x.getTsFilePath(), true);
                      } catch (IOException e) {
                        throw new RuntimeException(
                            String.format(
                                "Failed to construct sequence reader for %s", tsFileResource));
                      }
                    }),
                device,
                measurement);
        if (schema != null) {
          schemaMap.put(measurement, schema);
          break;
        }
      }
    }
    return schemaMap;
  }

  private MeasurementSchema getMeasurementSchemaFromReader(
      TsFileResource resource, TsFileSequenceReader reader, String device, String measurement)
      throws IllegalPathException, IOException {
    List<ChunkMetadata> chunkMetadata =
        reader.getChunkMetadataList(new PartialPath(device, measurement), true);
    if (chunkMetadata.size() > 0) {
      chunkMetadata.get(0).setFilePath(resource.getTsFilePath());
      Chunk chunk = ChunkCache.getInstance().get(chunkMetadata.get(0));
      ChunkHeader header = chunk.getHeader();
      return new MeasurementSchema(
          measurement, header.getDataType(), header.getEncodingType(), header.getCompressionType());
    }
    return null;
  }

  private void clearReaderCache() throws IOException {
    for (TsFileResource resource : readerCacheMap.keySet()) {
      FileReaderManager.getInstance().decreaseFileReaderReference(resource, true);
    }
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
            device,
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
