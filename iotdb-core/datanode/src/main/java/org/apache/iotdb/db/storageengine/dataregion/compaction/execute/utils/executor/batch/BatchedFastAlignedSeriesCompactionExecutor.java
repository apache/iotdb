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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.AlignedSeriesBatchCompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.BatchCompactionPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactChunkPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.FirstBatchCompactionAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.FollowingBatchCompactionAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.FastAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.AlignedPageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.PageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.flushcontroller.FollowedBatchedCompactionFlushController;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchedFastAlignedSeriesCompactionExecutor
    extends FastAlignedSeriesCompactionExecutor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private AlignedSeriesBatchCompactionUtils.BatchColumnSelection batchColumnSelection;
  private final IMeasurementSchema timeSchema;
  private final List<IMeasurementSchema> valueMeasurementSchemas;
  private final List<TsFileResource> sortedSourceFiles;

  private final Map<TsFileResource, List<AlignedChunkMetadata>> alignedChunkMetadataCache;
  private final BatchCompactionPlan batchCompactionPlan;
  private final int batchSize =
      IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();

  public BatchedFastAlignedSeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<String, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
          modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      IDeviceID deviceId,
      int subTaskId,
      List<IMeasurementSchema> measurementSchemas,
      FastCompactionTaskSummary summary,
      boolean ignoreAllNullRows) {
    super(
        compactionWriter,
        timeseriesMetadataOffsetMap,
        readerCacheMap,
        modificationCacheMap,
        sortedSourceFiles,
        deviceId,
        subTaskId,
        measurementSchemas,
        summary,
        ignoreAllNullRows);
    timeSchema = measurementSchemas.remove(0);
    valueMeasurementSchemas = measurementSchemas;
    this.batchColumnSelection =
        new AlignedSeriesBatchCompactionUtils.BatchColumnSelection(
            valueMeasurementSchemas, batchSize);
    this.sortedSourceFiles = sortedSourceFiles;
    this.alignedChunkMetadataCache = new HashMap<>();
    this.batchCompactionPlan = new BatchCompactionPlan();
  }

  private List<AlignedChunkMetadata> getAlignedChunkMetadataListBySelectedValueColumn(
      TsFileResource tsFileResource, List<IMeasurementSchema> selectedValueMeasurementSchemas)
      throws IOException, IllegalPathException {
    // 1. get Full AlignedChunkMetadata from cache
    List<AlignedChunkMetadata> alignedChunkMetadataList = null;
    if (alignedChunkMetadataCache.containsKey(tsFileResource)) {
      alignedChunkMetadataList = alignedChunkMetadataCache.get(tsFileResource);
    } else {
      alignedChunkMetadataList = getAlignedChunkMetadataList(tsFileResource);
      AlignedSeriesBatchCompactionUtils.markAlignedChunkHasDeletion(alignedChunkMetadataList);
      alignedChunkMetadataCache.put(tsFileResource, alignedChunkMetadataList);
    }
    // 2. generate AlignedChunkMetadata list by selected value columns

    List<AlignedChunkMetadata> filteredAlignedChunkMetadataList = new ArrayList<>();
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
      filteredAlignedChunkMetadataList.add(
          AlignedSeriesBatchCompactionUtils.filterAlignedChunkMetadataByIndex(
              alignedChunkMetadata, batchColumnSelection.getSelectedColumnIndexList()));
    }
    return filteredAlignedChunkMetadataList;
  }

  @Override
  public void execute()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    compactFirstBatch();
    if (batchCompactionPlan.isEmpty()) {
      return;
    }
    compactLeftBatches();
  }

  private void compactFirstBatch()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    List<IMeasurementSchema> selectedMeasurementSchemas;
    if (!batchColumnSelection.hasNext()) {
      if (ignoreAllNullRows) {
        return;
      }
      selectedMeasurementSchemas = Collections.singletonList(timeSchema);
    } else {
      batchColumnSelection.next();
      selectedMeasurementSchemas =
          new ArrayList<>(batchColumnSelection.getCurrentSelectedColumnSchemaList().size() + 1);
      selectedMeasurementSchemas.add(timeSchema);
      selectedMeasurementSchemas.addAll(batchColumnSelection.getCurrentSelectedColumnSchemaList());
    }

    FirstBatchFastAlignedSeriesCompactionExecutor executor =
        new FirstBatchFastAlignedSeriesCompactionExecutor(
            compactionWriter,
            filterTimeseriesMetadataOffsetMap(selectedMeasurementSchemas),
            readerCacheMap,
            modificationCacheMap,
            sortedSourceFiles,
            deviceId,
            subTaskId,
            selectedMeasurementSchemas,
            summary,
            ignoreAllNullRows);
    executor.execute();
    LOGGER.debug(
        "[Batch Compaction] current device is {}, first batch compacted time chunk is {}",
        deviceId,
        batchCompactionPlan);
  }

  private void compactLeftBatches()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    while (batchColumnSelection.hasNext()) {
      batchColumnSelection.next();
      List<IMeasurementSchema> currentBatchMeasurementSchemas =
          new ArrayList<>(batchColumnSelection.getCurrentSelectedColumnSchemaList().size() + 1);
      currentBatchMeasurementSchemas.add(timeSchema);
      currentBatchMeasurementSchemas.addAll(
          batchColumnSelection.getCurrentSelectedColumnSchemaList());
      FollowingBatchFastAlignedSeriesCompactionExecutor executor =
          new FollowingBatchFastAlignedSeriesCompactionExecutor(
              compactionWriter,
              filterTimeseriesMetadataOffsetMap(currentBatchMeasurementSchemas),
              readerCacheMap,
              modificationCacheMap,
              sortedSourceFiles,
              deviceId,
              subTaskId,
              currentBatchMeasurementSchemas,
              summary,
              ignoreAllNullRows);
      executor.execute();
    }
  }

  private Map<String, Map<TsFileResource, Pair<Long, Long>>> filterTimeseriesMetadataOffsetMap(
      List<IMeasurementSchema> measurementSchemas) {
    Map<String, Map<TsFileResource, Pair<Long, Long>>> result = new HashMap<>();
    for (IMeasurementSchema measurementSchema : measurementSchemas) {
      String measurementId = measurementSchema.getMeasurementName();
      Map<TsFileResource, Pair<Long, Long>> entryValue =
          timeseriesMetadataOffsetMap.get(measurementSchema.getMeasurementName());
      result.put(measurementId, entryValue);
    }
    return result;
  }

  private class FirstBatchFastAlignedSeriesCompactionExecutor
      extends FastAlignedSeriesCompactionExecutor {

    public FirstBatchFastAlignedSeriesCompactionExecutor(
        AbstractCompactionWriter compactionWriter,
        Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
        Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
        Map<String, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
            modificationCacheMap,
        List<TsFileResource> sortedSourceFiles,
        IDeviceID deviceId,
        int subTaskId,
        List<IMeasurementSchema> measurementSchemas,
        FastCompactionTaskSummary summary,
        boolean ignoreAllNullRows) {
      super(
          compactionWriter,
          timeseriesMetadataOffsetMap,
          readerCacheMap,
          modificationCacheMap,
          sortedSourceFiles,
          deviceId,
          subTaskId,
          measurementSchemas,
          summary,
          ignoreAllNullRows);
      isBatchedCompaction = true;
    }

    @Override
    public void execute()
        throws PageException, IllegalPathException, IOException, WriteProcessException {
      FirstBatchCompactionAlignedChunkWriter firstBatchCompactionAlignedChunkWriter =
          new FirstBatchCompactionAlignedChunkWriter(
              this.measurementSchemas.remove(0), this.measurementSchemas);

      firstBatchCompactionAlignedChunkWriter.registerBeforeFlushChunkWriterCallback(
          chunkWriter -> {
            batchCompactionPlan.recordCompactedChunk(
                ((FirstBatchCompactionAlignedChunkWriter) chunkWriter).getCompactedChunkRecord());
          });

      compactionWriter.startMeasurement(
          TsFileConstant.TIME_COLUMN_ID, firstBatchCompactionAlignedChunkWriter, subTaskId);
      compactFiles();
      compactionWriter.endMeasurement(subTaskId);
    }

    @Override
    protected List<AlignedChunkMetadata> getAlignedChunkMetadataList(TsFileResource resource)
        throws IOException, IllegalPathException {
      return getAlignedChunkMetadataListBySelectedValueColumn(resource, measurementSchemas);
    }

    @Override
    protected Chunk readChunk(TsFileSequenceReader reader, ChunkMetadata chunkMetadata)
        throws IOException {
      Chunk chunk = super.readChunk(reader, chunkMetadata);
      if (AlignedSeriesBatchCompactionUtils.isTimeChunk(chunkMetadata)) {
        batchCompactionPlan.addTimeChunkToCache(
            reader.getFileName(), chunkMetadata.getOffsetOfChunkHeader(), chunk);
      }
      return chunk;
    }

    @Override
    protected boolean flushChunkToCompactionWriter(ChunkMetadataElement chunkMetadataElement)
        throws IOException {
      boolean success = super.flushChunkToCompactionWriter(chunkMetadataElement);
      if (success) {
        batchCompactionPlan.recordCompactedChunk(
            new CompactChunkPlan(
                chunkMetadataElement.chunkMetadata.getStartTime(),
                chunkMetadataElement.chunkMetadata.getEndTime()));
      }
      return success;
    }

    @Override
    protected ModifiedStatus isPageModified(PageElement pageElement) {
      AlignedPageElement alignedPageElement = (AlignedPageElement) pageElement;
      long startTime = alignedPageElement.getStartTime();
      long endTime = alignedPageElement.getEndTime();
      IChunkMetadata batchedAlignedChunkMetadata =
          alignedPageElement.getChunkMetadataElement().chunkMetadata;
      TsFileResource resource = alignedPageElement.getChunkMetadataElement().fileElement.resource;
      List<AlignedChunkMetadata> alignedChunkMetadataListOfFile =
          alignedChunkMetadataCache.get(resource);
      AlignedChunkMetadata originAlignedChunkMetadata = null;
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataListOfFile) {
        if (alignedChunkMetadata.getOffsetOfChunkHeader()
            == batchedAlignedChunkMetadata.getOffsetOfChunkHeader()) {
          originAlignedChunkMetadata = alignedChunkMetadata;
          break;
        }
      }

      ModifiedStatus modifiedStatus =
          AlignedSeriesBatchCompactionUtils.calculateAlignedPageModifiedStatus(
              startTime, endTime, originAlignedChunkMetadata, ignoreAllNullRows);
      batchCompactionPlan.recordPageModifiedStatus(
          resource.getTsFile().getName(), new TimeRange(startTime, endTime), modifiedStatus);
      return modifiedStatus;
    }
  }

  private class FollowingBatchFastAlignedSeriesCompactionExecutor
      extends FastAlignedSeriesCompactionExecutor {

    private FollowedBatchedCompactionFlushController flushController;

    public FollowingBatchFastAlignedSeriesCompactionExecutor(
        AbstractCompactionWriter compactionWriter,
        Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
        Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
        Map<String, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
            modificationCacheMap,
        List<TsFileResource> sortedSourceFiles,
        IDeviceID deviceId,
        int subTaskId,
        List<IMeasurementSchema> measurementSchemas,
        FastCompactionTaskSummary summary,
        boolean ignoreAllNullRows) {
      super(
          compactionWriter,
          timeseriesMetadataOffsetMap,
          readerCacheMap,
          modificationCacheMap,
          sortedSourceFiles,
          deviceId,
          subTaskId,
          measurementSchemas,
          summary,
          ignoreAllNullRows);
      isBatchedCompaction = true;
    }

    @Override
    public void execute()
        throws PageException, IllegalPathException, IOException, WriteProcessException {
      FollowingBatchCompactionAlignedChunkWriter followingBatchCompactionAlignedChunkWriter =
          new FollowingBatchCompactionAlignedChunkWriter(
              measurementSchemas.remove(0),
              measurementSchemas,
              batchCompactionPlan.getCompactChunkPlan(0));
      flushController =
          new FollowedBatchedCompactionFlushController(
              batchCompactionPlan, followingBatchCompactionAlignedChunkWriter);
      followingBatchCompactionAlignedChunkWriter.registerAfterFlushChunkWriterCallback(
          (chunkWriter) -> flushController.nextChunk());

      compactionWriter.startMeasurement(
          TsFileConstant.TIME_COLUMN_ID, followingBatchCompactionAlignedChunkWriter, subTaskId);
      compactFiles();
      compactionWriter.endMeasurement(subTaskId);
    }

    @Override
    protected ModifiedStatus isPageModified(PageElement pageElement) {
      String file =
          pageElement.getChunkMetadataElement().fileElement.resource.getTsFile().getName();
      long startTime = pageElement.getStartTime();
      long endTime = pageElement.getEndTime();
      return batchCompactionPlan.getAlignedPageModifiedStatus(
          file, new TimeRange(startTime, endTime));
    }

    @Override
    protected List<AlignedChunkMetadata> getAlignedChunkMetadataList(TsFileResource resource)
        throws IOException, IllegalPathException {
      return getAlignedChunkMetadataListBySelectedValueColumn(resource, measurementSchemas);
    }

    @Override
    protected Chunk readChunk(TsFileSequenceReader reader, ChunkMetadata chunkMetadata)
        throws IOException {
      if (AlignedSeriesBatchCompactionUtils.isTimeChunk(chunkMetadata)) {
        return batchCompactionPlan.getTimeChunkFromCache(reader, chunkMetadata);
      }
      return super.readChunk(reader, chunkMetadata);
    }

    @Override
    protected boolean flushChunkToCompactionWriter(ChunkMetadataElement chunkMetadataElement)
        throws IOException {
      boolean success =
          compactionWriter.flushBatchedValueChunk(chunkMetadataElement, subTaskId, flushController);
      if (success) {
        flushController.nextChunk();
      }
      return success;
    }

    @Override
    protected boolean flushPageToCompactionWriter(PageElement pageElement)
        throws PageException, IOException {
      AlignedPageElement alignedPageElement = (AlignedPageElement) pageElement;
      return compactionWriter.flushBatchedValuePage(alignedPageElement, subTaskId, flushController);
    }
  }
}
