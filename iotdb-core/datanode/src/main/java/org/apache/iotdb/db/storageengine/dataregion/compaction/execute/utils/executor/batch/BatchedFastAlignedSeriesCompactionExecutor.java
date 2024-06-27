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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.FastAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.AlignedPageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.PageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BatchedFastAlignedSeriesCompactionExecutor
    extends FastAlignedSeriesCompactionExecutor {

  private final Set<String> compactedMeasurements;
  private final IMeasurementSchema timeSchema;
  private final List<IMeasurementSchema> valueMeasurementSchemas;
  private final List<TsFileResource> sortedSourceFiles;

  private final Map<TsFileResource, List<AlignedChunkMetadata>> alignedChunkMetadataCache;

  public BatchedFastAlignedSeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<String, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>>
          modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      IDeviceID deviceId,
      int subTaskId,
      List<IMeasurementSchema> measurementSchemas,
      FastCompactionTaskSummary summary) {
    super(
        compactionWriter,
        timeseriesMetadataOffsetMap,
        readerCacheMap,
        modificationCacheMap,
        sortedSourceFiles,
        deviceId,
        subTaskId,
        measurementSchemas,
        summary);
    timeSchema = measurementSchemas.remove(0);
    valueMeasurementSchemas = measurementSchemas;
    this.compactedMeasurements = new HashSet<>();
    this.sortedSourceFiles = sortedSourceFiles;
    this.alignedChunkMetadataCache = new HashMap<>();
  }

  private List<AlignedChunkMetadata> getAlignedChunkMetadataListBySelectedValueColumn(
      TsFileResource tsFileResource, List<IMeasurementSchema> selectedValueMeasurementSchemas)
      throws IOException {
    // 1. get Full AlignedChunkMetadata from cache
    List<AlignedChunkMetadata> alignedChunkMetadataList = null;
    if (alignedChunkMetadataCache.containsKey(tsFileResource)) {
      alignedChunkMetadataList = alignedChunkMetadataCache.get(tsFileResource);
    } else {
      alignedChunkMetadataList = getAlignedChunkMetadataList(tsFileResource);
      AlignedSeriesGroupCompactionUtils.markAlignedChunkHasDeletion(alignedChunkMetadataList);
      alignedChunkMetadataCache.put(tsFileResource, alignedChunkMetadataList);
    }
    // 2. generate AlignedChunkMetadata list by selected value columns

    List<AlignedChunkMetadata> filteredAlignedChunkMetadataList = new ArrayList<>();
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
      List<String> selectedMeasurements =
          selectedValueMeasurementSchemas.stream()
              .map(IMeasurementSchema::getMeasurementId)
              .collect(Collectors.toList());
      filteredAlignedChunkMetadataList.add(
          AlignedSeriesGroupCompactionUtils.filterAlignedChunkMetadata(
              alignedChunkMetadata, selectedMeasurements));
    }
    return filteredAlignedChunkMetadataList;
  }

  @Override
  public void execute()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    List<CompactChunkPlan> compactChunkPlans = compactFirstBatch();
    compactLeftBatches(compactChunkPlans);
  }

  private List<CompactChunkPlan> compactFirstBatch()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    List<IMeasurementSchema> firstGroupMeasurements =
        AlignedSeriesGroupCompactionUtils.selectColumnGroupToCompact(
            valueMeasurementSchemas, compactedMeasurements);
    List<IMeasurementSchema> currentBatchMeasurementSchemas =
        new ArrayList<>(firstGroupMeasurements.size() + 1);
    currentBatchMeasurementSchemas.add(timeSchema);
    currentBatchMeasurementSchemas.addAll(firstGroupMeasurements);

    FirstBatchFastAlignedSeriesCompactionExecutor executor =
        new FirstBatchFastAlignedSeriesCompactionExecutor(
            compactionWriter,
            filterTimeseriesMetadataOffsetMap(currentBatchMeasurementSchemas),
            readerCacheMap,
            modificationCacheMap,
            sortedSourceFiles,
            deviceId,
            subTaskId,
            currentBatchMeasurementSchemas,
            summary);
    executor.execute();
    System.out.println(executor.getCompactChunkPlans());
    return executor.getCompactChunkPlans();
  }

  private void compactLeftBatches(List<CompactChunkPlan> compactionPlan)
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    while (compactedMeasurements.size() < valueMeasurementSchemas.size()) {
      List<IMeasurementSchema> selectedValueColumnGroup =
          AlignedSeriesGroupCompactionUtils.selectColumnGroupToCompact(
              valueMeasurementSchemas, compactedMeasurements);
      List<IMeasurementSchema> currentBatchMeasurementSchemas =
          new ArrayList<>(selectedValueColumnGroup.size() + 1);
      currentBatchMeasurementSchemas.add(timeSchema);
      currentBatchMeasurementSchemas.addAll(selectedValueColumnGroup);
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
              compactionPlan,
              summary);
      executor.execute();
    }
  }

  private Map<String, Map<TsFileResource, Pair<Long, Long>>> filterTimeseriesMetadataOffsetMap(
      List<IMeasurementSchema> measurementSchemas) {
    Map<String, Map<TsFileResource, Pair<Long, Long>>> result = new HashMap<>();
    for (IMeasurementSchema measurementSchema : measurementSchemas) {
      String measurementId = measurementSchema.getMeasurementId();
      Map<TsFileResource, Pair<Long, Long>> entryValue =
          timeseriesMetadataOffsetMap.get(measurementSchema.getMeasurementId());
      result.put(measurementId, entryValue);
    }
    return result;
  }

  private class FirstBatchFastAlignedSeriesCompactionExecutor
      extends FastAlignedSeriesCompactionExecutor {

    private List<CompactChunkPlan> compactionPlan = new ArrayList<>();

    public FirstBatchFastAlignedSeriesCompactionExecutor(
        AbstractCompactionWriter compactionWriter,
        Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
        Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
        Map<String, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>>
            modificationCacheMap,
        List<TsFileResource> sortedSourceFiles,
        IDeviceID deviceId,
        int subTaskId,
        List<IMeasurementSchema> measurementSchemas,
        FastCompactionTaskSummary summary) {
      super(
          compactionWriter,
          timeseriesMetadataOffsetMap,
          readerCacheMap,
          modificationCacheMap,
          sortedSourceFiles,
          deviceId,
          subTaskId,
          measurementSchemas,
          summary);
    }

    @Override
    public void execute()
        throws PageException, IllegalPathException, IOException, WriteProcessException {
      FirstBatchCompactionAlignedChunkWriter firstBatchCompactionAlignedChunkWriter =
          new FirstBatchCompactionAlignedChunkWriter(
              this.measurementSchemas.remove(0), this.measurementSchemas);

      firstBatchCompactionAlignedChunkWriter.registerFlushChunkWriterCallback(
          chunkWriter -> {
            this.compactionPlan.add(
                ((FirstBatchCompactionAlignedChunkWriter) chunkWriter).getCompactedChunkRecord());
          });

      compactionWriter.startMeasurement(
          TsFileConstant.TIME_COLUMN_ID, firstBatchCompactionAlignedChunkWriter, subTaskId);
      compactFiles();
      compactionWriter.endMeasurement(subTaskId);
    }

    @Override
    protected List<AlignedChunkMetadata> getAlignedChunkMetadataList(TsFileResource resource)
        throws IOException {
      return getAlignedChunkMetadataListBySelectedValueColumn(resource, measurementSchemas);
    }

    @Override
    protected void successFlushChunk(ChunkMetadataElement chunkMetadataElement) {
      compactionPlan.add(
          new CompactChunkPlan(
              chunkMetadataElement.chunkMetadata.getStartTime(),
              chunkMetadataElement.chunkMetadata.getEndTime()));
      super.successFlushChunk(chunkMetadataElement);
    }

    public List<CompactChunkPlan> getCompactChunkPlans() {
      return compactionPlan;
    }
  }

  private class FollowingBatchFastAlignedSeriesCompactionExecutor
      extends FastAlignedSeriesCompactionExecutor {

    private final List<CompactChunkPlan> compactionPlan;
    private int currentCompactChunk = 0;
    private FollowingBatchCompactionAlignedChunkWriter followingBatchCompactionAlignedChunkWriter;

    public FollowingBatchFastAlignedSeriesCompactionExecutor(
        AbstractCompactionWriter compactionWriter,
        Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
        Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
        Map<String, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>>
            modificationCacheMap,
        List<TsFileResource> sortedSourceFiles,
        IDeviceID deviceId,
        int subTaskId,
        List<IMeasurementSchema> measurementSchemas,
        List<CompactChunkPlan> compactionPlan,
        FastCompactionTaskSummary summary) {
      super(
          compactionWriter,
          timeseriesMetadataOffsetMap,
          readerCacheMap,
          modificationCacheMap,
          sortedSourceFiles,
          deviceId,
          subTaskId,
          measurementSchemas,
          summary);
      this.compactionPlan = compactionPlan;
      isFollowedBatch = true;
    }

    @Override
    public void execute()
        throws PageException, IllegalPathException, IOException, WriteProcessException {
      followingBatchCompactionAlignedChunkWriter =
          new FollowingBatchCompactionAlignedChunkWriter(
              measurementSchemas.remove(0),
              measurementSchemas,
              compactionPlan.get(currentCompactChunk));
      followingBatchCompactionAlignedChunkWriter.registerFlushChunkWriterCallback(
          (chunkWriter) -> {
            currentCompactChunk++;
            if (currentCompactChunk < compactionPlan.size()) {
              ((FollowingBatchCompactionAlignedChunkWriter) chunkWriter)
                  .setCompactChunkPlan(compactionPlan.get(currentCompactChunk));
            }
          });
      compactionWriter.startMeasurement(
          TsFileConstant.TIME_COLUMN_ID, followingBatchCompactionAlignedChunkWriter, subTaskId);
      compactFiles();
      compactionWriter.endMeasurement(subTaskId);
    }

    @Override
    protected List<AlignedChunkMetadata> getAlignedChunkMetadataList(TsFileResource resource)
        throws IOException {
      return getAlignedChunkMetadataListBySelectedValueColumn(resource, measurementSchemas);
    }

    protected void compactWithNonOverlapChunk(ChunkMetadataElement chunkMetadataElement)
        throws IOException, PageException, WriteProcessException, IllegalPathException {
      boolean success;
      success =
          compactionWriter.flushAlignedChunk(
              chunkMetadataElement,
              subTaskId,
              () -> compactionPlan.get(currentCompactChunk).isCompactedByDirectlyFlush());

      if (success) {
        // flush chunk successfully, then remove this chunk
        successFlushChunk(chunkMetadataElement);
        updateSummary(chunkMetadataElement, ChunkStatus.DIRECTORY_FLUSH);
        checkShouldRemoveFile(chunkMetadataElement);
      } else {
        // unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize
        // chunk
        summary.chunkNoneOverlapButDeserialize += 1;
        deserializeChunkIntoPageQueue(chunkMetadataElement);
        compactPages();
      }
    }

    @Override
    protected void compactWithNonOverlapPage(PageElement pageElement)
        throws PageException, IOException, WriteProcessException, IllegalPathException {
      boolean success;
      AlignedPageElement alignedPageElement = (AlignedPageElement) pageElement;
      success = compactionWriter.flushAlignedPage(alignedPageElement, subTaskId);
      if (success) {
        // flush the page successfully, then remove this page
        checkShouldRemoveFile(pageElement);
      } else {
        // unsealed page is not large enough or page.endTime > file.endTime, then deserialze it
        summary.pageNoneOverlapButDeserialize += 1;
        if (!pointPriorityReader.addNewPageIfPageNotEmpty(pageElement)) {
          return;
        }

        // write data points of the current page into chunk writer
        TimeValuePair point;
        while (pointPriorityReader.hasNext()) {
          point = pointPriorityReader.currentPoint();
          if (point.getTimestamp() > pageElement.getEndTime()) {
            // finish writing this page
            break;
          }
          compactionWriter.write(point, subTaskId);
          pointPriorityReader.next();
        }
      }
    }

    @Override
    protected void successFlushChunk(ChunkMetadataElement chunkMetadataElement) {
      currentCompactChunk++;
      if (currentCompactChunk < compactionPlan.size()) {
        followingBatchCompactionAlignedChunkWriter.setCompactChunkPlan(
            compactionPlan.get(currentCompactChunk));
      }
      super.successFlushChunk(chunkMetadataElement);
    }
  }
}
