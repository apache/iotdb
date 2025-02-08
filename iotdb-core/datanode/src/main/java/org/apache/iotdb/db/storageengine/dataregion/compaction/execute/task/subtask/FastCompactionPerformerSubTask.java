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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.BatchedFastAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.FastAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.FastNonAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@SuppressWarnings("squid:S107")
public class FastCompactionPerformerSubTask implements Callable<Void> {

  private final FastCompactionTaskSummary summary;

  private final AbstractCompactionWriter compactionWriter;

  private final int subTaskId;

  // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
  // used to get the chunk metadatas from tsfile directly according to timeseries metadata offset.
  private final Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap;

  private final Map<TsFileResource, TsFileSequenceReader> readerCacheMap;

  private final Map<String, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
      modificationCacheMap;

  // source files which are sorted by the start time of current device from old to new. Notice: If
  // the type of timeIndex is FileTimeIndex, it may contain resources in which the current device
  // does not exist.
  private final List<TsFileResource> sortedSourceFiles;

  private final boolean isAligned;

  private final boolean ignoreAllNullRows;

  private IDeviceID deviceId;

  private List<String> measurements;

  private List<IMeasurementSchema> measurementSchemas;

  /** Used for nonAligned timeseries. */
  @SuppressWarnings("squid:S107")
  public FastCompactionPerformerSubTask(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<String, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
          modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      List<String> measurements,
      IDeviceID deviceId,
      FastCompactionTaskSummary summary,
      int subTaskId) {
    this.compactionWriter = compactionWriter;
    this.subTaskId = subTaskId;
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.isAligned = false;
    this.deviceId = deviceId;
    this.readerCacheMap = readerCacheMap;
    this.modificationCacheMap = modificationCacheMap;
    this.sortedSourceFiles = sortedSourceFiles;
    this.measurements = measurements;
    this.summary = summary;
    this.ignoreAllNullRows = true;
  }

  /** Used for aligned timeseries. */
  public FastCompactionPerformerSubTask(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<String, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
          modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      List<IMeasurementSchema> measurementSchemas,
      IDeviceID deviceId,
      FastCompactionTaskSummary summary,
      boolean ignoreAllNullRows) {
    this.compactionWriter = compactionWriter;
    this.subTaskId = 0;
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.isAligned = true;
    this.deviceId = deviceId;
    this.readerCacheMap = readerCacheMap;
    this.modificationCacheMap = modificationCacheMap;
    this.sortedSourceFiles = sortedSourceFiles;
    this.measurementSchemas = measurementSchemas;
    this.summary = summary;
    this.ignoreAllNullRows = ignoreAllNullRows;
  }

  @Override
  public Void call()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    if (!isAligned) {
      FastNonAlignedSeriesCompactionExecutor seriesCompactionExecutor =
          new FastNonAlignedSeriesCompactionExecutor(
              compactionWriter,
              readerCacheMap,
              modificationCacheMap,
              sortedSourceFiles,
              deviceId,
              subTaskId,
              summary);
      for (String measurement : measurements) {
        seriesCompactionExecutor.setNewMeasurement(timeseriesMetadataOffsetMap.get(measurement));
        seriesCompactionExecutor.execute();
      }
    } else {
      FastAlignedSeriesCompactionExecutor seriesCompactionExecutor;
      if (measurementSchemas.size() - 1
          > IoTDBDescriptor.getInstance()
              .getConfig()
              .getCompactionMaxAlignedSeriesNumInOneBatch()) {
        seriesCompactionExecutor =
            new BatchedFastAlignedSeriesCompactionExecutor(
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
      } else {
        seriesCompactionExecutor =
            new FastAlignedSeriesCompactionExecutor(
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
      }
      seriesCompactionExecutor.execute();
    }
    return null;
  }
}
