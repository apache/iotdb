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
package org.apache.iotdb.db.engine.compaction.execute.task.subtask;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.AlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.NonAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class FastCompactionPerformerSubTask implements Callable<Void> {

  private FastCompactionTaskSummary summary;

  private AbstractCompactionWriter compactionWriter;

  private int subTaskId;

  // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
  // used to get the chunk metadatas from tsfile directly according to timeseries metadata offset.
  private Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap;

  private Map<TsFileResource, TsFileSequenceReader> readerCacheMap;

  private final Map<TsFileResource, List<Modification>> modificationCacheMap;

  // source files which are sorted by the start time of current device from old to new. Notice: If
  // the type of timeIndex is FileTimeIndex, it may contain resources in which the current device
  // does not exist.
  private List<TsFileResource> sortedSourceFiles;

  private final boolean isAligned;

  private String deviceId;

  private List<String> measurements;

  private List<IMeasurementSchema> measurementSchemas;

  /** Used for nonAligned timeseries. */
  public FastCompactionPerformerSubTask(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      List<String> measurements,
      String deviceId,
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
  }

  /** Used for aligned timeseries. */
  public FastCompactionPerformerSubTask(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      List<IMeasurementSchema> measurementSchemas,
      String deviceId,
      FastCompactionTaskSummary summary) {
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
  }

  @Override
  public Void call()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    if (!isAligned) {
      NonAlignedSeriesCompactionExecutor seriesCompactionExecutor =
          new NonAlignedSeriesCompactionExecutor(
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
      AlignedSeriesCompactionExecutor seriesCompactionExecutor =
          new AlignedSeriesCompactionExecutor(
              compactionWriter,
              timeseriesMetadataOffsetMap,
              readerCacheMap,
              modificationCacheMap,
              sortedSourceFiles,
              deviceId,
              subTaskId,
              measurementSchemas,
              summary);
      seriesCompactionExecutor.execute();
    }
    return null;
  }
}
