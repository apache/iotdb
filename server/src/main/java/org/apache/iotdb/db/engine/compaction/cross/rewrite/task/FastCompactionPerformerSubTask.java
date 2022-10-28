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
package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.engine.compaction.cross.utils.AlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.cross.utils.NonAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.writer.FastCrossCompactionWriter;
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

public class FastCompactionPerformerSubTask
    implements Callable<FastCompactionPerformerSubTask.Summary> {
  public class Summary {
    public int CHUNK_NONE_OVERLAP;
    public int CHUNK_NONE_OVERLAP_BUT_DESERIALIZE;
    public int CHUNK_OVERLAP;

    public int PAGE_NONE_OVERLAP;
    public int PAGE_OVERLAP;
    public int PAGE_FAKE_OVERLAP;
    public int PAGE_NONE_OVERLAP_BUT_DESERIALIZE;

    public void increase(Summary summary) {
      this.CHUNK_NONE_OVERLAP += summary.CHUNK_NONE_OVERLAP;
      this.CHUNK_NONE_OVERLAP_BUT_DESERIALIZE += summary.CHUNK_NONE_OVERLAP_BUT_DESERIALIZE;
      this.CHUNK_OVERLAP += summary.CHUNK_OVERLAP;
      this.PAGE_NONE_OVERLAP += summary.PAGE_NONE_OVERLAP;
      this.PAGE_OVERLAP += summary.PAGE_OVERLAP;
      this.PAGE_FAKE_OVERLAP += summary.PAGE_FAKE_OVERLAP;
      this.PAGE_NONE_OVERLAP_BUT_DESERIALIZE += summary.PAGE_NONE_OVERLAP_BUT_DESERIALIZE;
    }
  }

  private final Summary summary = new Summary();

  private FastCrossCompactionWriter compactionWriter;

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
      FastCrossCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      List<String> measurements,
      String deviceId,
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
  }

  /** Used for aligned timeseries. */
  public FastCompactionPerformerSubTask(
      FastCrossCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      List<IMeasurementSchema> measurementSchemas,
      String deviceId) {
    this.compactionWriter = compactionWriter;
    this.subTaskId = 0;
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.isAligned = true;
    this.deviceId = deviceId;
    this.readerCacheMap = readerCacheMap;
    this.modificationCacheMap = modificationCacheMap;
    this.sortedSourceFiles = sortedSourceFiles;
    this.measurementSchemas = measurementSchemas;
  }

  @Override
  public Summary call()
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
        seriesCompactionExecutor.startNewtMeasurement(timeseriesMetadataOffsetMap.get(measurement));
        seriesCompactionExecutor.excute();
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
      seriesCompactionExecutor.excute();
    }
    return summary;
  }
}
