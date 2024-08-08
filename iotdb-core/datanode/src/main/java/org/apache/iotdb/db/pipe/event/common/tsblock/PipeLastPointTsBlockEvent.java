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

package org.apache.iotdb.db.pipe.event.common.tsblock;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeLastPointTabletEvent;
import org.apache.iotdb.db.pipe.processor.downsampling.PartialPathLastObjectCache;
import org.apache.iotdb.db.pipe.processor.downsampling.lastpoint.LastPointFilter;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.time.LocalDate;
import java.util.List;

public class PipeLastPointTsBlockEvent extends EnrichedEvent {

  private final PartialPath partialPath;

  private final List<MeasurementSchema> measurementSchemas;

  private final long captureTime;

  private TsBlock tsBlock;

  private PipeMemoryBlock pipeMemoryBlock;

  public PipeLastPointTsBlockEvent(
      final TsBlock tsBlock,
      final long captureTime,
      final PartialPath partialPath,
      final List<MeasurementSchema> measurementSchemas,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pipePattern,
      final long startTime,
      final long endTime) {
    super(pipeName, creationTime, pipeTaskMeta, pipePattern, startTime, endTime);
    this.tsBlock = tsBlock;
    this.partialPath = partialPath;
    this.measurementSchemas = measurementSchemas;
    this.captureTime = captureTime;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    pipeMemoryBlock =
        PipeDataNodeResourceManager.memory().forceAllocate(tsBlock.getRetainedSizeInBytes());
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    pipeMemoryBlock.close();
    tsBlock = null;
    return false;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return MinimumProgressIndex.INSTANCE;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName,
      long creationTime,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pattern,
      long startTime,
      long endTime) {
    return new PipeLastPointTsBlockEvent(
        tsBlock,
        captureTime,
        partialPath,
        measurementSchemas,
        pipeName,
        creationTime,
        pipeTaskMeta,
        pipePattern,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return true;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return true;
  }

  /////////////////////////// PipeLastPointTabletEvent ///////////////////////////

  public PipeLastPointTabletEvent convertToPipeLastPointTabletEvent(
      PartialPathLastObjectCache<LastPointFilter<?>> partialPathToLatestTimeCache) {
    return new PipeLastPointTabletEvent(
        convertToTablet(),
        captureTime,
        partialPathToLatestTimeCache,
        pipeName,
        creationTime,
        pipeTaskMeta,
        pipePattern,
        startTime,
        endTime);
  }

  /////////////////////////// convertToTablet ///////////////////////////

  private Tablet convertToTablet() {
    final int columnCount = tsBlock.getValueColumnCount();
    Object[] values = new Object[columnCount];
    BitMap[] bitMaps = new BitMap[columnCount];

    for (int i = 0; i < columnCount; ++i) {
      final Column column = tsBlock.getColumn(i);
      final int rowCount = column.getPositionCount();
      final BitMap bitMap = new BitMap(rowCount);
      for (int j = 0; j < rowCount; j++) {
        if (column.isNull(j)) {
          bitMap.mark(j);
        }
      }
      bitMaps[i] = bitMap;

      final TSDataType type = column.getDataType();
      switch (type) {
        case BOOLEAN:
          values[i] = column.getBooleans();
          break;
        case INT32:
          values[i] = column.getInts();
          break;
        case DATE:
          int[] dates = column.getInts();
          LocalDate[] localDates = new LocalDate[dates.length];
          for (int row = 0; row < dates.length; row++) {
            localDates[row] = DateUtils.parseIntToLocalDate(dates[row]);
          }
          values[i] = localDates;
          break;
        case INT64:
        case TIMESTAMP:
          values[i] = column.getLongs();
          break;
        case FLOAT:
          values[i] = column.getFloats();
          break;
        case DOUBLE:
          values[i] = column.getDoubles();
          break;
        case TEXT:
        case BLOB:
        case STRING:
          values[i] = column.getBinaries();
          break;
        default:
          throw new UnSupportedDataTypeException(
              "TsBlock format to Tablet format does not support type " + type);
      }
    }

    final Tablet tablet =
        new Tablet(
            partialPath.getDevice(),
            measurementSchemas,
            tsBlock.getTimeColumn().getLongs(),
            values,
            bitMaps,
            tsBlock.getValueColumnCount());

    return tablet;
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeLastPointTsBlockEvent{tsBlock=%s, partialPath=%s, measurementSchemas=%s, captureTime=%s, pipeMemoryBlock=%s}",
            tsBlock, partialPath, measurementSchemas, captureTime, pipeMemoryBlock)
        + " - "
        + super.toString();
  }

  /////////////////////////// Getter ///////////////////////////

  public long getCaptureTime() {
    return captureTime;
  }
}
