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

package org.apache.iotdb.db.pipe.processor.downsampling.lastpoint;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.pipe.event.common.row.PipeRemarkableRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeLastPointTabletEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsblock.PipeLastPointTsBlockEvent;
import org.apache.iotdb.db.pipe.processor.downsampling.PartialPathLastObjectCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeSchemaCache;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_LAST_POINT_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_LAST_POINT_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY;

public class LastPointSamplingProcessor implements PipeProcessor {

  private final DataNodeSchemaCache DATA_NODE_SCHEMA_CACHE = DataNodeSchemaCache.getInstance();

  private PartialPathLastObjectCache<LastPointFilter<?>> partialPathToLatestTimeCache;

  protected long memoryLimitInBytes;

  private PartialPathLastObjectCache<LastPointFilter<?>> initPathLastObjectCache(
      long memoryLimitInBytes) {
    partialPathToLatestTimeCache =
        new PartialPathLastObjectCache<LastPointFilter<?>>(memoryLimitInBytes) {
          @Override
          protected long calculateMemoryUsage(LastPointFilter<?> filter) {
            return 64; // Long.BYTES * 8
          }
        };
    return partialPathToLatestTimeCache;
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      return;
    }

    final AtomicReference<Exception> exception = new AtomicReference<>();

    final Iterable<TabletInsertionEvent> iterable =
        tabletInsertionEvent.processMaxTimestampRowByRow(
            (row, rowCollector) -> {
              processRow(row, rowCollector, exception);
            });

    iterable.forEach(
        event -> {
          try {
            PipeRawTabletInsertionEvent insertionEvent = (PipeRawTabletInsertionEvent) event;
            eventCollector.collect(
                new PipeLastPointTabletEvent(
                    insertionEvent.convertToTablet(),
                    partialPathToLatestTimeCache,
                    insertionEvent.getPipeName(),
                    insertionEvent.getCreationTime(),
                    insertionEvent.getPipeTaskMeta(),
                    insertionEvent.getPipePattern(),
                    insertionEvent.getStartTime(),
                    insertionEvent.getEndTime()));
          } catch (Exception e) {
            exception.set(e);
          }
        });

    if (exception.get() != null) {
      throw exception.get();
    }
  }

  protected void processRow(
      Row row, RowCollector rowCollector, AtomicReference<Exception> exception) {

    final PipeRemarkableRow remarkableRow = new PipeRemarkableRow((PipeRow) row);

    boolean hasLatestPointMeasurements = false;
    for (int i = 0, size = row.size(); i < size; i++) {
      if (row.isNull(i)) {
        continue;
      }
      TimeValuePair timeValuePair = null;
      String fullPath = row.getDeviceId() + TsFileConstant.PATH_SEPARATOR + row.getColumnName(i);
      try {
        // global latest point filter
        timeValuePair = DATA_NODE_SCHEMA_CACHE.getLastCache(new PartialPath(fullPath));
      } catch (Exception e) {
        exception.set(e);
      }

      if (timeValuePair != null && timeValuePair.getTimestamp() > row.getTime()) {
        remarkableRow.markNull(i);
        return;
      }

      // local latest point filter
      final LastPointFilter filter =
          partialPathToLatestTimeCache.getPartialPathLastObject(fullPath);
      if (filter != null) {
        if (filter.filter(row.getTime(), row.getObject(i))) {
          hasLatestPointMeasurements = true;
        } else {
          remarkableRow.markNull(i);
        }
      } else {
        hasLatestPointMeasurements = true;
        partialPathToLatestTimeCache.setPartialPathLastObject(
            fullPath, new LastPointFilter<>(row.getTime(), row.getObject(i)));
      }

      if (hasLatestPointMeasurements) {
        try {
          rowCollector.collectRow(row);
        } catch (Exception e) {
          exception.set(e);
        }
      }
    }
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    try {
      for (final TabletInsertionEvent tabletInsertionEvent :
          tsFileInsertionEvent.toTabletInsertionEvents()) {
        process(tabletInsertionEvent, eventCollector);
      }
    } finally {
      tsFileInsertionEvent.close();
    }
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws IOException {
    if (!(event instanceof PipeLastPointTsBlockEvent)) {
      eventCollector.collect(event);
      return;
    }
    PipeLastPointTsBlockEvent tsBlockEvent = (PipeLastPointTsBlockEvent) event;

    final PipeLastPointTabletEvent pipeLastPointTabletEvent =
        tsBlockEvent.convertToPipeLastPointTabletEvent(
            partialPathToLatestTimeCache, this::markEligibleColumnsBasedOnFilter);

    if (pipeLastPointTabletEvent != null) {
      eventCollector.collect(pipeLastPointTabletEvent);
    }
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {

    memoryLimitInBytes =
        validator
            .getParameters()
            .getLongOrDefault(
                PROCESSOR_LAST_POINT_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY,
                PROCESSOR_LAST_POINT_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE);

    validator.validate(
        memoryLimitInBytes -> (Long) memoryLimitInBytes > 0,
        String.format(
            "%s must be > 0, but got %s",
            PROCESSOR_LAST_POINT_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY, memoryLimitInBytes),
        memoryLimitInBytes);
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    partialPathToLatestTimeCache = initPathLastObjectCache(memoryLimitInBytes);
  }

  @Override
  public void close() throws Exception {
    partialPathToLatestTimeCache.close();
  }

  /**
   * Marks a column in the TsBlock as eligible based on the presence of the latest data point.
   *
   * <p>This method evaluates a single row within the TsBlock to determine if the column contains
   * the latest data point. If a LastPointFilter for the column is found in the cache, the method
   * checks whether the current data point is the latest. If no filter is present in the cache, the
   * column is automatically considered eligible.
   *
   * @param columnSelectionMap The BitMap that marks the eligibility of the column.
   * @param tsBlock The TsBlock that contains the columns to be evaluated.
   * @param deviceId The device identifier used to build the filter path.
   * @param measurementSchemas The list of measurement schemas used to complete the filter path.
   */
  private void markEligibleColumnsBasedOnFilter(
      BitMap columnSelectionMap,
      TsBlock tsBlock,
      String deviceId,
      List<MeasurementSchema> measurementSchemas) {
    int columnCount = tsBlock.getValueColumnCount();

    for (int i = 0; i < columnCount; i++) {
      Column column = tsBlock.getColumn(i);
      if (column.isNull(0)) {
        columnSelectionMap.mark(i);
        continue;
      }
      String fullPath =
          deviceId + TsFileConstant.PATH_SEPARATOR + measurementSchemas.get(i).getMeasurementId();

      final LastPointFilter filter =
          partialPathToLatestTimeCache.getPartialPathLastObject(fullPath);
      if (filter != null && !filter.filter(tsBlock.getTimeByIndex(0), column.getObject(0))) {
        columnSelectionMap.mark(i);
      }
    }
  }
}
