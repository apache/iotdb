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

package org.apache.iotdb.db.pipe.processor;

import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.DateTimeUtils;
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
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_START_TIME_KEY;

public class TimeRangeFilterProcessor implements PipeProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeRangeFilterProcessor.class);

  private long filterStartTime; // Event time
  private long filterEndTime; // Event time

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    PipeParameters parameters = validator.getParameters();

    if (parameters.hasAnyAttributes(SOURCE_START_TIME_KEY, SOURCE_END_TIME_KEY)) {
      try {
        filterStartTime =
            parameters.hasAnyAttributes(SOURCE_START_TIME_KEY)
                ? DateTimeUtils.convertDatetimeStrToLong(
                    parameters.getStringByKeys(SOURCE_START_TIME_KEY), ZoneId.systemDefault())
                : Long.MIN_VALUE;
        filterEndTime =
            parameters.hasAnyAttributes(SOURCE_END_TIME_KEY)
                ? DateTimeUtils.convertDatetimeStrToLong(
                    parameters.getStringByKeys(SOURCE_END_TIME_KEY), ZoneId.systemDefault())
                : Long.MAX_VALUE;
      } catch (Exception e) {
        // compatible with the current validation framework
        throw new PipeParameterNotValidException(e.getMessage());
      }
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    LOGGER.info(
        "TimeRangeFilterProcessor is initialized with {}: {}, {}: {}.",
        SOURCE_START_TIME_KEY,
        filterStartTime,
        SOURCE_END_TIME_KEY,
        filterEndTime);
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      eventCollector.collect(tabletInsertionEvent);
      return;
    }

    final AtomicReference<Exception> exception = new AtomicReference<>();

    tabletInsertionEvent
        .processRowByRow((row, rowCollector) -> processRow(row, rowCollector, exception))
        .forEach(
            event -> {
              try {
                eventCollector.collect(event);
              } catch (Exception e) {
                exception.set(e);
              }
            });

    if (exception.get() != null) {
      throw exception.get();
    }
  }

  private void processRow(
      Row row, RowCollector rowCollector, AtomicReference<Exception> exception) {
    if (isRowTimeOverlappedWithTimeRange(row)) {
      try {
        rowCollector.collectRow(row);
      } catch (Exception e) {
        exception.set(e);
      }
    }
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      eventCollector.collect(tsFileInsertionEvent);
      return;
    }

    if (isTsFileResourceOverlappedWithTimeRange(
        ((PipeTsFileInsertionEvent) tsFileInsertionEvent).getTsFileResource())) {
      eventCollector.collect(tsFileInsertionEvent);
    } else {
      // TODO: release event
    }
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }

  private boolean isTsFileResourceOverlappedWithTimeRange(TsFileResource resource) {
    return !(resource.getFileEndTime() < filterStartTime
        || filterEndTime < resource.getFileStartTime());
  }

  private boolean isRowTimeOverlappedWithTimeRange(Row row) {
    return filterStartTime <= row.getTime() && row.getTime() <= filterEndTime;
  }
}
