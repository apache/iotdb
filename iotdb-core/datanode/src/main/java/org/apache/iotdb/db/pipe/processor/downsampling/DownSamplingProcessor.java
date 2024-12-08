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

package org.apache.iotdb.db.pipe.processor.downsampling;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskProcessorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.storageengine.StorageEngine;
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

import org.apache.tsfile.common.constant.TsFileConstant;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY;

public abstract class DownSamplingProcessor implements PipeProcessor {

  protected long memoryLimitInBytes;

  protected boolean shouldSplitFile;

  protected String dataBaseNameWithPathSeparator;

  /**
   * The minimum interval of arrival times in milliseconds. Represents the minimum time interval
   * between the arrival times of two consecutive events.
   */
  protected long arrivalTimeMinInterval;

  /**
   * The maximum interval of arrival times in milliseconds. Represents the maximum time interval
   * between the arrival times of two consecutive events.
   */
  protected long arrivalTimeMaxInterval;

  /**
   * The minimum interval of event times in milliseconds. Represents the minimum time interval
   * between the event times of two consecutive events.
   */
  protected long eventTimeMinInterval;

  /**
   * The maximum interval of event times in milliseconds. Represents the maximum time interval
   * between the event times of two consecutive events.
   */
  protected long eventTimeMaxInterval;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    memoryLimitInBytes =
        validator
            .getParameters()
            .getLongOrDefault(
                PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY,
                PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE);

    validator.validate(
        memoryLimitInBytes -> (Long) memoryLimitInBytes > 0,
        String.format(
            "%s must be > 0, but got %s",
            PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY, memoryLimitInBytes),
        memoryLimitInBytes);
  }

  public void validatorTimeInterval(final PipeParameterValidator validator) throws Exception {
    validator
        .validate(
            eventTimeMinInterval -> (long) eventTimeMinInterval >= 0,
            String.format(
                "%s must be >= 0, but got %s", "event-time.min-interval", eventTimeMinInterval),
            eventTimeMinInterval)
        .validate(
            eventTimeMaxInterval -> (long) eventTimeMaxInterval >= 0,
            String.format(
                "%s must be >= 0, but got %s", "event-time.max-interval", eventTimeMaxInterval),
            eventTimeMaxInterval)
        .validate(
            minMaxPair -> (Long) minMaxPair[0] <= (Long) minMaxPair[1],
            String.format(
                "%s must be <= %s, but got %s and %s",
                "event-time.min-interval",
                "event-time.max-interval",
                eventTimeMinInterval,
                eventTimeMaxInterval),
            eventTimeMinInterval,
            eventTimeMaxInterval)
        .validate(
            arrivalTimeMinInterval -> (long) arrivalTimeMinInterval >= 0,
            String.format(
                "%s must be >= 0, but got %s", "arrival-time.min-interval", arrivalTimeMinInterval),
            arrivalTimeMinInterval)
        .validate(
            arrivalTimeMaxInterval -> (long) arrivalTimeMaxInterval >= 0,
            String.format(
                "%s must be >= 0, but got %s", "arrival-time.max-interval", arrivalTimeMaxInterval),
            arrivalTimeMaxInterval)
        .validate(
            minMaxPair -> (Long) minMaxPair[0] <= (Long) minMaxPair[1],
            String.format(
                "%s must be <= %s, but got %s and %s",
                "arrival-time.min-interval",
                "arrival-time.max-interval",
                arrivalTimeMinInterval,
                arrivalTimeMaxInterval),
            arrivalTimeMinInterval,
            arrivalTimeMaxInterval);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    shouldSplitFile =
        parameters.getBooleanOrDefault(
            PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY,
            PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_DEFAULT_VALUE);

    dataBaseNameWithPathSeparator =
        StorageEngine.getInstance()
                .getDataRegion(
                    new DataRegionId(
                        ((PipeTaskProcessorRuntimeEnvironment)
                                configuration.getRuntimeEnvironment())
                            .getRegionId()))
                .getDatabaseName()
            + TsFileConstant.PATH_SEPARATOR;
  }

  protected abstract void initPathLastObjectCache(long memoryLimitInBytes);

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      eventCollector.collect(tabletInsertionEvent);
      return;
    }

    final AtomicReference<String> deviceSuffix = new AtomicReference<>();
    final AtomicReference<Exception> exception = new AtomicReference<>();

    tabletInsertionEvent
        .processRowByRow(
            (row, rowCollector) -> {
              // To reduce the memory usage, we use the device suffix
              // instead of the full path as the key.
              if (deviceSuffix.get() == null) {
                deviceSuffix.set(
                    row.getDeviceId().replaceFirst(this.dataBaseNameWithPathSeparator, ""));
              }

              processRow(row, rowCollector, deviceSuffix.get(), exception);
            })
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

  protected abstract void processRow(
      Row row,
      RowCollector rowCollector,
      String deviceSuffix,
      AtomicReference<Exception> exception);

  /**
   * Determine the arrival time interval and event time interval.
   *
   * @return true to indicate that the process does not need to be continued and the data can be
   *     updated directly. False means that the event is discarded directly. Null means that the
   *     downsampling algorithm can continue to be executed.
   */
  protected Boolean filterArrivalTimeAndEventTime(
      final DownSamplingFilter filter, final long arrivalTime, final long eventTime) {
    final long arrivalTimeInterval = Math.abs(arrivalTime - filter.getLastPointArrivalTime());

    if (filter.isFilteredByArrivalTime()) {
      if (arrivalTimeInterval >= arrivalTimeMaxInterval) {
        return Boolean.TRUE;
      }
      if (arrivalTimeInterval < arrivalTimeMinInterval) {
        return Boolean.FALSE;
      }
    }

    final long eventTimeInterval = Math.abs(eventTime - filter.getLastPointEventTime());
    if (eventTimeInterval >= eventTimeMaxInterval) {
      return Boolean.TRUE;
    }

    if (eventTimeInterval < eventTimeMinInterval) {
      return Boolean.FALSE;
    }
    return null;
  }

  /**
   * If data comes in {@link TsFileInsertionEvent}, we will not split it into {@link
   * TabletInsertionEvent} by default, because the data in {@link TsFileInsertionEvent} is already
   * compressed, down-sampling may not reduce the size of data but will surely increase the CPU
   * usage.
   */
  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (shouldSplitFile) {
      try {
        for (final TabletInsertionEvent tabletInsertionEvent :
            tsFileInsertionEvent.toTabletInsertionEvents()) {
          process(tabletInsertionEvent, eventCollector);
        }
      } finally {
        tsFileInsertionEvent.close();
      }
    } else {
      eventCollector.collect(tsFileInsertionEvent);
    }
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    eventCollector.collect(event);
  }
}
