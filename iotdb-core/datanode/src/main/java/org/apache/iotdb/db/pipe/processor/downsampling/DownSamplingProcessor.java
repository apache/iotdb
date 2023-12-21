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
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
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
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY;

public class DownSamplingProcessor implements PipeProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DownSamplingProcessor.class);

  private String dataBaseNameWithPathSeparator;
  private long intervalInCurrentPrecision;
  private boolean shouldSplitFile;

  private PartialPathLastTimeCache partialPathLastTimeCache;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // No need to validate.
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    final String dataBaseName =
        StorageEngine.getInstance()
            .getDataRegion(
                new DataRegionId(
                    ((PipeTaskProcessorRuntimeEnvironment) configuration.getRuntimeEnvironment())
                        .getRegionId()))
            .getDatabaseName();
    final long intervalSeconds =
        parameters.getLongOrDefault(
            PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_KEY,
            PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_DEFAULT_VALUE);
    final long memoryLimitInBytes =
        parameters.getLongOrDefault(
            PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY,
            PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE);
    shouldSplitFile =
        parameters.getBooleanOrDefault(
            PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY,
            PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_DEFAULT_VALUE);
    LOGGER.info(
        "DownSamplingProcessor in {} is initialized with {}: {}s, {}: {}, {}: {}.",
        dataBaseName,
        PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_KEY,
        intervalSeconds,
        PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY,
        memoryLimitInBytes,
        PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY,
        shouldSplitFile);

    dataBaseNameWithPathSeparator = dataBaseName + TsFileConstant.PATH_SEPARATOR;
    intervalInCurrentPrecision =
        TimestampPrecisionUtils.convertToCurrPrecision(intervalSeconds, TimeUnit.SECONDS);

    partialPathLastTimeCache = new PartialPathLastTimeCache(memoryLimitInBytes);
  }

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
                deviceSuffix.set(row.getDeviceId().replaceFirst(dataBaseNameWithPathSeparator, ""));
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

  private void processRow(
      Row row,
      RowCollector rowCollector,
      String deviceSuffix,
      AtomicReference<Exception> exception) {
    boolean hasNonNullMeasurements = false;

    for (int index = 0, size = row.size(); index < size; ++index) {
      if (row.isNull(index)) {
        continue;
      }

      final String timeSeriesSuffix =
          deviceSuffix + TsFileConstant.PATH_SEPARATOR + row.getColumnName(index);
      final Long lastSampleTime = partialPathLastTimeCache.getPartialPathLastTime(timeSeriesSuffix);

      if (lastSampleTime != null) {
        if (Math.abs(row.getTime() - lastSampleTime) >= intervalInCurrentPrecision) {
          hasNonNullMeasurements = true;
          partialPathLastTimeCache.setPartialPathLastTime(timeSeriesSuffix, row.getTime());
        }
      } else {
        hasNonNullMeasurements = true;
        partialPathLastTimeCache.setPartialPathLastTime(timeSeriesSuffix, row.getTime());
      }
    }

    if (hasNonNullMeasurements) {
      try {
        rowCollector.collectRow(row);
      } catch (Exception e) {
        exception.set(e);
      }
    }
  }

  /**
   * If data comes in {@link TsFileInsertionEvent}, we will not split it into {@link
   * TabletInsertionEvent} by default, because the data in {@link TsFileInsertionEvent} is already
   * compressed, down-sampling will not reduce the size of data but will increase the CPU usage.
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

  @Override
  public void close() throws Exception {
    if (partialPathLastTimeCache != null) {
      partialPathLastTimeCache.close();
    }
  }
}
