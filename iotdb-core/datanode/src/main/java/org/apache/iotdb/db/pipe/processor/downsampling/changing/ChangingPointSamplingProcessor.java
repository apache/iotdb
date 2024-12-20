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

package org.apache.iotdb.db.pipe.processor.downsampling.changing;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.db.pipe.event.common.row.PipeRemarkableRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.pipe.processor.downsampling.DownSamplingProcessor;
import org.apache.iotdb.db.pipe.processor.downsampling.PartialPathLastObjectCache;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ChangingPointSamplingProcessor extends DownSamplingProcessor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ChangingPointSamplingProcessor.class);

  /**
   * The maximum absolute difference the user set if the data's value is within
   * compressionDeviation, it will be compressed and discarded after compression
   */
  private double compressionDeviation;

  private boolean isFilteredByArrivalTime = true;

  private PartialPathLastObjectCache<ChangingPointFilter> pathLastObjectCache;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final PipeParameters parameters = validator.getParameters();
    compressionDeviation =
        parameters.getDoubleOrDefault(
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION,
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION_DEFAULT_VALUE);

    final boolean isChangingPointProcessor =
        BuiltinPipePlugin.CHANGING_POINT_SAMPLING_PROCESSOR
            .getPipePluginName()
            .equals(parameters.getString("processor"));

    if (isChangingPointProcessor) {
      isFilteredByArrivalTime = true;
      compressionDeviation =
          parameters.getDoubleOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_VALUE_VARIATION,
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_VALUE_VARIATION_DEFAULT_VALUE);
      eventTimeMinInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_EVENT_TIME_MIN_INTERVAL,
              TimestampPrecisionUtils.convertToCurrPrecision(
                  PipeProcessorConstant
                      .PROCESSOR_CHANGING_POINT_EVENT_TIME_MIN_INTERVAL_DEFAULT_VALUE,
                  TimeUnit.MILLISECONDS));
      eventTimeMaxInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_EVENT_TIME_MAX_INTERVAL,
              TimestampPrecisionUtils.convertToCurrPrecision(
                  PipeProcessorConstant
                      .PROCESSOR_CHANGING_POINT_EVENT_TIME_MAX_INTERVAL_DEFAULT_VALUE,
                  TimeUnit.MILLISECONDS));
      arrivalTimeMinInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MIN_INTERVAL,
              TimestampPrecisionUtils.convertToCurrPrecision(
                  PipeProcessorConstant
                      .PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MIN_INTERVAL_DEFAULT_VALUE,
                  TimeUnit.MILLISECONDS));
      arrivalTimeMaxInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MAX_INTERVAL,
              TimestampPrecisionUtils.convertToCurrPrecision(
                  PipeProcessorConstant
                      .PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MAX_INTERVAL_DEFAULT_VALUE,
                  TimeUnit.MILLISECONDS));
    } else {
      isFilteredByArrivalTime = false;
      compressionDeviation =
          parameters.getDoubleOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION,
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_VALUE_VARIATION_DEFAULT_VALUE);
      eventTimeMinInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_KEY,
              TimestampPrecisionUtils.convertToCurrPrecision(
                  PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_DEFAULT_VALUE,
                  TimeUnit.MILLISECONDS));
      eventTimeMaxInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_KEY,
              TimestampPrecisionUtils.convertToCurrPrecision(
                  PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_DEFAULT_VALUE,
                  TimeUnit.MILLISECONDS));
      // will not be used
      arrivalTimeMinInterval = 0;
      arrivalTimeMaxInterval = Long.MAX_VALUE;
    }

    validateTimeInterval(validator);
    validator.validate(
        compressionDeviation -> (Double) compressionDeviation >= 0,
        String.format(
            "%s must be >= 0, but got %s",
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION,
            compressionDeviation),
        compressionDeviation);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    super.customize(parameters, configuration);

    final boolean isChangingPointProcessor =
        BuiltinPipePlugin.CHANGING_POINT_SAMPLING_PROCESSOR
            .getPipePluginName()
            .equals(parameters.getString("processor"));

    if (isChangingPointProcessor) {
      LOGGER.info(
          "ChangingPointSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}, {}: {}, {}: {}.",
          dataBaseNameWithPathSeparator,
          PipeProcessorConstant.PROCESSOR_CHANGING_POINT_VALUE_VARIATION,
          compressionDeviation,
          PipeProcessorConstant.PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MIN_INTERVAL,
          arrivalTimeMinInterval,
          PipeProcessorConstant.PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MAX_INTERVAL,
          arrivalTimeMaxInterval,
          PipeProcessorConstant.PROCESSOR_CHANGING_POINT_EVENT_TIME_MIN_INTERVAL,
          eventTimeMinInterval,
          PipeProcessorConstant.PROCESSOR_CHANGING_POINT_EVENT_TIME_MAX_INTERVAL,
          eventTimeMaxInterval);
    } else {
      LOGGER.info(
          "ChangingValueSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}.",
          dataBaseNameWithPathSeparator,
          PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION,
          compressionDeviation,
          PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_KEY,
          eventTimeMinInterval,
          PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_KEY,
          eventTimeMaxInterval);
    }
  }

  @Override
  protected void initPathLastObjectCache(final long memoryLimitInBytes) {
    pathLastObjectCache =
        new PartialPathLastObjectCache<ChangingPointFilter>(memoryLimitInBytes) {
          @Override
          protected long calculateMemoryUsage(ChangingPointFilter object) {
            return object.estimatedMemory();
          }
        };
  }

  @Override
  protected void processRow(
      Row row,
      RowCollector rowCollector,
      String deviceSuffix,
      AtomicReference<Exception> exception) {
    final PipeRemarkableRow remarkableRow = new PipeRemarkableRow((PipeRow) row);
    final long currentRowTime = row.getTime();
    final long arrivalTime = currentTime.apply();

    boolean hasNonNullMeasurements = false;
    for (int i = 0, size = row.size(); i < size; i++) {
      if (row.isNull(i)) {
        continue;
      }

      final String timeSeriesSuffix =
          deviceSuffix + TsFileConstant.PATH_SEPARATOR + row.getColumnName(i);
      final ChangingPointFilter filter =
          pathLastObjectCache.getPartialPathLastObject(timeSeriesSuffix);

      if (Objects.nonNull(filter)) {
        final Boolean result = filterArrivalTimeAndEventTime(filter, arrivalTime, currentRowTime);
        if (Objects.isNull(result)) {
          if (filter.filter(arrivalTime, currentRowTime, row.getObject(i))) {
            hasNonNullMeasurements = true;
          } else {
            remarkableRow.markNull(i);
          }
          continue;
        }

        // It will not be null
        if (!result) {
          remarkableRow.markNull(i);
          continue;
        }

        // The arrival time or event time is greater than the maximum time interval
        filter.reset(arrivalTime, currentRowTime, row.getObject(i));
      } else {
        pathLastObjectCache.setPartialPathLastObject(
            timeSeriesSuffix,
            new ChangingPointFilter(
                arrivalTime,
                currentRowTime,
                row.getObject(i),
                compressionDeviation,
                isFilteredByArrivalTime));
      }

      hasNonNullMeasurements = true;
    }

    if (hasNonNullMeasurements) {
      try {
        rowCollector.collectRow(remarkableRow);
      } catch (IOException e) {
        exception.set(e);
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (pathLastObjectCache != null) {
      pathLastObjectCache.close();
    }
  }
}
