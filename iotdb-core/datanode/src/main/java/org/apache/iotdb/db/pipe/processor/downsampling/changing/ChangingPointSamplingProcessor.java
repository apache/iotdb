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

import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.db.pipe.event.common.row.PipeRemarkableRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.pipe.processor.downsampling.DownSamplingProcessor;
import org.apache.iotdb.db.pipe.processor.downsampling.PartialPathLastObjectCache;
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
import java.util.concurrent.atomic.AtomicReference;

public class ChangingPointSamplingProcessor extends DownSamplingProcessor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ChangingPointSamplingProcessor.class);

  /**
   * The maximum absolute difference the user set if the data's value is within
   * compressionDeviation, it will be compressed and discarded after compression
   */
  private double compressionDeviation;

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
        parameters.getString("processor").equals("changing-point-sampling-processor");

    if (isChangingPointProcessor) {
      compressionDeviation =
          parameters.getDoubleOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_VALUE_INTERVAL,
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_VALUE_INTERVAL_DEFAULT_VALUE);
      eventTimeMinInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_EVENT_TIME_MIN_INTERVAL,
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_EVENT_TIME_MIN_INTERVAL_DEFAULT_VALUE);
      eventTimeMaxInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_EVENT_TIME_MAX_INTERVAL,
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_EVENT_TIME_MAX_INTERVAL_DEFAULT_VALUE);
      arrivalTimeMinInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MIN_INTERVAL,
              PipeProcessorConstant
                  .PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MIN_INTERVAL_DEFAULT_VALUE);
      arrivalTimeMaxInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MAX_INTERVAL,
              PipeProcessorConstant
                  .PROCESSOR_CHANGING_POINT_ARRIVAL_TIME_MAX_INTERVAL_DEFAULT_VALUE);
    } else {
      compressionDeviation =
          parameters.getDoubleOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION,
              PipeProcessorConstant.PROCESSOR_CHANGING_POINT_VALUE_INTERVAL_DEFAULT_VALUE);
      eventTimeMinInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_KEY,
              PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_DEFAULT_VALUE);
      eventTimeMinInterval =
          parameters.getLongOrDefault(
              PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_KEY,
              PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_DEFAULT_VALUE);
      arrivalTimeMinInterval = 0;
      arrivalTimeMaxInterval = Long.MAX_VALUE;
    }

    validatorTimeInterval(validator);
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
        parameters.getString("processor").equals("changing-point-sampling-processor");

    if (isChangingPointProcessor) {
      LOGGER.info(
          "ChangingPointSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}, {}: {}, {}: {}.",
          dataBaseNameWithPathSeparator,
          PipeProcessorConstant.PROCESSOR_CHANGING_POINT_VALUE_INTERVAL,
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

    initPathLastObjectCache(memoryLimitInBytes);
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
    final long arrivalTime = System.currentTimeMillis();

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

        if (result == Boolean.FALSE) {
          remarkableRow.markNull(i);
          continue;
        }
      } else {
        pathLastObjectCache.setPartialPathLastObject(
            timeSeriesSuffix,
            new ChangingPointFilter(
                arrivalTime, currentRowTime, row.getObject(i), compressionDeviation));
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
