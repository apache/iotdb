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

package org.apache.iotdb.db.pipe.processor.downsampling.sdt;

import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.db.pipe.event.common.row.PipeRemarkableRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.pipe.processor.downsampling.DownSamplingProcessor;
import org.apache.iotdb.db.pipe.processor.downsampling.PartialPathLastObjectCache;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@TreeModel
public class SwingingDoorTrendingSamplingProcessor extends DownSamplingProcessor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SwingingDoorTrendingSamplingProcessor.class);

  /**
   * The maximum absolute difference the user set if the data's value is within
   * compressionDeviation, it will be compressed and discarded after compression, it will only store
   * out of range (time, data) to form the trend
   */
  private double compressionDeviation;

  private PartialPathLastObjectCache<SwingingDoorTrendingFilter> pathLastObjectCache;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final PipeParameters parameters = validator.getParameters();
    compressionDeviation =
        parameters.getDoubleOrDefault(
            PipeProcessorConstant.PROCESSOR_SDT_COMPRESSION_DEVIATION_KEY,
            PipeProcessorConstant.PROCESSOR_SDT_COMPRESSION_DEVIATION_DEFAULT_VALUE);
    eventTimeMinInterval =
        parameters.getLongOrDefault(
            Arrays.asList(
                PipeProcessorConstant.PROCESSOR_SDT_EVENT_TIME_MIN_INTERVAL,
                PipeProcessorConstant.PROCESSOR_SDT_MIN_TIME_INTERVAL_KEY),
            TimestampPrecisionUtils.convertToCurrPrecision(
                PipeProcessorConstant.PROCESSOR_SDT_EVENT_TIME_MIN_INTERVAL_DEFAULT_VALUE,
                TimeUnit.MILLISECONDS));
    eventTimeMaxInterval =
        parameters.getLongOrDefault(
            Arrays.asList(
                PipeProcessorConstant.PROCESSOR_SDT_EVENT_TIME_MAX_INTERVAL,
                PipeProcessorConstant.PROCESSOR_SDT_MAX_TIME_INTERVAL_KEY),
            TimestampPrecisionUtils.convertToCurrPrecision(
                PipeProcessorConstant.PROCESSOR_SDT_EVENT_TIME_MAX_INTERVAL_DEFAULT_VALUE,
                TimeUnit.MILLISECONDS));
    arrivalTimeMinInterval =
        parameters.getLongOrDefault(
            PipeProcessorConstant.PROCESSOR_SDT_ARRIVAL_TIME_MIN_INTERVAL,
            TimestampPrecisionUtils.convertToCurrPrecision(
                PipeProcessorConstant.PROCESSOR_SDT_ARRIVAL_TIME_MIN_INTERVAL_DEFAULT_VALUE,
                TimeUnit.MILLISECONDS));
    arrivalTimeMaxInterval =
        parameters.getLongOrDefault(
            PipeProcessorConstant.PROCESSOR_SDT_ARRIVAL_TIME_MAX_INTERVAL,
            TimestampPrecisionUtils.convertToCurrPrecision(
                PipeProcessorConstant.PROCESSOR_SDT_ARRIVAL_TIME_MAX_INTERVAL_DEFAULT_VALUE,
                TimeUnit.MILLISECONDS));

    validateTimeInterval(validator);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    super.customize(parameters, configuration);

    LOGGER.info(
        "SwingingDoorTrendingSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}, {}: {}, {}: {}.",
        dataBaseNameWithPathSeparator,
        PipeProcessorConstant.PROCESSOR_SDT_COMPRESSION_DEVIATION_KEY,
        compressionDeviation,
        PipeProcessorConstant.PROCESSOR_SDT_ARRIVAL_TIME_MIN_INTERVAL,
        arrivalTimeMinInterval,
        PipeProcessorConstant.PROCESSOR_SDT_ARRIVAL_TIME_MAX_INTERVAL,
        arrivalTimeMaxInterval,
        PipeProcessorConstant.PROCESSOR_SDT_EVENT_TIME_MIN_INTERVAL,
        eventTimeMinInterval,
        PipeProcessorConstant.PROCESSOR_SDT_EVENT_TIME_MAX_INTERVAL,
        eventTimeMaxInterval);
  }

  @Override
  protected void initPathLastObjectCache(final long memoryLimitInBytes) {
    pathLastObjectCache =
        new PartialPathLastObjectCache<SwingingDoorTrendingFilter>(memoryLimitInBytes) {
          @Override
          protected long calculateMemoryUsage(SwingingDoorTrendingFilter object) {
            return object.ramBytesUsed();
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
      final SwingingDoorTrendingFilter filter =
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
            new SwingingDoorTrendingFilter(
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
