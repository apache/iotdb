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

  /**
   * The minimum time distance between two stored data points if current point time to the last
   * stored point time distance <= compressionMinTimeInterval, current point will NOT be stored
   * regardless of compression deviation
   */
  private long compressionMinTimeInterval;

  /**
   * The maximum time distance between two stored data points if current point time to the last
   * stored point time distance >= compressionMaxTimeInterval, current point will be stored
   * regardless of compression deviation
   */
  private long compressionMaxTimeInterval;

  private PartialPathLastObjectCache<SwingingDoorTrendingFilter<?>> pathLastObjectCache;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final PipeParameters parameters = validator.getParameters();
    compressionDeviation =
        parameters.getDoubleOrDefault(
            PipeProcessorConstant.PROCESSOR_SDT_COMPRESSION_DEVIATION_KEY,
            PipeProcessorConstant.PROCESSOR_SDT_COMPRESSION_DEVIATION_DEFAULT_VALUE);
    compressionMinTimeInterval =
        parameters.getLongOrDefault(
            PipeProcessorConstant.PROCESSOR_SDT_MIN_TIME_INTERVAL_KEY,
            PipeProcessorConstant.PROCESSOR_SDT_MIN_TIME_INTERVAL_DEFAULT_VALUE);
    compressionMaxTimeInterval =
        parameters.getLongOrDefault(
            PipeProcessorConstant.PROCESSOR_SDT_MAX_TIME_INTERVAL_KEY,
            PipeProcessorConstant.PROCESSOR_SDT_MAX_TIME_INTERVAL_DEFAULT_VALUE);

    validator
        .validate(
            compressionDeviation -> (Double) compressionDeviation >= 0,
            String.format(
                "%s must be >= 0, but got %s",
                PipeProcessorConstant.PROCESSOR_SDT_COMPRESSION_DEVIATION_KEY,
                compressionDeviation),
            compressionDeviation)
        .validate(
            compressionMinTimeInterval -> (Long) compressionMinTimeInterval >= 0,
            String.format(
                "%s must be >= 0, but got %s",
                PipeProcessorConstant.PROCESSOR_SDT_MIN_TIME_INTERVAL_KEY,
                compressionMinTimeInterval),
            compressionMinTimeInterval)
        .validate(
            compressionMaxTimeInterval -> (Long) compressionMaxTimeInterval >= 0,
            String.format(
                "%s must be >= 0, but got %s",
                PipeProcessorConstant.PROCESSOR_SDT_MAX_TIME_INTERVAL_KEY,
                compressionMaxTimeInterval),
            compressionMaxTimeInterval)
        .validate(
            minMaxPair -> (Long) minMaxPair[0] <= (Long) minMaxPair[1],
            String.format(
                "%s must be <= %s, but got %s and %s",
                PipeProcessorConstant.PROCESSOR_SDT_MIN_TIME_INTERVAL_KEY,
                PipeProcessorConstant.PROCESSOR_SDT_MAX_TIME_INTERVAL_KEY,
                compressionMinTimeInterval,
                compressionMaxTimeInterval),
            compressionMinTimeInterval,
            compressionMaxTimeInterval);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    super.customize(parameters, configuration);

    LOGGER.info(
        "SwingingDoorTrendingSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}.",
        dataBaseNameWithPathSeparator,
        PipeProcessorConstant.PROCESSOR_SDT_COMPRESSION_DEVIATION_KEY,
        compressionDeviation,
        PipeProcessorConstant.PROCESSOR_SDT_MIN_TIME_INTERVAL_KEY,
        compressionMinTimeInterval,
        PipeProcessorConstant.PROCESSOR_SDT_MAX_TIME_INTERVAL_KEY,
        compressionMaxTimeInterval);
  }

  @Override
  protected PartialPathLastObjectCache<?> initPathLastObjectCache(long memoryLimitInBytes) {
    pathLastObjectCache =
        new PartialPathLastObjectCache<SwingingDoorTrendingFilter<?>>(memoryLimitInBytes) {
          @Override
          protected long calculateMemoryUsage(SwingingDoorTrendingFilter<?> object) {
            return 64; // Long.BYTES * 8
          }
        };
    return pathLastObjectCache;
  }

  @Override
  protected void processRow(
      Row row,
      RowCollector rowCollector,
      String deviceSuffix,
      AtomicReference<Exception> exception) {
    final PipeRemarkableRow remarkableRow = new PipeRemarkableRow((PipeRow) row);

    boolean hasNonNullMeasurements = false;
    for (int i = 0, size = row.size(); i < size; i++) {
      if (row.isNull(i)) {
        continue;
      }

      final String timeSeriesSuffix =
          deviceSuffix + TsFileConstant.PATH_SEPARATOR + row.getColumnName(i);
      final SwingingDoorTrendingFilter filter =
          pathLastObjectCache.getPartialPathLastObject(timeSeriesSuffix);

      if (filter != null) {
        if (filter.filter(row.getTime(), row.getObject(i))) {
          hasNonNullMeasurements = true;
        } else {
          remarkableRow.markNull(i);
        }
      } else {
        hasNonNullMeasurements = true;
        pathLastObjectCache.setPartialPathLastObject(
            timeSeriesSuffix,
            new SwingingDoorTrendingFilter<>(this, row.getTime(), row.getObject(i)));
      }
    }

    if (hasNonNullMeasurements) {
      try {
        rowCollector.collectRow(remarkableRow);
      } catch (IOException e) {
        exception.set(e);
      }
    }
  }

  double getCompressionDeviation() {
    return compressionDeviation;
  }

  long getCompressionMinTimeInterval() {
    return compressionMinTimeInterval;
  }

  long getCompressionMaxTimeInterval() {
    return compressionMaxTimeInterval;
  }
}
