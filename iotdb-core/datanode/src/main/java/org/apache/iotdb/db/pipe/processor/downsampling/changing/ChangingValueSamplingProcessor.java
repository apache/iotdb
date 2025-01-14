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
public class ChangingValueSamplingProcessor extends DownSamplingProcessor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ChangingValueSamplingProcessor.class);

  /**
   * The maximum absolute difference the user set if the data's value is within
   * compressionDeviation, it will be compressed and discarded after compression
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

  private PartialPathLastObjectCache<ChangingValueFilter<?>> pathLastObjectCache;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final PipeParameters parameters = validator.getParameters();
    compressionDeviation =
        parameters.getDoubleOrDefault(
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION,
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION_DEFAULT_VALUE);
    compressionMinTimeInterval =
        parameters.getLongOrDefault(
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_KEY,
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_DEFAULT_VALUE);
    compressionMaxTimeInterval =
        parameters.getLongOrDefault(
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_KEY,
            PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_DEFAULT_VALUE);

    validator
        .validate(
            compressionDeviation -> (Double) compressionDeviation >= 0,
            String.format(
                "%s must be >= 0, but got %s",
                PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION,
                compressionDeviation),
            compressionDeviation)
        .validate(
            compressionMinTimeInterval -> (Long) compressionMinTimeInterval >= 0,
            String.format(
                "%s must be >= 0, but got %s",
                PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_KEY,
                compressionMinTimeInterval),
            compressionMinTimeInterval)
        .validate(
            compressionMaxTimeInterval -> (Long) compressionMaxTimeInterval >= 0,
            String.format(
                "%s must be >= 0, but got %s",
                PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_KEY,
                compressionMaxTimeInterval),
            compressionMaxTimeInterval)
        .validate(
            minMaxPair -> (Long) minMaxPair[0] <= (Long) minMaxPair[1],
            String.format(
                "%s must be <= %s, but got %s and %s",
                PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_KEY,
                PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_KEY,
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
        "ChangingValueSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}.",
        dataBaseNameWithPathSeparator,
        PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION,
        compressionDeviation,
        PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_KEY,
        compressionMinTimeInterval,
        PipeProcessorConstant.PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_KEY,
        compressionMaxTimeInterval);
  }

  @Override
  protected PartialPathLastObjectCache<?> initPathLastObjectCache(long memoryLimitInBytes) {
    pathLastObjectCache =
        new PartialPathLastObjectCache<ChangingValueFilter<?>>(memoryLimitInBytes) {
          @Override
          protected long calculateMemoryUsage(ChangingValueFilter<?> object) {
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
      final ChangingValueFilter filter =
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
            timeSeriesSuffix, new ChangingValueFilter<>(this, row.getTime(), row.getObject(i)));
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
