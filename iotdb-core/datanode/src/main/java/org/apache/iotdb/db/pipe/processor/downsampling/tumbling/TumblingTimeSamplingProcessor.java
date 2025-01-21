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

package org.apache.iotdb.db.pipe.processor.downsampling.tumbling;

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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_TUMBLING_TIME_INTERVAL_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_TUMBLING_TIME_INTERVAL_SECONDS_KEY;

@TreeModel
public class TumblingTimeSamplingProcessor extends DownSamplingProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TumblingTimeSamplingProcessor.class);

  private long intervalInCurrentPrecision;

  private PartialPathLastObjectCache<Long> pathLastObjectCache;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final long intervalSeconds =
        validator
            .getParameters()
            .getLongOrDefault(
                PROCESSOR_TUMBLING_TIME_INTERVAL_SECONDS_KEY,
                PROCESSOR_TUMBLING_TIME_INTERVAL_SECONDS_DEFAULT_VALUE);
    validator.validate(
        seconds -> (Long) seconds > 0,
        String.format(
            "The value of %s must be greater than 0, but got %d.",
            PROCESSOR_TUMBLING_TIME_INTERVAL_SECONDS_KEY, intervalSeconds),
        intervalSeconds);
    intervalInCurrentPrecision =
        TimestampPrecisionUtils.convertToCurrPrecision(intervalSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    super.customize(parameters, configuration);

    LOGGER.info(
        "TumblingTimeSamplingProcessor in {} is initialized with {}: {}s, {}: {}, {}: {}.",
        dataBaseNameWithPathSeparator,
        PROCESSOR_TUMBLING_TIME_INTERVAL_SECONDS_KEY,
        intervalInCurrentPrecision,
        PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY,
        memoryLimitInBytes,
        PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY,
        shouldSplitFile);
  }

  @Override
  protected PartialPathLastObjectCache<?> initPathLastObjectCache(long memoryLimitInBytes) {
    pathLastObjectCache =
        new PartialPathLastObjectCache<Long>(memoryLimitInBytes) {
          @Override
          protected long calculateMemoryUsage(Long object) {
            return Long.BYTES;
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
    for (int index = 0, size = row.size(); index < size; ++index) {
      if (row.isNull(index)) {
        continue;
      }

      final String timeSeriesSuffix =
          deviceSuffix + TsFileConstant.PATH_SEPARATOR + row.getColumnName(index);
      final long currentRowTime = row.getTime();
      final Long lastSampleTime = pathLastObjectCache.getPartialPathLastObject(timeSeriesSuffix);

      if (lastSampleTime == null
          || Math.abs(currentRowTime - lastSampleTime) >= intervalInCurrentPrecision) {
        try {
          rowCollector.collectRow(row);

          pathLastObjectCache.setPartialPathLastObject(timeSeriesSuffix, currentRowTime);
          for (int j = index + 1; j < size; ++j) {
            if (!row.isNull(j)) {
              pathLastObjectCache.setPartialPathLastObject(
                  deviceSuffix + TsFileConstant.PATH_SEPARATOR + row.getColumnName(j),
                  currentRowTime);
            }
          }
          return;
        } catch (Exception e) {
          exception.set(e);
        }
      }
    }
  }
}
