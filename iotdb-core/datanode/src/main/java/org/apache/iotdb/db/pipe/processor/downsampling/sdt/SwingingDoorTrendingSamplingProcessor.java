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
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class SwingingDoorTrendingSamplingProcessor extends DownSamplingProcessor {
  private double compressionDeviation;
  private long compressionMinTimeInterval;
  private long compressionMaxTimeInterval;

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    super.customize(parameters, configuration);
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
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {}

  @Override
  protected void processRow(
      Row row,
      RowCollector rowCollector,
      String deviceSuffix,
      AtomicReference<Exception> exception) {
    boolean hasNonNullMeasurements = false;
    PipeRemarkableRow remarkableRow = new PipeRemarkableRow((PipeRow) row);

    for (int i = 0; i < row.size(); i++) {
      if (row.isNull(i)) {
        continue;
      }

      final String timeSeriesSuffix =
          deviceSuffix + TsFileConstant.PATH_SEPARATOR + row.getColumnName(i);
      final SwingingDoorTrendingFilter filter =
          (SwingingDoorTrendingFilter)
              pathLastObjectCache.getPartialPathLastObject(timeSeriesSuffix);

      if (filter != null) {
        if (filter.shouldStore(row.getTime(), row.getObject(i))) {
          hasNonNullMeasurements = true;
        } else {
          remarkableRow.markNull(i);
        }
      } else {
        hasNonNullMeasurements = true;
        pathLastObjectCache.setPartialPathLastObject(
            timeSeriesSuffix,
            new SwingingDoorTrendingFilter<>(this, row.getTime(), getValue(row, i)));
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

  public double getCompressionDeviation() {
    return compressionDeviation;
  }

  public long getCompressionMinTimeInterval() {
    return compressionMinTimeInterval;
  }

  public long getCompressionMaxTimeInterval() {
    return compressionMaxTimeInterval;
  }

  private Object getValue(Row row, int index) {
    switch (row.getDataType(index)) {
      case INT32:
        return row.getInt(index);
      case INT64:
        return row.getLong(index);
      case FLOAT:
        return row.getFloat(index);
      case DOUBLE:
        return row.getDouble(index);
      case TEXT:
        return row.getString(index);
      case BOOLEAN:
        return row.getBoolean(index);
    }
    return null;
  }
}
