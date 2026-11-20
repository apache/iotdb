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

package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/**
 * This function is used to calculate the distribution histogram of a single column of numerical
 * data.
 */
public class UDTFHistogram implements UDTF {

  private int[] bucket;
  private double gap;
  private int count;
  private double start;
  private double end;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            count -> (int) count > 0,
            LibraryUdfMessages.EXCEPTION_PARAMETER_COUNT_SHOULD_BE_LARGER_THAN_0_8E8FFF68,
            validator.getParameters().getIntOrDefault("count", 1))
        .validate(
            params -> (double) params[0] <= (double) params[1],
            LibraryUdfMessages
                .EXCEPTION_PARAMETER_END_SHOULD_BE_LARGER_THAN_OR_EQUAL_TO_START_FBDBED0B,
            validator.getParameters().getDoubleOrDefault("min", -Double.MAX_VALUE),
            validator.getParameters().getDoubleOrDefault("max", Double.MAX_VALUE));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.INT32);
    // input param min is var start, param max is var end
    start = parameters.getDoubleOrDefault("min", -Double.MAX_VALUE);
    end = parameters.getDoubleOrDefault("max", Double.MAX_VALUE);
    count = parameters.getIntOrDefault("count", 1);
    bucket = new int[count];
    gap = (end - start) / count;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double value = Util.getValueAsDouble(row);
    if (Double.isFinite(value)) {
      bucket[bucketId(value)]++;
    }
  }

  /**
   * Locate the bucket the given value belongs to. When (end - start) does not overflow, the
   * original uniform binning is kept. When it overflows to +/-Infinity (e.g. the default min/max
   * range), both (value - start) and the bucket width lose their meaning in double arithmetic, so a
   * numerically stable relative position derived from the interval center is used instead; this
   * keeps the mapping monotonic and every bucket reachable.
   */
  private int bucketId(double value) {
    if (!Double.isInfinite(gap)) {
      return Math.min(Math.max((int) Math.floor((value - start) / gap), 0), count - 1);
    }
    double center = start / 2 + end / 2;
    double halfSpan = end / 2 - start / 2;
    if (!(halfSpan > 0)) {
      // degenerate range (start == end); mirror the old clamping behavior
      return value <= start ? 0 : count - 1;
    }
    double position = 0.5 + 0.5 * ((value - center) / halfSpan);
    if (!(position >= 0)) {
      return 0; // NaN or below the lower bound
    }
    if (position >= 1) {
      return count - 1;
    }
    return Math.min((int) Math.floor(position * count), count - 1);
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    for (int i = 0; i < count; i++) {
      collector.putInt(i, bucket[i]);
    }
  }
}
