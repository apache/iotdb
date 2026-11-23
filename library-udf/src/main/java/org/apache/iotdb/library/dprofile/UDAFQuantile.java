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
import org.apache.iotdb.library.util.TypeServices;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

/** calculate the approximate percentile. */
public class UDAFQuantile implements UDTF {
  private org.apache.iotdb.library.dprofile.util.HeapLongKLLSketch sketch;
  private double rank;
  private QuantileOperations operations;

  private static final QuantileOperations INT_OPERATIONS =
      new QuantileOperations() {
        public long encode(Row row) throws IOException {
          return row.getInt(0);
        }

        public void write(long result, PointCollector collector) throws IOException {
          collector.putInt(0, (int) result);
        }
      };
  private static final QuantileOperations LONG_OPERATIONS =
      new QuantileOperations() {
        public long encode(Row row) throws IOException {
          return row.getLong(0);
        }

        public void write(long result, PointCollector collector) throws IOException {
          collector.putLong(0, result);
        }
      };
  private static final QuantileOperations FLOAT_OPERATIONS =
      new QuantileOperations() {
        public long encode(Row row) throws IOException {
          float value = row.getFloat(0);
          long bits = Float.floatToIntBits(value);
          return value >= 0f ? bits : bits ^ Long.MAX_VALUE;
        }

        public void write(long result, PointCollector collector) throws IOException {
          result = (result >>> 31) == 0 ? result : result ^ Long.MAX_VALUE;
          collector.putFloat(0, Float.intBitsToFloat((int) result));
        }
      };
  private static final QuantileOperations DOUBLE_OPERATIONS =
      new QuantileOperations() {
        public long encode(Row row) throws IOException {
          double value = row.getDouble(0);
          long bits = Double.doubleToLongBits(value);
          return value >= 0d ? bits : bits ^ Long.MAX_VALUE;
        }

        public void write(long result, PointCollector collector) throws IOException {
          result = (result >>> 63) == 0 ? result : result ^ Long.MAX_VALUE;
          collector.putDouble(0, Double.longBitsToDouble(result));
        }
      };
  private static final QuantileOperations UNSUPPORTED_OPERATIONS =
      new QuantileOperations() {
        public long encode(Row row) {
          throw new IllegalArgumentException(
              LibraryUdfMessages.EXCEPTION_UNSUPPORTED_DATA_TYPE_A8CA7BE7);
        }

        public void write(long result, PointCollector collector) {}
      };

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            k -> (int) k >= 100,
            LibraryUdfMessages.EXCEPTION_SIZE_K_HAS_TO_BE_GREATER_THAN_OR_EQUAL_TO_100_C514D1C3,
            validator.getParameters().getIntOrDefault("K", 800))
        .validate(
            rank -> (double) rank > 0 && (double) rank <= 1,
            LibraryUdfMessages
                .EXCEPTION_RANK_HAS_TO_BE_GREATER_THAN_0_AND_LESS_THAN_OR_EQUAL_TO_1_0F16AF94,
            validator.getParameters().getDoubleOrDefault("rank", 0.5));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(parameters.getDataType(0));
    Type dataType = parameters.getDataType(0);
    operations =
        TypeServices.numericService(
                INT_OPERATIONS,
                LONG_OPERATIONS,
                FLOAT_OPERATIONS,
                DOUBLE_OPERATIONS,
                UNSUPPORTED_OPERATIONS)
            .call(TypeServices.toReadType(dataType));
    int k = parameters.getIntOrDefault("K", 800);
    rank = parameters.getDoubleOrDefault("rank", 0.5);

    sketch = new org.apache.iotdb.library.dprofile.util.HeapLongKLLSketch(k * 8);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    sketch.update(operations.encode(row));
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    long n = sketch.getN();
    // Nearest-rank: k-th smallest uses getApproxRank (strictly-less-than count) in [0, n-1];
    // rank=1 must map to k=n-1, not k=n which is unreachable and can overshoot the max sample.
    long k = 0;
    if (n > 0) {
      k = (long) Math.ceil(rank * n) - 1;
      if (k < 0) {
        k = 0;
      } else if (k >= n) {
        k = n - 1;
      }
    }
    operations.write(sketch.findMinValueWithRank(k), collector);
  }

  private interface QuantileOperations {
    long encode(Row row) throws IOException;

    void write(long result, PointCollector collector) throws IOException;
  }
}
