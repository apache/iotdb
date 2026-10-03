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

import org.apache.iotdb.library.dprofile.util.ExactOrderStatistics;
import org.apache.iotdb.library.dprofile.util.GKArray;
import org.apache.iotdb.library.util.TypeServices;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/** calculate the approximate percentile. */
public class UDAFPercentile implements UDTF {
  protected Map<Integer, Long> intDic;
  protected Map<Long, Long> longDic;
  protected Map<Float, Long> floatDic;
  protected Map<Double, Long> doubleDic;
  private ExactOrderStatistics statistics;
  private GKArray sketch;
  private boolean exact;
  private double rank;
  private TypeServices.NumericRowReader rowReader;
  private PercentileOperations operations;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            error -> (double) error >= 0 && (double) error < 1,
            "error has to be greater than or equal to 0 and less than 1.",
            validator.getParameters().getDoubleOrDefault("error", 0))
        .validate(
            rank -> (double) rank > 0 && (double) rank <= 1,
            "rank has to be greater than 0 and less than or equal to 1.",
            validator.getParameters().getDoubleOrDefault("rank", 0.5));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(parameters.getDataType(0));
    Type dataType = parameters.getDataType(0);
    org.apache.tsfile.read.common.type.Type type = TypeServices.toReadType(dataType);
    rowReader = TypeServices.NUMERIC_ROW_READER_SERVICE.call(type);
    operations =
        TypeServices.numericService(
                INT_OPERATIONS,
                LONG_OPERATIONS,
                FLOAT_OPERATIONS,
                DOUBLE_OPERATIONS,
                UNSUPPORTED_OPERATIONS)
            .call(type);
    double error = parameters.getDoubleOrDefault("error", 0);
    rank = parameters.getDoubleOrDefault("rank", 0.5);
    exact = (error == 0);
    if (exact) {
      statistics = new ExactOrderStatistics(parameters.getDataType(0));
    } else {
      sketch = new GKArray(error);
    }
    operations.initialize(this);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      return;
    }
    if (exact) {
      statistics.insert(row);
      operations.insertExact(this, row);
    } else {
      double value = rowReader.read(row);
      if (Double.isFinite(value)) {
        sketch.insert(value);
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    try {
      if (exact) {
        operations.writeExact(this, collector);
      } else {
        double res = sketch.query(rank);
        operations.writeApprox(res, collector);
      }
    } catch (NoSuchElementException | ArithmeticException e) {
      // Empty inputs have no percentile to emit.
    }
  }

  private static final PercentileOperations INT_OPERATIONS =
      new PercentileOperations() {
        public void initialize(UDAFPercentile target) {
          target.intDic = new HashMap<>();
        }

        public void insertExact(UDAFPercentile target, Row row) throws Exception {
          target.intDic.put(row.getInt(0), row.getTime());
        }

        public void writeExact(UDAFPercentile target, PointCollector collector) throws Exception {
          int value = Integer.parseInt(target.statistics.getPercentile(target.rank));
          collector.putInt(target.intDic.getOrDefault(value, 0L), value);
        }

        public void writeApprox(double value, PointCollector collector) throws Exception {
          collector.putInt(0, (int) value);
        }
      };
  private static final PercentileOperations LONG_OPERATIONS =
      new PercentileOperations() {
        public void initialize(UDAFPercentile target) {
          target.longDic = new HashMap<>();
        }

        public void insertExact(UDAFPercentile target, Row row) throws Exception {
          target.longDic.put(row.getLong(0), row.getTime());
        }

        public void writeExact(UDAFPercentile target, PointCollector collector) throws Exception {
          long value = Long.parseLong(target.statistics.getPercentile(target.rank));
          collector.putLong(target.longDic.getOrDefault(value, 0L), value);
        }

        public void writeApprox(double value, PointCollector collector) throws Exception {
          collector.putLong(0, (long) value);
        }
      };
  private static final PercentileOperations FLOAT_OPERATIONS =
      new PercentileOperations() {
        public void initialize(UDAFPercentile target) {
          target.floatDic = new HashMap<>();
        }

        public void insertExact(UDAFPercentile target, Row row) throws Exception {
          float value = row.getFloat(0);
          if (Float.isFinite(value)) {
            target.floatDic.put(value, row.getTime());
          }
        }

        public void writeExact(UDAFPercentile target, PointCollector collector) throws Exception {
          float value = Float.parseFloat(target.statistics.getPercentile(target.rank));
          collector.putFloat(target.floatDic.getOrDefault(value, 0L), value);
        }

        public void writeApprox(double value, PointCollector collector) throws Exception {
          collector.putFloat(0, (float) value);
        }
      };
  private static final PercentileOperations DOUBLE_OPERATIONS =
      new PercentileOperations() {
        public void initialize(UDAFPercentile target) {
          target.doubleDic = new HashMap<>();
        }

        public void insertExact(UDAFPercentile target, Row row) throws Exception {
          double value = row.getDouble(0);
          if (Double.isFinite(value)) {
            target.doubleDic.put(value, row.getTime());
          }
        }

        public void writeExact(UDAFPercentile target, PointCollector collector) throws Exception {
          double value = Double.parseDouble(target.statistics.getPercentile(target.rank));
          collector.putDouble(target.doubleDic.getOrDefault(value, 0L), value);
        }

        public void writeApprox(double value, PointCollector collector) throws Exception {
          collector.putDouble(0, value);
        }
      };
  private static final PercentileOperations UNSUPPORTED_OPERATIONS =
      new PercentileOperations() {
        public void initialize(UDAFPercentile target) {}

        public void insertExact(UDAFPercentile target, Row row) {}

        public void writeExact(UDAFPercentile target, PointCollector collector) {}

        public void writeApprox(double value, PointCollector collector) {}
      };

  private interface PercentileOperations {
    void initialize(UDAFPercentile target);

    void insertExact(UDAFPercentile target, Row row) throws Exception;

    void writeExact(UDAFPercentile target, PointCollector collector) throws Exception;

    void writeApprox(double value, PointCollector collector) throws Exception;
  }
}
