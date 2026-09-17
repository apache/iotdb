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

/** calculate the approximate percentile. */
public class UDAFPercentile implements UDTF {
  protected static Map<Integer, Long> intDic;
  protected static Map<Long, Long> longDic;
  protected static Map<Float, Long> floatDic;
  protected static Map<Double, Long> doubleDic;
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
    operations.initialize();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (exact) {
      statistics.insert(row);
      operations.insertExact(row);
    } else {
      sketch.insert(rowReader.read(row));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (exact) {
      operations.writeExact(this, collector);
    } else {
      double res = sketch.query(rank);
      operations.writeApprox(res, collector);
    }
  }

  private static final PercentileOperations INT_OPERATIONS =
      new PercentileOperations() {
        public void initialize() {
          intDic = new HashMap<>();
        }

        public void insertExact(Row row) throws Exception {
          intDic.put(row.getInt(0), row.getTime());
        }

        public void writeExact(UDAFPercentile target, PointCollector collector) throws Exception {
          int value = Integer.parseInt(target.statistics.getPercentile(target.rank));
          collector.putInt(intDic.getOrDefault(value, 0L), value);
        }

        public void writeApprox(double value, PointCollector collector) throws Exception {
          collector.putInt(0, (int) value);
        }
      };
  private static final PercentileOperations LONG_OPERATIONS =
      new PercentileOperations() {
        public void initialize() {
          longDic = new HashMap<>();
        }

        public void insertExact(Row row) throws Exception {
          longDic.put(row.getLong(0), row.getTime());
        }

        public void writeExact(UDAFPercentile target, PointCollector collector) throws Exception {
          long value = Long.parseLong(target.statistics.getPercentile(target.rank));
          collector.putLong(longDic.getOrDefault(value, 0L), value);
        }

        public void writeApprox(double value, PointCollector collector) throws Exception {
          collector.putLong(0, (long) value);
        }
      };
  private static final PercentileOperations FLOAT_OPERATIONS =
      new PercentileOperations() {
        public void initialize() {
          floatDic = new HashMap<>();
        }

        public void insertExact(Row row) throws Exception {
          floatDic.put(row.getFloat(0), row.getTime());
        }

        public void writeExact(UDAFPercentile target, PointCollector collector) throws Exception {
          float value = Float.parseFloat(target.statistics.getPercentile(target.rank));
          collector.putFloat(floatDic.getOrDefault(value, 0L), value);
        }

        public void writeApprox(double value, PointCollector collector) throws Exception {
          collector.putFloat(0, (float) value);
        }
      };
  private static final PercentileOperations DOUBLE_OPERATIONS =
      new PercentileOperations() {
        public void initialize() {
          doubleDic = new HashMap<>();
        }

        public void insertExact(Row row) throws Exception {
          doubleDic.put(row.getDouble(0), row.getTime());
        }

        public void writeExact(UDAFPercentile target, PointCollector collector) throws Exception {
          double value = Double.parseDouble(target.statistics.getPercentile(target.rank));
          collector.putDouble(doubleDic.getOrDefault(value, 0L), value);
        }

        public void writeApprox(double value, PointCollector collector) throws Exception {
          collector.putDouble(0, value);
        }
      };
  private static final PercentileOperations UNSUPPORTED_OPERATIONS =
      new PercentileOperations() {
        public void initialize() {}

        public void insertExact(Row row) {}

        public void writeExact(UDAFPercentile target, PointCollector collector) {}

        public void writeApprox(double value, PointCollector collector) {}
      };

  private interface PercentileOperations {
    void initialize();

    void insertExact(Row row) throws Exception;

    void writeExact(UDAFPercentile target, PointCollector collector) throws Exception;

    void writeApprox(double value, PointCollector collector) throws Exception;
  }
}
