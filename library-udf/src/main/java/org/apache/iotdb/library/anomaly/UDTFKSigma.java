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

package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.library.util.CircularQueue;
import org.apache.iotdb.library.util.LongCircularQueue;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** This function detects outliers which lies over average +/- k * sigma. */
public class UDTFKSigma implements UDTF {
  private double mean = 0.0;
  private double variance = 0.0;
  private double sumX2 = 0.0;
  private double sumX1 = 0.0;
  private double multipleK;
  private int windowSize = 0;
  private CircularQueue<Object> v;
  private LongCircularQueue t;
  private Type dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x > 0,
            "Window size should be larger than 0.",
            validator.getParameters().getIntOrDefault("window", 10))
        .validate(
            x -> (double) x > 0,
            "Parameter k should be larger than 0.",
            validator.getParameters().getDoubleOrDefault("k", 3));
  }

  @Override
  public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations)
      throws Exception {
    udtfConfigurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(udfParameters.getDataType(0));
    this.multipleK = udfParameters.getDoubleOrDefault("k", 3);
    this.dataType = udfParameters.getDataType(0);
    this.windowSize = udfParameters.getIntOrDefault("window", 10000);
    this.v = new CircularQueue<>(windowSize);
    this.t = new LongCircularQueue(windowSize);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double value = Util.getValueAsDouble(row);
    long timestamp = row.getTime();
    if (Double.isFinite(value) && !Double.isNaN(value)) {
      if (v.isFull()) {
        final double frontValue = Double.parseDouble(v.pop().toString());
        switch (dataType) {
          case INT32:
            v.push(row.getInt(0));
            break;
          case INT64:
            v.push(row.getLong(0));
            break;
          case DOUBLE:
            v.push(row.getDouble(0));
            break;
          case FLOAT:
            v.push(row.getFloat(0));
            break;
          case TIMESTAMP:
          case DATE:
          case TEXT:
          case STRING:
          case BLOB:
          case BOOLEAN:
          default:
            break;
        }
        t.pop();
        t.push(timestamp);
        this.sumX1 = this.sumX1 - frontValue + value;
        this.sumX2 = this.sumX2 - frontValue * frontValue + value * value;
        this.mean = this.sumX1 / v.getSize();
        this.variance = this.sumX2 / v.getSize() - this.mean * this.mean;
        if (Math.abs(value - mean)
            > multipleK * Math.sqrt(this.variance * v.getSize() / (v.getSize() - 1))) {
          Util.putValue(collector, dataType, timestamp, Util.getValueAsObject(row));
        }
      } else {
        switch (dataType) {
          case INT32:
            v.push(row.getInt(0));
            break;
          case INT64:
            v.push(row.getLong(0));
            break;
          case DOUBLE:
            v.push(row.getDouble(0));
            break;
          case FLOAT:
            v.push(row.getFloat(0));
            break;
          case BLOB:
          case BOOLEAN:
          case STRING:
          case TEXT:
          case DATE:
          case TIMESTAMP:
          default:
            break;
        }
        t.push(timestamp);
        this.sumX1 = this.sumX1 + value;
        this.sumX2 = this.sumX2 + value * value;
        this.mean = this.sumX1 / v.getSize();
        this.variance = this.sumX2 / v.getSize() - this.mean * this.mean;
        if (v.getSize() == this.windowSize) {
          double stddev = Math.sqrt(this.variance * v.getSize() / (v.getSize() - 1));
          for (int i = 0; i < v.getSize(); i++) {
            Object vi = this.v.get(i);
            timestamp = this.t.get(i);
            if (Math.abs(Double.parseDouble(vi.toString()) - mean) > multipleK * stddev) {
              Util.putValue(collector, dataType, timestamp, vi);
            }
          }
        }
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (!v.isFull() && v.getSize() > 1) {
      double stddev = Math.sqrt(this.variance * v.getSize() / (v.getSize() - 1));
      for (int i = 0; i < v.getSize(); i++) {
        Object vi = this.v.get(i);
        long timestamp = this.t.get(i);
        if (Math.abs(Double.parseDouble(vi.toString()) - mean) > multipleK * stddev) {
          Util.putValue(collector, dataType, timestamp, vi);
        }
      }
    }
  }
}
