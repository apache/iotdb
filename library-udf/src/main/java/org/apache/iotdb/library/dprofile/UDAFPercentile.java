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
import org.apache.iotdb.library.util.Util;
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
  private Type dataType;

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
    dataType = parameters.getDataType(0);
    double error = parameters.getDoubleOrDefault("error", 0);
    rank = parameters.getDoubleOrDefault("rank", 0.5);
    exact = (error == 0);
    if (exact) {
      statistics = new ExactOrderStatistics(parameters.getDataType(0));
    } else {
      sketch = new GKArray(error);
    }
    switch (dataType) {
      case INT32:
        intDic = new HashMap<>();
        break;
      case INT64:
        longDic = new HashMap<>();
        break;
      case FLOAT:
        floatDic = new HashMap<>();
        break;
      case DOUBLE:
        doubleDic = new HashMap<>();
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
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (exact) {
      statistics.insert(row);
      switch (dataType) {
        case INT32:
          intDic.put(row.getInt(0), row.getTime());
          break;
        case INT64:
          longDic.put(row.getLong(0), row.getTime());
          break;
        case FLOAT:
          floatDic.put(row.getFloat(0), row.getTime());
          break;
        case DOUBLE:
          doubleDic.put(row.getDouble(0), row.getTime());
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
    } else {
      double res = Util.getValueAsDouble(row);
      sketch.insert(res);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (exact) {
      long time;
      switch (dataType) {
        case INT32:
          int ires = Integer.parseInt(statistics.getPercentile(rank));
          time = intDic.getOrDefault(ires, 0L);
          collector.putInt(time, ires);
          break;
        case INT64:
          long lres = Long.parseLong(statistics.getPercentile(rank));
          time = longDic.getOrDefault(lres, 0L);
          collector.putLong(time, lres);
          break;
        case FLOAT:
          float fres = Float.parseFloat(statistics.getPercentile(rank));
          time = floatDic.getOrDefault(fres, 0L);
          collector.putFloat(time, fres);
          break;
        case DOUBLE:
          double dres = Double.parseDouble(statistics.getPercentile(rank));
          time = doubleDic.getOrDefault(dres, 0L);
          collector.putDouble(time, dres);
          break;
        case DATE:
        case TIMESTAMP:
        case TEXT:
        case STRING:
        case BOOLEAN:
        case BLOB:
        default:
          break;
      }
    } else {
      double res = sketch.query(rank);
      switch (dataType) {
        case INT32:
          collector.putInt(0, (int) res);
          break;
        case INT64:
          collector.putLong(0, (long) res);
          break;
        case FLOAT:
          collector.putFloat(0, (float) res);
          break;
        case DOUBLE:
          collector.putDouble(0, res);
          break;
        case BOOLEAN:
        case BLOB:
        case STRING:
        case TEXT:
        case TIMESTAMP:
        case DATE:
        default:
          break;
      }
    }
  }
}
