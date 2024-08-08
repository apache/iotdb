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

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** calculate the approximate percentile */
public class UDAFQuantile implements UDTF {
  private org.apache.iotdb.library.dprofile.util.HeapLongKLLSketch sketch;
  private double rank;
  private TSDataType dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            K -> (int) K >= 100,
            "Size K has to be greater or equal than 100.",
            validator.getParameters().getIntOrDefault("K", 800))
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
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    int K = parameters.getIntOrDefault("K", 800);
    rank = parameters.getDoubleOrDefault("rank", 0.5);

    sketch = new org.apache.iotdb.library.dprofile.util.HeapLongKLLSketch(K * 8);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double res = Util.getValueAsDouble(row);
    sketch.update(dataToLong(res));
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    long result = sketch.findMinValueWithRank((long) (rank * sketch.getN()));
    double res = longToResult(result);
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
    }
  }

  private long dataToLong(Object data) {
    long result;
    switch (dataType) {
      case INT32:
        return (int) data;
      case FLOAT:
        result = Float.floatToIntBits((float) data);
        return (float) data >= 0f ? result : result ^ Long.MAX_VALUE;
      case INT64:
        return (long) data;
      case DOUBLE:
        result = Double.doubleToLongBits((double) data);
        return (double) data >= 0d ? result : result ^ Long.MAX_VALUE;
      default:
        return (long) data;
    }
  }

  private double longToResult(long result) {
    switch (dataType) {
      case INT32:
        return (double) (result);
      case FLOAT:
        result = (result >>> 31) == 0 ? result : result ^ Long.MAX_VALUE;
        return Float.intBitsToFloat((int) (result));
      case INT64:
        return (double) (result);
      case DOUBLE:
        result = (result >>> 63) == 0 ? result : result ^ Long.MAX_VALUE;
        return Double.longBitsToDouble(result);
      default:
        return (double) (result);
    }
  }
}
