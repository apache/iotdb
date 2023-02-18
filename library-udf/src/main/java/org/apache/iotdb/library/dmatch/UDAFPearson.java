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

package org.apache.iotdb.library.dmatch;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** This function calculates Pearson's r between two input series. */
public class UDAFPearson implements UDTF {

  private long count = 0;
  private double sum_x = 0.0;
  private double sum_y = 0.0;
  private double sum_xy = 0.0;
  private double sum_xx = 0.0;
  private double sum_yy = 0.0;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validateInputSeriesDataType(1, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    count = 0;
    sum_x = 0.0;
    sum_y = 0.0;
    sum_xy = 0.0;
    sum_xx = 0.0;
    sum_yy = 0.0;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0) || row.isNull(1)) { // skip null rows
      return;
    }
    double x = Util.getValueAsDouble(row, 0);
    double y = Util.getValueAsDouble(row, 1);
    if (Double.isFinite(x) && Double.isFinite(y)) { // skip NaN rows
      count++;
      sum_x += x;
      sum_y += y;
      sum_xy += x * y;
      sum_xx += x * x;
      sum_yy += y * y;
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (count > 0) { // calculate R only when there is more than 1 point
      double pearson =
          (count * sum_xy - sum_x * sum_y)
              / Math.sqrt(count * sum_xx - sum_x * sum_x)
              / Math.sqrt(count * sum_yy - sum_y * sum_y);
      collector.putDouble(0, pearson);
    } else {
      collector.putDouble(0, Double.NaN);
    }
  }
}
