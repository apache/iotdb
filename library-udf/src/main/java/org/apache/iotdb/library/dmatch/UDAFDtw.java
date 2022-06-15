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
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

/** This function calculates DTW distance between two input series. */
public class UDAFDtw implements UDTF {

  private double[][] dp;
  private int m;

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
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
        .setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    DoubleArrayList a = new DoubleArrayList();
    DoubleArrayList b = new DoubleArrayList();
    int n = rowWindow.windowSize();
    for (int i = 0; i < n; i++) {
      Row row = rowWindow.getRow(i);
      if (row.isNull(0) || row.isNull(1)) {
        continue;
      }
      a.add(Util.getValueAsDouble(row, 0));
      b.add(Util.getValueAsDouble(row, 1));
    }
    m = a.size();
    dp = new double[m + 1][m + 1];
    for (int i = 1; i <= m; i++) {
      dp[0][i] = dp[i][0] = Double.MAX_VALUE;
    }
    dp[0][0] = 0;
    for (int i = 1; i <= m; i++) {
      for (int j = 1; j <= m; j++) {
        dp[i][j] =
            Math.abs(a.get(i - 1) - b.get(j - 1))
                + Math.min(Math.min(dp[i][j - 1], dp[i - 1][j]), dp[i - 1][j - 1]);
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    collector.putDouble(0, dp[m][m]);
  }
}
