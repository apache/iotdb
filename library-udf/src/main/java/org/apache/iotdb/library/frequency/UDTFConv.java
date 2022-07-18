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

package org.apache.iotdb.library.frequency;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;

/** This function calculates convolution of two input series. */
public class UDTFConv implements UDTF {

  private final ArrayList<Double> list1 = new ArrayList<>();
  private final ArrayList<Double> list2 = new ArrayList<>();

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validateInputSeriesDataType(1, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    list1.clear();
    list2.clear();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!row.isNull(0) && Double.isFinite(Util.getValueAsDouble(row, 0))) {
      list1.add(Util.getValueAsDouble(row, 0));
    }
    if (!row.isNull(1) && Double.isFinite(Util.getValueAsDouble(row, 1))) {
      list2.add(Util.getValueAsDouble(row, 1));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    double[] ans = new double[list1.size() + list2.size() - 1];
    for (int i = 0; i < list1.size(); i++) {
      for (int j = 0; j < list2.size(); j++) {
        ans[i + j] += list1.get(i) * list2.get(j);
      }
    }
    for (int i = 0; i < ans.length; i++) {
      collector.putDouble(i, ans[i]);
    }
  }
}
