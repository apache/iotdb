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

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.jtransforms.fft.DoubleFFT_1D;

/** This function filters high frequency components. */
public class UDTFHighPass implements UDTF {

  private double wpass;
  private final DoubleArrayList valueList = new DoubleArrayList();
  private final LongArrayList timeList = new LongArrayList();

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validateRequiredAttribute("wpass")
        .validate(
            x -> (double) x > 0 && (double) x < 1,
            "Wpass should be within (0,1).",
            validator.getParameters().getDouble("wpass"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    this.wpass = parameters.getDouble("wpass");
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double v = Util.getValueAsDouble(row);
    if (Double.isFinite(v)) {
      valueList.add(v);
      timeList.add(row.getTime());
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    int n = valueList.size();
    DoubleFFT_1D fft = new DoubleFFT_1D(n);
    // each data point count for 2 double values, same with UDTFFFT
    double[] a = new double[2 * n];
    for (int i = 0; i < n; i++) {
      a[2 * i] = valueList.get(i);
      a[2 * i + 1] = 0;
    }
    fft.complexForward(a);
    // remove low frequency components
    int m = (int) Math.floor(wpass * n / 2);
    for (int i = 0; i <= 2 * m + 1; i++) {
      a[i] = 0;
    }
    for (int i = 2 * (n - m); i < 2 * n; i++) {
      a[i] = 0;
    }
    fft.complexInverse(a, true);
    // output
    for (int i = 0; i < n; i++) {
      collector.putDouble(timeList.get(i), a[i * 2]);
    }
  }
}
