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
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.jtransforms.fft.DoubleFFT_1D;

/** This function does Inverse Fast Fourier Transform for input series. */
public class UDTFIFFT implements UDTF {

  private static final String START_PARAM = "start";
  private final DoubleArrayList real = new DoubleArrayList();
  private final DoubleArrayList imag = new DoubleArrayList();
  private final IntArrayList time = new IntArrayList();

  private long start;
  private long interval;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validateInputSeriesDataType(1, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x -> Util.isPositiveTime((String) x, validator.getParameters()),
            "interval should be a time period whose unit is ms, s, m, h, d.",
            validator.getParameters().getStringOrDefault("interval", "1s"));
    if (validator.getParameters().hasAttribute(START_PARAM)) {
      validator.validate(
          x -> Util.isPositiveDateTime((String) x),
          "start should conform to the format yyyy-MM-dd HH:mm:ss.",
          validator.getParameters().getString(START_PARAM));
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    real.clear();
    imag.clear();
    time.clear();
    this.interval = Util.parseTime(parameters.getStringOrDefault("interval", "1s"), parameters);
    this.start = 0;
    if (parameters.hasAttribute(START_PARAM)) {
      this.start = Util.parseDateTime(parameters.getString(START_PARAM));
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.getTime() < 0 || row.getTime() >= Integer.MAX_VALUE / 2) {
      return;
    }
    if (!row.isNull(0) && !row.isNull(1)) {
      double realValue = Util.getValueAsDouble(row, 0);
      double imagValue = Util.getValueAsDouble(row, 1);
      if (!Double.isFinite(realValue) || !Double.isFinite(imagValue)) {
        return;
      }
      time.add((int) row.getTime());
      real.add(realValue);
      imag.add(imagValue);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (time.isEmpty()) {
      return;
    }
    int n = time.get(time.size() - 1) + 1;
    double[] a = new double[n * 2];
    for (int i = 0; i < time.size(); i++) {
      int k = time.get(i);
      a[k * 2] = real.get(i);
      a[k * 2 + 1] = imag.get(i);
      // conjugate
      if (k > 0) {
        k = n - k;
        a[k * 2] = real.get(i);
        a[k * 2 + 1] = -imag.get(i);
      }
    }
    DoubleFFT_1D fft = new DoubleFFT_1D(n);
    fft.complexInverse(a, true);
    for (int i = 0; i < n; i++) {
      double value = a[2 * i];
      if (Double.isFinite(value)) {
        try {
          collector.putDouble(Math.addExact(start, Math.multiplyExact((long) i, interval)), value);
        } catch (ArithmeticException ignored) {
          // skip timestamps that overflow the long range
        }
      }
    }
  }
}
