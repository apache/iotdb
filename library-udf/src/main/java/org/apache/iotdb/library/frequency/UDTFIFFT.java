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

import java.text.SimpleDateFormat;

/** This function does Inverse Fast Fourier Transform for input series. */
public class UDTFIFFT implements UDTF {

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
        .validate(
            x -> (long) x > 0,
            "interval should be a time period whose unit is ms, s, m, h, d.",
            Util.parseTime(validator.getParameters().getStringOrDefault("interval", "1s")));
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    if (validator.getParameters().hasAttribute("start")) {
      validator.validate(
          x -> (long) x > 0,
          "start should conform to the format yyyy-MM-dd HH:mm:ss.",
          format.parse(validator.getParameters().getString("start")).getTime());
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    this.interval = Util.parseTime(parameters.getStringOrDefault("interval", "1s"));
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    this.start = 0;
    if (parameters.hasAttribute("start")) {
      this.start = format.parse(parameters.getString("start")).getTime();
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!row.isNull(0)
        && !row.isNull(1)
        && Double.isFinite(Util.getValueAsDouble(row, 0))
        && Double.isFinite(Util.getValueAsDouble(row, 1))) {
      time.add((int) row.getTime());
      real.add(Util.getValueAsDouble(row, 0));
      imag.add(Util.getValueAsDouble(row, 1));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    int n = time.get(time.size() - 1) + 1;
    double a[] = new double[n * 2];
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
      collector.putDouble(start + i * interval, a[2 * i]);
    }
  }
}
