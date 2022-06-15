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

import org.apache.iotdb.library.frequency.util.FFTUtil;
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
import org.jtransforms.fft.DoubleFFT_1D;

/** This function does Fast Fourier Transform for input series. */
public class UDTFFFT implements UDTF {

  private boolean compressed;
  private FFTUtil fftutil;
  private final DoubleArrayList list = new DoubleArrayList();

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x ->
                ((String) x).equalsIgnoreCase("uniform")
                    || ((String) x).equalsIgnoreCase("nonuniform"),
            "Type should be 'uniform' or 'nonuniform'.",
            validator.getParameters().getStringOrDefault("method", "uniform"))
        .validate(
            x ->
                "real".equalsIgnoreCase((String) x)
                    || "imag".equalsIgnoreCase((String) x)
                    || "abs".equalsIgnoreCase((String) x)
                    || "angle".equalsIgnoreCase((String) x),
            "Result should be 'real', 'imag', 'abs' or 'angle'.",
            validator.getParameters().getStringOrDefault("result", "abs"))
        .validate(
            x -> (double) x > 0 && (double) x <= 1,
            "Compress should be within (0,1].",
            validator.getParameters().getDoubleOrDefault("compress", 1));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    String result = parameters.getStringOrDefault("result", "abs");
    this.compressed = parameters.hasAttribute("compress");
    double compressRate = parameters.getDoubleOrDefault("compress", 1);
    this.fftutil = new FFTUtil(result, compressRate);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double v = Util.getValueAsDouble(row);
    if (Double.isFinite(v)) {
      list.add(v);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    int n = list.size();
    DoubleFFT_1D fft = new DoubleFFT_1D(n);
    // each data point count for 2 double values (re and im)
    double[] a = new double[2 * n];
    for (int i = 0; i < n; i++) {
      a[2 * i] = list.get(i);
      a[2 * i + 1] = 0;
    }
    fft.complexForward(a);
    if (compressed) {
      fftutil.outputCompressed(collector, a);
    } else {
      fftutil.outputUncompressed(collector, a);
    }
  }
}
