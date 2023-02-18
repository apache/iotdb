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

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.analysis.interpolation.AkimaSplineInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

import java.util.ArrayList;

public class UDTFSpline implements UDTF {
  AkimaSplineInterpolator asi;
  int samplePoints; // convert to is closest integer
  ArrayList<Long> timestamp = new ArrayList<>();
  ArrayList<Double> yDouble = new ArrayList<>();
  ArrayList<Double> xDouble = new ArrayList<>();
  Long minimumTimestamp = -1L;
  PolynomialSplineFunction psf;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.FLOAT, Type.DOUBLE, Type.INT32, Type.INT64);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    samplePoints = parameters.getInt("points");
    timestamp.clear();
    xDouble.clear();
    yDouble.clear();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double v = Util.getValueAsDouble(row);
    if (Double.isFinite(v)) {
      Long t = row.getTime();
      if (minimumTimestamp < 0) {
        minimumTimestamp = t;
      }
      timestamp.add(t);
      xDouble.add((Double.valueOf(Long.toString(t - minimumTimestamp))));
      yDouble.add(Util.getValueAsDouble(row));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (yDouble.size() >= 4 && samplePoints >= 2) { // 4个点以上才进行插值
      asi = new AkimaSplineInterpolator();
      double[] x = ArrayUtils.toPrimitive(xDouble.toArray(new Double[0]));
      double[] y = ArrayUtils.toPrimitive(yDouble.toArray(new Double[0]));
      psf = asi.interpolate(x, y);
      for (int i = 0; i < samplePoints; i++) {
        int approximation =
            (int)
                Math.floor(
                    (x[0] * (samplePoints - 1 - i) + x[yDouble.size() - 1] * (i))
                            / (samplePoints - 1)
                        + 0.5);
        double yhead = psf.value(approximation);
        collector.putDouble(minimumTimestamp + (long) approximation, yhead);
      }
    }
  }
}
