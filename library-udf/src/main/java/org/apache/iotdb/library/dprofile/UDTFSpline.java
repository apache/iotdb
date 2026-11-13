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

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
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

import java.math.BigInteger;
import java.util.ArrayList;

public class UDTFSpline implements UDTF {
  AkimaSplineInterpolator asi;
  int samplePoints; // convert to is closest integer
  ArrayList<Long> timestamp = new ArrayList<>();
  ArrayList<Double> yDouble = new ArrayList<>();
  ArrayList<Double> xDouble = new ArrayList<>();
  Long minimumTimestamp;
  PolynomialSplineFunction psf;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.FLOAT, Type.DOUBLE, Type.INT32, Type.INT64)
        .validateRequiredAttribute("points")
        .validate(
            x -> (int) x >= 2,
            LibraryUdfMessages.EXCEPTION_PARAMETER_POINTS_SHOULD_BE_AT_LEAST_2_A4F55048,
            validator.getParameters().getInt("points"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    samplePoints = parameters.getInt("points");
    timestamp.clear();
    xDouble.clear();
    yDouble.clear();
    minimumTimestamp = null;
    psf = null;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      return;
    }
    double v = Util.getValueAsDouble(row);
    if (Double.isFinite(v)) {
      Long t = row.getTime();
      if (minimumTimestamp == null) {
        minimumTimestamp = t;
      } else if (t <= timestamp.get(timestamp.size() - 1)) {
        return;
      }
      timestamp.add(t);
      yDouble.add(v);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (yDouble.size() >= 5 && samplePoints >= 2) {
      BigInteger start = BigInteger.valueOf(timestamp.get(0));
      BigInteger end = BigInteger.valueOf(timestamp.get(timestamp.size() - 1));
      BigInteger span = end.subtract(start);
      BigInteger coordinateOrigin = start.add(end).divide(BigInteger.TWO);
      double coordinateScale = span.doubleValue();
      ArrayList<Double> interpolationValues = new ArrayList<>();
      for (int i = 0; i < timestamp.size(); i++) {
        double coordinate =
            BigInteger.valueOf(timestamp.get(i)).subtract(coordinateOrigin).doubleValue()
                / coordinateScale;
        if (xDouble.isEmpty() || coordinate > xDouble.get(xDouble.size() - 1)) {
          xDouble.add(coordinate);
          interpolationValues.add(yDouble.get(i));
        } else if (i == timestamp.size() - 1) {
          // Keep the terminal value when adjacent timestamps share a double coordinate.
          interpolationValues.set(interpolationValues.size() - 1, yDouble.get(i));
        }
      }
      if (xDouble.size() < 5) {
        return;
      }
      asi = new AkimaSplineInterpolator();
      double[] x = ArrayUtils.toPrimitive(xDouble.toArray(new Double[0]));
      double[] y = ArrayUtils.toPrimitive(interpolationValues.toArray(new Double[0]));
      psf = asi.interpolate(x, y);
      int outputPoints = samplePoints;
      if (span.compareTo(BigInteger.valueOf(samplePoints - 1L)) < 0) {
        outputPoints = span.intValueExact() + 1;
      }
      for (int i = 0; i < outputPoints; i++) {
        long outputTimestamp = getOutputTimestamp(i, outputPoints);
        double approximation =
            BigInteger.valueOf(outputTimestamp).subtract(coordinateOrigin).doubleValue()
                / coordinateScale;
        double yhead;
        if (i == 0) {
          yhead = y[0];
        } else if (i == outputPoints - 1) {
          yhead = y[y.length - 1];
        } else {
          yhead = psf.value(approximation);
        }
        if (Double.isFinite(yhead)) {
          collector.putDouble(outputTimestamp, yhead);
        }
      }
    }
  }

  private long getOutputTimestamp(int index, int outputPoints) {
    BigInteger start = BigInteger.valueOf(minimumTimestamp);
    BigInteger span = BigInteger.valueOf(timestamp.get(timestamp.size() - 1)).subtract(start);
    BigInteger denominator = BigInteger.valueOf(outputPoints - 1L);
    BigInteger offset =
        span.multiply(BigInteger.valueOf(index)).add(denominator.shiftRight(1)).divide(denominator);
    return start.add(offset).longValueExact();
  }
}
