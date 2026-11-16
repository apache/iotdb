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
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.util.Precision;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;

public class UDTFSpline implements UDTF {
  int samplePoints; // convert to is closest integer
  ArrayList<Long> timestamp = new ArrayList<>();
  ArrayList<Double> yDouble = new ArrayList<>();

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
    yDouble.clear();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      return;
    }
    double v = Util.getValueAsDouble(row);
    if (Double.isFinite(v)) {
      Long t = row.getTime();
      if (!timestamp.isEmpty() && t <= timestamp.get(timestamp.size() - 1)) {
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
      double coordinateScale = span.doubleValue();
      double[] y = ArrayUtils.toPrimitive(yDouble.toArray(new Double[0]));
      double[] widths = new double[timestamp.size() - 1];
      for (int i = 0; i < widths.length; i++) {
        widths[i] =
            BigInteger.valueOf(timestamp.get(i + 1))
                    .subtract(BigInteger.valueOf(timestamp.get(i)))
                    .doubleValue()
                / coordinateScale;
      }
      PolynomialFunction[] polynomials = interpolateAkima(widths, y);
      int outputPoints = samplePoints;
      if (span.compareTo(BigInteger.valueOf(samplePoints - 1L)) < 0) {
        outputPoints = span.intValueExact() + 1;
      }
      for (int i = 0; i < outputPoints; i++) {
        long outputTimestamp = getOutputTimestamp(i, outputPoints);
        double yhead;
        if (i == 0) {
          yhead = y[0];
        } else if (i == outputPoints - 1) {
          yhead = y[y.length - 1];
        } else {
          yhead = interpolateAtTimestamp(outputTimestamp, y, polynomials);
        }
        if (Double.isFinite(yhead)) {
          collector.putDouble(outputTimestamp, yhead);
        }
      }
    }
  }

  private PolynomialFunction[] interpolateAkima(double[] widths, double[] y) {
    int last = widths.length;
    double[] slopes = new double[last];
    double[] weights = new double[last];
    double[] derivatives = new double[y.length];
    for (int i = 0; i < last; i++) {
      slopes[i] = (y[i + 1] - y[i]) / widths[i];
      if (i > 0) {
        weights[i] = Math.abs(slopes[i] - slopes[i - 1]);
      }
    }
    for (int i = 2; i < last - 1; i++) {
      double previousWeight = weights[i - 1];
      double nextWeight = weights[i + 1];
      if (Precision.equals(previousWeight, 0.0) && Precision.equals(nextWeight, 0.0)) {
        derivatives[i] =
            (widths[i] * slopes[i - 1] + widths[i - 1] * slopes[i]) / (widths[i] + widths[i - 1]);
      } else {
        derivatives[i] =
            (nextWeight * slopes[i - 1] + previousWeight * slopes[i])
                / (nextWeight + previousWeight);
      }
    }
    // Use adjacent intervals directly so close timestamps remain distinct at both endpoints.
    double startCurvature = (slopes[1] - slopes[0]) / (widths[0] + widths[1]);
    derivatives[0] = slopes[0] - startCurvature * widths[0];
    derivatives[1] = slopes[0] + startCurvature * widths[0];
    double endCurvature =
        (slopes[last - 1] - slopes[last - 2]) / (widths[last - 1] + widths[last - 2]);
    derivatives[last - 1] = slopes[last - 2] + endCurvature * widths[last - 2];
    derivatives[last] = slopes[last - 1] + endCurvature * widths[last - 1];

    PolynomialFunction[] polynomials = new PolynomialFunction[last];
    for (int i = 0; i < last; i++) {
      double width = widths[i];
      double delta = y[i + 1] - y[i];
      double derivativeStart = derivatives[i] * width;
      double derivativeEnd = derivatives[i + 1] * width;
      polynomials[i] =
          new PolynomialFunction(
              new double[] {
                y[i],
                derivativeStart,
                3 * delta - 2 * derivativeStart - derivativeEnd,
                -2 * delta + derivativeStart + derivativeEnd
              });
    }
    return polynomials;
  }

  private double interpolateAtTimestamp(
      long outputTimestamp, double[] y, PolynomialFunction[] polynomials) {
    int position = Collections.binarySearch(timestamp, outputTimestamp);
    if (position >= 0) {
      return y[position];
    }
    int upper = -position - 1;
    int lower = upper - 1;
    BigInteger leftTimestamp = BigInteger.valueOf(timestamp.get(lower));
    BigInteger interval = BigInteger.valueOf(timestamp.get(upper)).subtract(leftTimestamp);
    double fraction =
        BigInteger.valueOf(outputTimestamp).subtract(leftTimestamp).doubleValue()
            / interval.doubleValue();
    if (fraction <= 0) {
      return y[lower];
    }
    if (fraction >= 1) {
      return y[upper];
    }
    return polynomials[lower].value(fraction);
  }

  private long getOutputTimestamp(int index, int outputPoints) {
    BigInteger start = BigInteger.valueOf(timestamp.get(0));
    BigInteger span = BigInteger.valueOf(timestamp.get(timestamp.size() - 1)).subtract(start);
    BigInteger denominator = BigInteger.valueOf(outputPoints - 1L);
    BigInteger offset =
        span.multiply(BigInteger.valueOf(index)).add(denominator.shiftRight(1)).divide(denominator);
    return start.add(offset).longValueExact();
  }
}
