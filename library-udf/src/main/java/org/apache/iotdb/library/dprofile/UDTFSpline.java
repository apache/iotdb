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
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.util.Precision;

import java.math.BigInteger;
import java.util.ArrayList;

public class UDTFSpline implements UDTF {
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
      double coordinateScale = span.doubleValue();
      BigInteger coordinateOrigin = selectCoordinateOrigin(start, end, coordinateScale);
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
      double[] x = ArrayUtils.toPrimitive(xDouble.toArray(new Double[0]));
      double[] y = ArrayUtils.toPrimitive(interpolationValues.toArray(new Double[0]));
      psf = interpolateAkima(x, y);
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

  private BigInteger selectCoordinateOrigin(BigInteger start, BigInteger end, double scale) {
    BigInteger midpoint = start.add(end).divide(BigInteger.TWO);
    BigInteger bestOrigin = midpoint;
    int mostDistinctCoordinates = 0;
    // A centered origin preserves close interior timestamps, while an endpoint origin can
    // preserve a dense cluster near that endpoint. Keep the origin that loses the fewest knots.
    for (BigInteger origin : new BigInteger[] {midpoint, start, end}) {
      int distinctCoordinates = 0;
      double previous = Double.NEGATIVE_INFINITY;
      for (long time : timestamp) {
        double coordinate = BigInteger.valueOf(time).subtract(origin).doubleValue() / scale;
        if (coordinate > previous) {
          previous = coordinate;
          distinctCoordinates++;
        }
      }
      if (distinctCoordinates > mostDistinctCoordinates) {
        bestOrigin = origin;
        mostDistinctCoordinates = distinctCoordinates;
      }
      if (distinctCoordinates == timestamp.size()) {
        break;
      }
    }
    return bestOrigin;
  }

  private PolynomialSplineFunction interpolateAkima(double[] x, double[] y) {
    int last = x.length - 1;
    double[] widths = new double[last];
    double[] slopes = new double[last];
    double[] weights = new double[last];
    double[] derivatives = new double[x.length];
    for (int i = 0; i < last; i++) {
      widths[i] = x[i + 1] - x[i];
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
            (widths[i] * slopes[i - 1] + widths[i - 1] * slopes[i]) / (x[i + 1] - x[i - 1]);
      } else {
        derivatives[i] =
            (nextWeight * slopes[i - 1] + previousWeight * slopes[i])
                / (nextWeight + previousWeight);
      }
    }
    // Keep the Akima three-point endpoint derivatives, using divided differences. Subtracting
    // squared distances from the first knot can round to zero for a distant knot and a close pair.
    double startCurvature = (slopes[1] - slopes[0]) / (x[2] - x[0]);
    derivatives[0] = slopes[0] - startCurvature * widths[0];
    derivatives[1] = slopes[0] + startCurvature * widths[0];
    double endCurvature = (slopes[last - 1] - slopes[last - 2]) / (x[last] - x[last - 2]);
    derivatives[last - 1] = slopes[last - 2] + endCurvature * widths[last - 2];
    derivatives[last] = slopes[last - 1] + endCurvature * widths[last - 1];

    PolynomialFunction[] polynomials = new PolynomialFunction[last];
    for (int i = 0; i < last; i++) {
      double width = widths[i];
      polynomials[i] =
          new PolynomialFunction(
              new double[] {
                y[i],
                derivatives[i],
                (3 * slopes[i] - 2 * derivatives[i] - derivatives[i + 1]) / width,
                (-2 * slopes[i] + derivatives[i] + derivatives[i + 1]) / (width * width)
              });
    }
    return new PolynomialSplineFunction(x, polynomials);
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
