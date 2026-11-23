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
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.math.BigDecimal;
import java.math.MathContext;

/** This function calculates Covariance between two input series. */
public class UDAFCov implements UDTF {

  private long count = 0;
  private double maxAbsX = 0.0;
  private double maxAbsY = 0.0;
  private double scaleX = 0.0;
  private double scaleY = 0.0;
  private double meanX = 0.0;
  private double meanY = 0.0;
  private double coMoment = 0.0;
  private boolean scaledAccumulation;

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
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    count = 0;
    maxAbsX = 0;
    maxAbsY = 0;
    scaleX = 0;
    scaleY = 0;
    meanX = 0;
    meanY = 0;
    coMoment = 0;
    scaledAccumulation = false;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0) || row.isNull(1)) { // skip null rows
      return;
    }
    double x = Util.getValueAsDouble(row, 0);
    double y = Util.getValueAsDouble(row, 1);
    if (Double.isFinite(x) && Double.isFinite(y)) { // skip NaN rows
      if (!scaledAccumulation) {
        double nextCount = count + 1.0;
        double deltaX = x - meanX;
        double deltaY = y - meanY;
        double nextMeanY = meanY + deltaY / nextCount;
        double nextCoMoment = coMoment + deltaX * (y - nextMeanY);
        if (Double.isFinite(deltaX) && Double.isFinite(deltaY) && Double.isFinite(nextCoMoment)) {
          count++;
          meanX += deltaX / count;
          meanY = nextMeanY;
          coMoment = nextCoMoment;
          maxAbsX = Math.max(maxAbsX, Math.abs(x));
          maxAbsY = Math.max(maxAbsY, Math.abs(y));
          return;
        }
        initializeScaledAccumulation(x, y);
      }
      addScaledValue(x, y);
    }
  }

  private void initializeScaledAccumulation(double x, double y) {
    scaleX = Math.max(maxAbsX, Math.abs(x));
    scaleY = Math.max(maxAbsY, Math.abs(y));
    meanX = scaleX == 0 ? 0 : meanX / scaleX;
    meanY = scaleY == 0 ? 0 : meanY / scaleY;
    coMoment =
        scaleX == 0 || scaleY == 0
            ? 0
            : new BigDecimal(coMoment)
                .divide(new BigDecimal(scaleX), MathContext.DECIMAL128)
                .divide(new BigDecimal(scaleY), MathContext.DECIMAL128)
                .doubleValue();
    scaledAccumulation = true;
  }

  private void addScaledValue(double x, double y) {
    double absoluteX = Math.abs(x);
    if (absoluteX > scaleX) {
      double ratio = scaleX == 0 ? 0 : scaleX / absoluteX;
      meanX *= ratio;
      coMoment *= ratio;
      scaleX = absoluteX;
    }
    double absoluteY = Math.abs(y);
    if (absoluteY > scaleY) {
      double ratio = scaleY == 0 ? 0 : scaleY / absoluteY;
      meanY *= ratio;
      coMoment *= ratio;
      scaleY = absoluteY;
    }

    double normalizedX = scaleX == 0 ? 0 : x / scaleX;
    double normalizedY = scaleY == 0 ? 0 : y / scaleY;
    count++;
    double deltaX = normalizedX - meanX;
    double deltaY = normalizedY - meanY;
    meanX += deltaX / count;
    meanY += deltaY / count;
    coMoment += deltaX * (normalizedY - meanY);
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (count > 0) {
      double cov = coMoment / count;
      if (scaledAccumulation) {
        cov =
            new BigDecimal(coMoment)
                .multiply(new BigDecimal(scaleX))
                .multiply(new BigDecimal(scaleY))
                .divide(BigDecimal.valueOf(count), MathContext.DECIMAL128)
                .doubleValue();
      }
      if (Double.isFinite(cov)) {
        collector.putDouble(0, cov);
      }
    }
  }
}
