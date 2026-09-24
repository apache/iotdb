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

package org.apache.iotdb.library.drepair;

import org.apache.iotdb.library.drepair.util.LsGreedy;
import org.apache.iotdb.library.drepair.util.Screen;
import org.apache.iotdb.library.drepair.util.ValueRepair;
import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.library.util.TypeServices;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

/** This function is used to repair the value of the time series. */
public class UDTFValueRepair implements UDTF {
  private static final String METHOD_SCREEN = "screen";
  private static final String METHOD_LS_GREEDY = "lsgreedy";

  String method;
  double minSpeed;
  double maxSpeed;
  double center;
  double sigma;
  private TypeServices.NumericWindowWriter windowWriter;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.FLOAT, Type.DOUBLE, Type.INT32, Type.INT64)
        .validate(
            x -> Double.isFinite((double) x) && (double) x > 0,
            "Parameter $sigma$ should be finite and larger than 0.",
            validator.getParameters().getDoubleOrDefault("sigma", 1.0))
        .validate(
            params ->
                Double.isFinite((double) params[0])
                    && Double.isFinite((double) params[1])
                    && (double) params[0] < (double) params[1],
            "parameter $minSpeed$ and $maxSpeed$ should be finite, and $minSpeed$ should be smaller than $maxSpeed$.",
            validator.getParameters().getDoubleOrDefault("minSpeed", -1),
            validator.getParameters().getDoubleOrDefault("maxSpeed", 1))
        .validate(
            x -> Double.isFinite((double) x),
            "Parameter $center$ should be finite.",
            validator.getParameters().getDoubleOrDefault("center", 0))
        .validate(
            method -> isValidMethod((String) method),
            "Method should be screen or lsgreedy.",
            validator.getParameters().getStringOrDefault("method", METHOD_SCREEN));
  }

  private static boolean isValidMethod(String method) {
    return METHOD_SCREEN.equalsIgnoreCase(method) || METHOD_LS_GREEDY.equalsIgnoreCase(method);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
        .setOutputDataType(parameters.getDataType(0));
    method = parameters.getStringOrDefault("method", METHOD_SCREEN);
    minSpeed = parameters.getDoubleOrDefault("minSpeed", Double.NaN);
    maxSpeed = parameters.getDoubleOrDefault("maxSpeed", Double.NaN);
    center = parameters.getDoubleOrDefault("center", 0);
    sigma = parameters.getDoubleOrDefault("sigma", Double.NaN);
    windowWriter =
        TypeServices.NUMERIC_WINDOW_WRITER_SERVICE.call(
            TypeServices.toReadType(parameters.getDataType(0)));
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    ValueRepair vr;
    try {
      if (METHOD_SCREEN.equalsIgnoreCase(method)) {
        Screen screen = new Screen(rowWindow.getRowIterator());
        if (!Double.isNaN(minSpeed)) {
          screen.setSmin(minSpeed);
        }
        if (!Double.isNaN(maxSpeed)) {
          screen.setSmax(maxSpeed);
        }
        vr = screen;
      } else if (METHOD_LS_GREEDY.equalsIgnoreCase(method)) {
        LsGreedy lsGreedy = new LsGreedy(rowWindow.getRowIterator());
        if (!Double.isNaN(sigma)) {
          lsGreedy.setSigma(sigma);
        }
        lsGreedy.setCenter(center);
        vr = lsGreedy;
      } else {
        throw new UDFException(LibraryUdfMessages.ILLEGAL_METHOD_WITH_DOT);
      }
      vr.repair();
    } catch (UDFException e) {
      if (LibraryUdfMessages.AT_LEAST_TWO_NON_NAN_VALUES_NEEDED.equals(e.getMessage())) {
        return;
      }
      throw e;
    }
    double[] repaired = vr.getRepaired();
    long[] time = vr.getTime();
    writeFinite(time, repaired, collector);
  }

  private void writeFinite(long[] time, double[] values, PointCollector collector)
      throws Exception {
    int count = 0;
    for (double value : values) {
      if (Double.isFinite(value)) {
        count++;
      }
    }
    long[] validTime = new long[count];
    double[] validValues = new double[count];
    int index = 0;
    for (int i = 0; i < values.length; i++) {
      if (Double.isFinite(values[i])) {
        validTime[index] = time[i];
        validValues[index++] = values[i];
      }
    }
    windowWriter.write(validTime, validValues, collector);
  }
}
