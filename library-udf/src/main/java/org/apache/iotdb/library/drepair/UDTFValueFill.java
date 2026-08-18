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

import org.apache.iotdb.library.drepair.util.ARFill;
import org.apache.iotdb.library.drepair.util.LikelihoodFill;
import org.apache.iotdb.library.drepair.util.LinearFill;
import org.apache.iotdb.library.drepair.util.MeanFill;
import org.apache.iotdb.library.drepair.util.PreviousFill;
import org.apache.iotdb.library.drepair.util.ScreenFill;
import org.apache.iotdb.library.drepair.util.ValueFill;
import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

/** This function is used to interpolate time series. */
public class UDTFValueFill implements UDTF {
  private static final String METHOD_PREVIOUS = "previous";
  private static final String METHOD_LINEAR = "linear";
  private static final String METHOD_MEAN = "mean";
  private static final String METHOD_AR = "ar";
  private static final String METHOD_SCREEN = "screen";
  private static final String METHOD_LIKELIHOOD = "likelihood";

  private String method;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.FLOAT, Type.DOUBLE, Type.INT32, Type.INT64)
        .validate(
            method -> isValidMethod((String) method),
            "Illegal fill method.",
            validator.getParameters().getStringOrDefault("method", METHOD_LINEAR));
  }

  private static boolean isValidMethod(String method) {
    return METHOD_PREVIOUS.equalsIgnoreCase(method)
        || METHOD_LINEAR.equalsIgnoreCase(method)
        || METHOD_MEAN.equalsIgnoreCase(method)
        || METHOD_AR.equalsIgnoreCase(method)
        || METHOD_SCREEN.equalsIgnoreCase(method)
        || METHOD_LIKELIHOOD.equalsIgnoreCase(method);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
        .setOutputDataType(parameters.getDataType(0));
    method = parameters.getStringOrDefault("method", METHOD_LINEAR);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    ValueFill vf;
    if (METHOD_PREVIOUS.equalsIgnoreCase(method)) {
      vf = new PreviousFill(rowWindow.getRowIterator());
    } else if (METHOD_LINEAR.equalsIgnoreCase(method)) {
      vf = new LinearFill(rowWindow.getRowIterator());
    } else if (METHOD_MEAN.equalsIgnoreCase(method)) {
      vf = new MeanFill(rowWindow.getRowIterator());
    } else if (METHOD_AR.equalsIgnoreCase(method)) {
      vf = new ARFill(rowWindow.getRowIterator());
    } else if (METHOD_SCREEN.equalsIgnoreCase(method)) {
      vf = new ScreenFill(rowWindow.getRowIterator());
    } else if (METHOD_LIKELIHOOD.equalsIgnoreCase(method)) {
      vf = new LikelihoodFill(rowWindow.getRowIterator());
    } else {
      throw new UDFException(LibraryUdfMessages.ILLEGAL_METHOD);
    }
    if (!vf.hasValidValue()) {
      return;
    }
    vf.fill();
    double[] repaired = vf.getFilled();
    long[] time = vf.getTime();
    switch (rowWindow.getDataType(0)) {
      case DOUBLE:
        for (int i = 0; i < time.length; i++) {
          if (Double.isFinite(repaired[i])) {
            collector.putDouble(time[i], repaired[i]);
          }
        }
        break;
      case FLOAT:
        for (int i = 0; i < time.length; i++) {
          if (Double.isFinite(repaired[i])) {
            collector.putFloat(time[i], (float) repaired[i]);
          }
        }
        break;
      case INT32:
        for (int i = 0; i < time.length; i++) {
          if (Double.isFinite(repaired[i])) {
            collector.putInt(time[i], (int) Math.round(repaired[i]));
          }
        }
        break;
      case INT64:
        for (int i = 0; i < time.length; i++) {
          if (Double.isFinite(repaired[i])) {
            collector.putLong(time[i], Math.round(repaired[i]));
          }
        }
        break;
      case TEXT:
      case STRING:
      case BOOLEAN:
      case BLOB:
      case TIMESTAMP:
      case DATE:
      default:
        throw new UDFException("");
    }
  }
}
