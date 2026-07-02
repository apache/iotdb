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

package org.apache.iotdb.library.dquality;

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

final class QualityUDTFConfigs {
  private static final String WINDOW_KEY = "window";

  private QualityUDTFConfigs() {
    throw new IllegalStateException(LibraryUdfMessages.UTILITY_CLASS);
  }

  static void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);

    if (validator.getParameters().hasAttribute(WINDOW_KEY)) {
      validator.validate(
          window -> isValidWindow((String) window, validator.getParameters()),
          "Parameter window should be a positive duration or a positive integer.",
          validator.getParameters().getString(WINDOW_KEY));
    }
  }

  static void configureAccessStrategy(UDFParameters parameters, UDTFConfigurations configurations) {
    WindowConfig windowConfig = parseWindow(parameters);
    if (windowConfig.isTimeWindow) {
      configurations.setAccessStrategy(new SlidingTimeWindowAccessStrategy(windowConfig.window));
    } else {
      configurations.setAccessStrategy(
          new SlidingSizeWindowAccessStrategy((int) windowConfig.window));
    }
  }

  static boolean isInsufficientValidData(UDFException exception) {
    return LibraryUdfMessages.AT_LEAST_TWO_NON_NAN_VALUES_NEEDED.equals(exception.getMessage());
  }

  private static boolean isValidWindow(String window, UDFParameters parameters) {
    try {
      parseWindow(window, parameters);
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static WindowConfig parseWindow(UDFParameters parameters) {
    if (!parameters.hasAttribute(WINDOW_KEY)) {
      return new WindowConfig(false, Integer.MAX_VALUE);
    }
    return parseWindow(parameters.getString(WINDOW_KEY), parameters);
  }

  private static WindowConfig parseWindow(String window, UDFParameters parameters) {
    long parsedWindow = Util.parseTime(window, parameters);
    if (parsedWindow > 0) {
      return new WindowConfig(true, parsedWindow);
    }

    long sizeWindow = Long.parseLong(window.replace(" ", ""));
    if (sizeWindow <= 0 || sizeWindow > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Window size should be in (0, Integer.MAX_VALUE].");
    }
    return new WindowConfig(false, sizeWindow);
  }

  private static class WindowConfig {
    private final boolean isTimeWindow;
    private final long window;

    private WindowConfig(boolean isTimeWindow, long window) {
      this.isTimeWindow = isTimeWindow;
      this.window = window;
    }
  }
}
