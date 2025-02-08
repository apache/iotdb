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

package org.apache.iotdb.library.match;

import org.apache.iotdb.library.match.model.DTWMatchResult;
import org.apache.iotdb.library.match.model.DTWState;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UDAFDTWMatch implements UDAF {
  //    private static final Logger LOGGER = Logger.getLogger(DTWMatch.class);
  private Double[] pattern;
  private float threshold;
  private DTWState state;

  @Override
  public void beforeStart(UDFParameters udfParameters, UDAFConfigurations udafConfigurations) {
    udafConfigurations.setOutputDataType(Type.TEXT);
    Map<String, String> attributes = udfParameters.getAttributes();
    threshold = Float.parseFloat(attributes.get("threshold"));
    pattern =
        Arrays.stream(attributes.get("pattern").split(","))
            .map(Double::valueOf)
            .toArray(Double[]::new);
    if (state != null) {
      state.setSize(pattern.length);
    }
  }

  @Override
  public State createState() {
    if (pattern != null) {
      state = new DTWState(pattern.length);
    } else {
      state = new DTWState();
    }

    return state;
  }

  @Override
  public void addInput(State state, Column[] columns, BitMap bitMap) {
    DTWState DTWState = (DTWState) state;

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        long timestamp = columns[1].getLong(i);
        double value = getValue(columns[0], i);
        DTWState.updateBuffer(timestamp, value);
        if (DTWState.getValueBuffer().length == pattern.length) {
          float dtw = calculateDTW(DTWState.getValueBuffer(), pattern);
          if (dtw <= threshold) {
            ((DTWState) state)
                .addMatchResult(
                    new DTWMatchResult(dtw, DTWState.getFirstTime(), DTWState.getLastTime()));
          }
        }
      }
    }
  }

  private double getValue(Column column, int i) {
    switch (column.getDataType()) {
      case INT32:
        return column.getInt(i);
      case INT64:
        return column.getLong(i);
      case FLOAT:
        return column.getFloat(i);
      case DOUBLE:
        return column.getDouble(i);
      case BOOLEAN:
        return column.getBoolean(i) ? 1.0D : 0.0D;
      default:
        throw new RuntimeException(String.format("Unsupported datatype %s", column.getDataType()));
    }
  }

  private float calculateDTW(Double[] series1, Double[] series2) {
    int n = series1.length;
    double[][] dtw = new double[n][n];

    // Initialize the DTW matrix
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        dtw[i][j] = Double.POSITIVE_INFINITY;
      }
    }
    dtw[0][0] = 0;

    // Compute the DTW distance
    for (int i = 1; i < n; i++) {
      for (int j = 1; j < n; j++) {
        double cost = Math.abs(series1[i] - series2[j]);
        dtw[i][j] = cost + Math.min(Math.min(dtw[i - 1][j], dtw[i][j - 1]), dtw[i - 1][j - 1]);
      }
    }

    return (float) dtw[n - 1][n - 1];
  }

  @Override
  public void combineState(State state, State state1) {
    DTWState dtwState = (DTWState) state;
    DTWState newDTWState = (DTWState) state1;

    Long[] times = newDTWState.getTimeBuffer();
    Double[] values = newDTWState.getValueBuffer();

    for (int i = 0; i < times.length; i++) {
      if (times[i] > dtwState.getFirstTime()) {
        dtwState.updateBuffer(times[i], values[i]);
        if (dtwState.getValueBuffer().length == pattern.length) {
          float dtw = calculateDTW(dtwState.getValueBuffer(), pattern);
          if (dtw <= threshold) {
            dtwState.addMatchResult(
                new DTWMatchResult(dtw, dtwState.getFirstTime(), dtwState.getLastTime()));
          }
        }
      }
    }
  }

  public List<DTWMatchResult> calcMatch(
      List<Long> times, List<Double> values, Double[] pattern, float threshold) {
    this.pattern = pattern;
    this.threshold = threshold;
    DTWState dtwState = (DTWState) this.createState();
    dtwState.reset();
    for (int i = 0; i < times.size(); i++) {
      dtwState.updateBuffer(times.get(i), values.get(i));
      if (dtwState.getValueBuffer().length == pattern.length) {
        float dtw = calculateDTW(dtwState.getValueBuffer(), pattern);
        if (dtw <= threshold) {
          dtwState.addMatchResult(
              new DTWMatchResult(dtw, dtwState.getFirstTime(), dtwState.getLastTime()));
        }
      }
    }
    return dtwState.getMatchResults();
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    DTWState DTWState = (DTWState) state;
    List<DTWMatchResult> matchResults = DTWState.getMatchResults();
    if (!matchResults.isEmpty()) {
      resultValue.setBinary(new Binary(matchResults.toString(), Charset.defaultCharset()));
    } else {
      resultValue.setNull();
    }
  }

  @Override
  public void removeState(State state, State removed) {}

  @Override
  public void validate(UDFParameterValidator validator) {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.BOOLEAN)
        .validateRequiredAttribute("pattern")
        .validateRequiredAttribute("threshold");
  }
}
