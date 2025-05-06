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

import org.apache.iotdb.library.match.model.PatternContext;
import org.apache.iotdb.library.match.model.PatternResult;
import org.apache.iotdb.library.match.model.PatternState;
import org.apache.iotdb.library.match.model.Point;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class UDAFPatternMatch implements UDAF {

  static final String THRESHOLD_PARAM = "threshold";
  static final String TIME_PATTERN_PARAM = "timePattern";
  static final String VALUE_PATTERN_PARAM = "valuePattern";

  private Long[] timePattern;
  private Double[] valuePattern;
  private float threshold;
  private PatternState state;

  @Override
  public void beforeStart(UDFParameters udfParameters, UDAFConfigurations udafConfigurations) {
    udafConfigurations.setOutputDataType(Type.TEXT);
    Map<String, String> attributes = udfParameters.getAttributes();
    threshold = Float.parseFloat(attributes.get(THRESHOLD_PARAM));
  }

  @Override
  public State createState() {
    state = new PatternState();
    return state;
  }

  @Override
  public void addInput(State state, Column[] columns, BitMap bitMap) { // TODO 类似于UDTF里面的process，接受一行输入，然后处理
    PatternState matchState = (PatternState) state; // TODO 这里的state看起来像是传递了一个引用

    int count = columns[0].getPositionCount();// TODO 这里应该只有1，所以只处理第一列和时间列。
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) { // TODO 这里的标记的意思是这个地方有数字？
        continue;
      }
      if (!columns[1].isNull(i)) {
        long timestamp = columns[1].getLong(i);
        double value = getValue(columns[0], i);
        matchState.updateBuffer(timestamp, value);
      }
    }
  }

  @Override
  public void combineState(State state, State state1) { // 这里应该是因为分段聚合而产出的，多个中间结果合并成为最终结果的操作。
    PatternState matchState = (PatternState) state;
    PatternState newMatchState = (PatternState) state1;

    List<Long> times = newMatchState.getTimeBuffer();
    List<Double> values = newMatchState.getValueBuffer();

    for (int i = 0; i < times.size(); i++) {
      matchState.updateBuffer(times.get(i), values.get(i));
    }
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) { // 这里的state存储了所有的列数据
    PatternState matchState = (PatternState) state;
    PatternExecutor executor = new PatternExecutor();

    List<Point> sourcePointsExtract =
        executor.scalePoint(matchState.getTimeBuffer(), matchState.getValueBuffer()); // 对序列数据进行处理
    List<Point> queryPointsExtract = executor.extractPoints(timePattern, valuePattern); // 对模板数据进行处理

    executor.setPoints(queryPointsExtract); // 设置模板数据的处理结果
    PatternContext ctx = new PatternContext();
    ctx.setThreshold(threshold);
    ctx.setDataPoints(sourcePointsExtract);
    // State only records time and recorded values, and the final result is calculated
    List<PatternResult> results = executor.executeQuery(ctx); // 实际的匹配过程，计算误差并进行过滤
    resultValue.setBinary(new Binary(results.toString(), Charset.defaultCharset())); // 传入的也是引用，记录了最后的结果的tostring结果
  }

  @Override
  public void validate(UDFParameterValidator validator) {

    try {
      String timePatternStr = validator.getParameters().getStringOrDefault(TIME_PATTERN_PARAM, "");
      timePattern =
          Arrays.stream(timePatternStr.split(",")).map(Long::valueOf).toArray(Long[]::new);

    } catch (Exception e) {
      throw new UDFParameterNotValidException(
          "Illegal parameter, timePattern must be long,long...");
    }
    try {
      String valuePatternStr =
          validator.getParameters().getStringOrDefault(VALUE_PATTERN_PARAM, "");
      valuePattern =
          Arrays.stream(valuePatternStr.split(",")).map(Double::valueOf).toArray(Double[]::new);
    } catch (Exception e) {
      throw new UDFParameterNotValidException(
          "Illegal parameter, valuePattern must be double,double...");
    }
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.BOOLEAN)
        .validateRequiredAttribute(THRESHOLD_PARAM)
        .validateRequiredAttribute(TIME_PATTERN_PARAM)
        .validateRequiredAttribute(VALUE_PATTERN_PARAM)
        .validate(
            (UDFParameterValidator.SingleObjectValidationRule)
                payload -> ((Long[]) payload).length > 1,
            "Illegal parameter, timePattern size must larger 1.",
            timePattern)
        .validate(
            (UDFParameterValidator.SingleObjectValidationRule)
                payload ->
                    IntStream.range(1, ((Long[]) payload).length)
                        .allMatch(i -> ((Long[]) payload)[i] > ((Long[]) payload)[i - 1]),
            "Illegal parameter, timePattern value must be in ascending order.",
            timePattern)
        .validate(
            payload -> ((Long[]) payload[0]).length == ((Double[]) payload[1]).length,
            "Illegal parameter, timePattern size must equals valuePattern size.",
            timePattern,
            valuePattern);
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
}
