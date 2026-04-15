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

package org.apache.iotdb.db.queryengine.plan.planner.plan.parameter;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CrossSeriesAggregationDescriptor extends AggregationDescriptor {

  private final List<Expression> outputExpressions;

  private List<List<Expression>> groupedInputExpressions;

  /**
   * Records how many Expressions are in one input, used for calculation of inputColumnNames
   *
   * <p>.e.g input[count_if(root.db.d1.s1, 3), count_if(root.db.d2.s1, 3)], expressionNumOfOneInput
   * = 2
   */
  private final int expressionNumOfOneInput;

  public CrossSeriesAggregationDescriptor(
      String aggregationFuncName,
      AggregationStep step,
      List<Expression> inputExpressions,
      int numberOfInput,
      Map<String, String> inputAttributes,
      List<Expression> outputExpressions) {
    super(aggregationFuncName, step, inputExpressions, inputAttributes);
    this.outputExpressions = outputExpressions;
    this.expressionNumOfOneInput = inputExpressions.size() / numberOfInput;
    initGroupedInputExpressions();
  }

  /**
   * Please ensure only one Expression in one input when you use this construction, now only
   * GroupByTagNode use it
   */
  public CrossSeriesAggregationDescriptor(
      String aggregationFuncName,
      AggregationStep step,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      List<Expression> outputExpressions) {
    super(aggregationFuncName, step, inputExpressions, inputAttributes);
    this.outputExpressions = outputExpressions;
    this.expressionNumOfOneInput = 1;
    initGroupedInputExpressions();
  }

  public CrossSeriesAggregationDescriptor(
      AggregationDescriptor aggregationDescriptor,
      List<Expression> outputExpressions,
      int expressionNumOfOneInput) {
    super(aggregationDescriptor);
    this.outputExpressions = outputExpressions;
    this.expressionNumOfOneInput = expressionNumOfOneInput;
    initGroupedInputExpressions();
  }

  public List<Expression> getOutputExpressions() {
    return outputExpressions;
  }

  /**
   * Generates the parameter part of the output column name.
   *
   * <p>Example:
   *
   * <p>Full output column name -> count_if(root.*.*.s1, 3)
   *
   * <p>The parameter part -> root.*.*.s1, 3
   */
  @Override
  public String getParametersString() {
    if (parametersString == null) {
      StringBuilder builder;
      if (TAggregationType.COUNT_IF.equals(aggregationType)) {
        builder = new StringBuilder(outputExpressions.get(0).getExpressionString());
        for (int i = 1; i < expressionNumOfOneInput; i++) {
          builder.append(", ").append(inputExpressions.get(i).getExpressionString());
        }
        appendAttributes(builder);
      } else {
        builder = getOutputExpressionsAsBuilder();
      }
      parametersString = builder.toString();
    }
    return parametersString;
  }

  @Override
  public List<List<String>> getInputColumnNamesList() {
    if (step.isInputRaw()) {
      List<String> inputColumnNames =
          SqlConstant.COUNT_IF.equalsIgnoreCase(aggregationFuncName)
              ? Collections.singletonList(inputExpressions.get(0).getExpressionString())
              : inputExpressions.stream()
                  .map(Expression::getExpressionString)
                  .collect(Collectors.toList());
      return Collections.singletonList(inputColumnNames);
    }

    List<List<String>> inputColumnNamesList = new ArrayList<>();
    groupedInputExpressions.forEach(list -> inputColumnNamesList.add(getInputColumnNames(list)));
    return inputColumnNamesList;
  }

  @Override
  public List<String> getInputExpressionsAsStringList() {
    List<String> res = new ArrayList<>();
    for (int i = 0; i < inputExpressions.size(); i += expressionNumOfOneInput) {
      List<Expression> expressions = new ArrayList<>();
      for (int j = 0; j < expressionNumOfOneInput; j++) {
        expressions.add(inputExpressions.get(i + j));
      }
      res.add(getInputString(expressions));
    }
    return res;
  }

  /**
   * For an aggregate function that takes two inputs, the inputExpressions may be like
   * [root.sg.d1.s1, root.sg.d1.s2, root.sg.d2.s1, root.sg.d2.s2]. The inputExpressions is a
   * one-dimensional List, but it is split by expressionNumOfOneInput. The split result is stored in
   * groupedInputExpressions So the returned map of this method is : <"root.sg.d1.s1,
   * root.sg.d1.s2", [root.sg.d1.s1, root.sg.d1.s2]> <"root.sg.d2.s1, root.sg.d2.s2",
   * [root.sg.d2.s1, root.sg.d2.s2]>
   */
  public Map<String, List<Expression>> getGroupedInputStringToExpressionsMap() {
    Map<String, List<Expression>> map = new HashMap<>();
    groupedInputExpressions.forEach(list -> map.put(getInputString(list), list));
    return map;
  }

  public List<String> getGroupedInputExpressionStrings() {
    return groupedInputExpressions.stream().map(this::getInputString).collect(Collectors.toList());
  }

  public List<List<Expression>> getGroupedInputExpressions() {
    return groupedInputExpressions;
  }

  public StringBuilder getOutputExpressionsAsBuilder() {
    StringBuilder builder = new StringBuilder(outputExpressions.get(0).getExpressionString());
    for (int i = 1; i < outputExpressions.size(); i++) {
      builder.append(", ").append(outputExpressions.get(i).getExpressionString());
    }
    appendAttributes(builder);
    return builder;
  }

  private void initGroupedInputExpressions() {
    this.groupedInputExpressions = new ArrayList<>();
    for (int i = 0; i < inputExpressions.size(); i += expressionNumOfOneInput) {
      List<Expression> expressions = new ArrayList<>(expressionNumOfOneInput);
      for (int j = 0; j < expressionNumOfOneInput; j++) {
        expressions.add(inputExpressions.get(i + j));
      }
      this.groupedInputExpressions.add(expressions);
    }
  }

  private List<String> getInputColumnNames(List<Expression> expressions) {
    List<String> inputAggregationNames = getActualAggregationNames(step.isInputPartial());
    List<String> inputColumnNames = new ArrayList<>();
    for (String funcName : inputAggregationNames) {
      inputColumnNames.add(funcName + "(" + getInputString(expressions) + ")");
    }
    return inputColumnNames;
  }

  @Override
  public void setInputExpressions(List<Expression> inputExpressions) {
    this.inputExpressions = inputExpressions;
    initGroupedInputExpressions();
  }

  @Override
  public CrossSeriesAggregationDescriptor deepClone() {
    return new CrossSeriesAggregationDescriptor(this, outputExpressions, expressionNumOfOneInput);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputExpressions.size(), byteBuffer);
    outputExpressions.forEach(x -> Expression.serialize(x, byteBuffer));
    ReadWriteIOUtils.write(expressionNumOfOneInput, byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(outputExpressions.size(), stream);
    for (Expression x : outputExpressions) {
      Expression.serialize(x, stream);
    }
    ReadWriteIOUtils.write(expressionNumOfOneInput, stream);
  }

  public static CrossSeriesAggregationDescriptor deserialize(ByteBuffer byteBuffer) {
    AggregationDescriptor aggregationDescriptor = AggregationDescriptor.deserialize(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Expression> outputExpressions = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      outputExpressions.add(Expression.deserialize(byteBuffer));
    }
    int expressionNumOfOneInput = ReadWriteIOUtils.readInt(byteBuffer);
    return new CrossSeriesAggregationDescriptor(
        aggregationDescriptor, outputExpressions, expressionNumOfOneInput);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    CrossSeriesAggregationDescriptor that = (CrossSeriesAggregationDescriptor) o;
    return Objects.equals(outputExpressions, that.outputExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), outputExpressions);
  }

  public int getExpressionNumOfOneInput() {
    return expressionNumOfOneInput;
  }
}
