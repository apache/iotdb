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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CrossSeriesAggregationDescriptor extends AggregationDescriptor {

  private final Expression outputExpression;

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
      Expression outputExpression) {
    super(aggregationFuncName, step, inputExpressions, inputAttributes);
    this.outputExpression = outputExpression;
    this.expressionNumOfOneInput = inputExpressions.size() / numberOfInput;
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
      Expression outputExpression) {
    super(aggregationFuncName, step, inputExpressions, inputAttributes);
    this.outputExpression = outputExpression;
    this.expressionNumOfOneInput = 1;
  }

  public CrossSeriesAggregationDescriptor(
      AggregationDescriptor aggregationDescriptor,
      Expression outputExpression,
      int expressionNumOfOneInput) {
    super(aggregationDescriptor);
    this.outputExpression = outputExpression;
    this.expressionNumOfOneInput = expressionNumOfOneInput;
  }

  public Expression getOutputExpression() {
    return outputExpression;
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
  protected String getParametersString() {
    if (parametersString == null) {
      StringBuilder builder = new StringBuilder(outputExpression.getExpressionString());
      for (int i = 1; i < expressionNumOfOneInput; i++) {
        builder.append(", ").append(inputExpressions.get(i).toString());
      }
      appendAttributes(builder);
      parametersString = builder.toString();
    }
    return parametersString;
  }

  @Override
  public List<List<String>> getInputColumnNamesList() {
    if (step.isInputRaw()) {
      return Collections.singletonList(
          Collections.singletonList(inputExpressions.get(0).getExpressionString()));
    }

    List<List<String>> inputColumnNamesList = new ArrayList<>();
    Expression[] expressions = new Expression[expressionNumOfOneInput];
    for (int i = 0; i < inputExpressions.size(); i += expressionNumOfOneInput) {
      for (int j = 0; j < expressionNumOfOneInput; j++) {
        expressions[j] = inputExpressions.get(i + j);
      }
      inputColumnNamesList.add(getInputColumnNames(expressions));
    }
    return inputColumnNamesList;
  }

  private List<String> getInputColumnNames(Expression[] expressions) {
    List<String> inputAggregationNames = getActualAggregationNames(step.isInputPartial());
    List<String> inputColumnNames = new ArrayList<>();
    for (String funcName : inputAggregationNames) {
      inputColumnNames.add(funcName + "(" + getInputString(expressions) + ")");
    }
    return inputColumnNames;
  }

  private String getInputString(Expression[] expressions) {
    StringBuilder builder = new StringBuilder();
    if (!(expressions.length == 0)) {
      builder.append(expressions[0].toString());
      for (int i = 1; i < expressions.length; ++i) {
        builder.append(", ").append(expressions[i].toString());
      }
    }
    appendAttributes(builder);
    return builder.toString();
  }

  @Override
  public CrossSeriesAggregationDescriptor deepClone() {
    return new CrossSeriesAggregationDescriptor(this, outputExpression, expressionNumOfOneInput);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    Expression.serialize(outputExpression, byteBuffer);
    ReadWriteIOUtils.write(expressionNumOfOneInput, byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    Expression.serialize(outputExpression, stream);
    ReadWriteIOUtils.write(expressionNumOfOneInput, stream);
  }

  public static CrossSeriesAggregationDescriptor deserialize(ByteBuffer byteBuffer) {
    AggregationDescriptor aggregationDescriptor = AggregationDescriptor.deserialize(byteBuffer);
    Expression outputExpression = Expression.deserialize(byteBuffer);
    int expressionNumOfOneInput = ReadWriteIOUtils.readInt(byteBuffer);
    return new CrossSeriesAggregationDescriptor(
        aggregationDescriptor, outputExpression, expressionNumOfOneInput);
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
    return Objects.equals(outputExpression, that.outputExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), outputExpression);
  }
}
