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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CrossSeriesAggregationDescriptor extends AggregationDescriptor {

  private final Expression outputExpression;

  private String outputParametersString;

  public CrossSeriesAggregationDescriptor(
      String aggregationFuncName,
      AggregationStep step,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      Expression outputExpression) {
    super(aggregationFuncName, step, inputExpressions, inputAttributes);
    this.outputExpression = outputExpression;
  }

  public CrossSeriesAggregationDescriptor(
      AggregationDescriptor aggregationDescriptor, Expression outputExpression) {
    super(aggregationDescriptor);
    this.outputExpression = outputExpression;
  }

  public Expression getOutputExpression() {
    return outputExpression;
  }

  public List<String> getOutputColumnNames() {
    List<String> outputAggregationNames = getActualAggregationNames(step.isOutputPartial());
    List<String> outputColumnNames = new ArrayList<>();
    for (String funcName : outputAggregationNames) {
      outputColumnNames.add(funcName + "(" + getOutputParametersString() + ")");
    }
    return outputColumnNames;
  }

  private String getOutputParametersString() {
    if (outputParametersString == null) {
      StringBuilder builder = new StringBuilder(outputExpression.getExpressionString());
      for (int i = 1; i < inputExpressions.size(); i++) {
        builder.append(", ").append(inputExpressions.get(i).toString());
      }
      appendAttributes(builder);
      outputParametersString = builder.toString();
    }
    return outputParametersString;
  }

  @Override
  public Map<String, Expression> getInputColumnCandidateMap() {
    Map<String, Expression> inputColumnNameToExpressionMap = super.getInputColumnCandidateMap();
    List<String> outputColumnNames = getOutputColumnNames();
    for (String outputColumnName : outputColumnNames) {
      inputColumnNameToExpressionMap.put(outputColumnName, outputExpression);
    }
    return inputColumnNameToExpressionMap;
  }

  @Override
  public CrossSeriesAggregationDescriptor deepClone() {
    return new CrossSeriesAggregationDescriptor(
        this.getAggregationFuncName(),
        this.getStep(),
        this.getInputExpressions(),
        this.getInputAttributes(),
        this.getOutputExpression());
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    Expression.serialize(outputExpression, byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    Expression.serialize(outputExpression, stream);
  }

  public static CrossSeriesAggregationDescriptor deserialize(ByteBuffer byteBuffer) {
    AggregationDescriptor aggregationDescriptor = AggregationDescriptor.deserialize(byteBuffer);
    Expression outputExpression = Expression.deserialize(byteBuffer);
    return new CrossSeriesAggregationDescriptor(aggregationDescriptor, outputExpression);
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
