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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GroupByLevelDescriptor extends AggregationDescriptor {

  private final Expression outputExpression;

  public GroupByLevelDescriptor(
      String aggregationFuncName,
      AggregationStep step,
      List<Expression> inputExpressions,
      Expression outputExpression) {
    super(aggregationFuncName, step, inputExpressions);
    this.outputExpression = outputExpression;
  }

  public GroupByLevelDescriptor(
      AggregationDescriptor aggregationDescriptor, Expression outputExpression) {
    super(aggregationDescriptor);
    this.outputExpression = outputExpression;
  }

  public Expression getOutputExpression() {
    return outputExpression;
  }

  @Override
  public String getParametersString() {
    return outputExpression.getExpressionString();
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

  public GroupByLevelDescriptor deepClone() {
    return new GroupByLevelDescriptor(
        this.getAggregationFuncName(),
        this.getStep(),
        this.getInputExpressions(),
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

  public static GroupByLevelDescriptor deserialize(ByteBuffer byteBuffer) {
    AggregationDescriptor aggregationDescriptor = AggregationDescriptor.deserialize(byteBuffer);
    Expression outputExpression = Expression.deserialize(byteBuffer);
    return new GroupByLevelDescriptor(aggregationDescriptor, outputExpression);
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
    GroupByLevelDescriptor that = (GroupByLevelDescriptor) o;
    return Objects.equals(outputExpression, that.outputExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), outputExpression);
  }
}
