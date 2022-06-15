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
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class AggregationDescriptor {

  // aggregation function name
  protected final AggregationType aggregationType;

  // indicate the input and output type
  protected AggregationStep step;

  /**
   * Input of aggregation function. Currently, we only support one series in the aggregation
   * function.
   *
   * <p>example: select sum(s1) from root.sg.d1; expression [root.sg.d1.s1] will be in this field.
   */
  protected List<Expression> inputExpressions;

  private String parametersString;

  public AggregationDescriptor(
      AggregationType aggregationType, AggregationStep step, List<Expression> inputExpressions) {
    this.aggregationType = aggregationType;
    this.step = step;
    this.inputExpressions = inputExpressions;
  }

  public AggregationDescriptor(AggregationDescriptor other) {
    this.aggregationType = other.getAggregationType();
    this.step = other.getStep();
    this.inputExpressions = other.getInputExpressions();
  }

  public List<String> getOutputColumnNames() {
    List<AggregationType> outputAggregationTypes =
        getActualAggregationTypes(step.isOutputPartial());
    List<String> outputColumnNames = new ArrayList<>();
    for (AggregationType funcName : outputAggregationTypes) {
      outputColumnNames.add(funcName.toString().toLowerCase() + "(" + getParametersString() + ")");
    }
    return outputColumnNames;
  }

  public List<List<String>> getInputColumnNamesList() {
    if (step.isInputRaw()) {
      return inputExpressions.stream()
          .map(expression -> Collections.singletonList(expression.getExpressionString()))
          .collect(Collectors.toList());
    }

    List<List<String>> inputColumnNames = new ArrayList<>();
    for (Expression expression : inputExpressions) {
      inputColumnNames.add(getInputColumnNames(expression));
    }
    return inputColumnNames;
  }

  public List<String> getInputColumnNames(Expression inputExpression) {
    List<AggregationType> inputAggregationTypes = getActualAggregationTypes(step.isInputPartial());
    List<String> inputColumnNames = new ArrayList<>();
    for (AggregationType funcName : inputAggregationTypes) {
      inputColumnNames.add(
          funcName.toString().toLowerCase() + "(" + inputExpression.getExpressionString() + ")");
    }
    return inputColumnNames;
  }

  public Map<String, Expression> getInputColumnCandidateMap() {
    Map<String, Expression> inputColumnNameToExpressionMap = new HashMap<>();
    for (Expression inputExpression : inputExpressions) {
      List<String> inputColumnNames = getInputColumnNames(inputExpression);
      for (String inputColumnName : inputColumnNames) {
        inputColumnNameToExpressionMap.put(inputColumnName, inputExpression);
      }
    }
    return inputColumnNameToExpressionMap;
  }

  protected List<AggregationType> getActualAggregationTypes(boolean isPartial) {
    List<AggregationType> outputAggregationTypes = new ArrayList<>();
    if (isPartial) {
      switch (aggregationType) {
        case AVG:
          outputAggregationTypes.add(AggregationType.COUNT);
          outputAggregationTypes.add(AggregationType.SUM);
          break;
        case FIRST_VALUE:
          outputAggregationTypes.add(AggregationType.FIRST_VALUE);
          outputAggregationTypes.add(AggregationType.MIN_TIME);
          break;
        case LAST_VALUE:
          outputAggregationTypes.add(AggregationType.LAST_VALUE);
          outputAggregationTypes.add(AggregationType.MAX_TIME);
          break;
        default:
          outputAggregationTypes.add(aggregationType);
      }
    } else {
      outputAggregationTypes.add(aggregationType);
    }
    return outputAggregationTypes;
  }

  /**
   * Generates the parameter part of the function column name.
   *
   * <p>Example:
   *
   * <p>Full column name -> udf(root.sg.d.s1, sin(root.sg.d.s1))
   *
   * <p>The parameter part -> root.sg.d.s1, sin(root.sg.d.s1)
   */
  public String getParametersString() {
    if (parametersString == null) {
      StringBuilder builder = new StringBuilder();
      if (!inputExpressions.isEmpty()) {
        builder.append(inputExpressions.get(0).toString());
        for (int i = 1; i < inputExpressions.size(); ++i) {
          builder.append(", ").append(inputExpressions.get(i).toString());
        }
      }
      parametersString = builder.toString();
    }
    return parametersString;
  }

  public List<Expression> getInputExpressions() {
    return inputExpressions;
  }

  public AggregationType getAggregationType() {
    return aggregationType;
  }

  public AggregationStep getStep() {
    return step;
  }

  public void setStep(AggregationStep step) {
    this.step = step;
  }

  public void setInputExpressions(List<Expression> inputExpressions) {
    this.inputExpressions = inputExpressions;
  }

  public AggregationDescriptor deepClone() {
    return new AggregationDescriptor(
        this.getAggregationType(), this.getStep(), this.getInputExpressions());
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(aggregationType.ordinal(), byteBuffer);
    step.serialize(byteBuffer);
    ReadWriteIOUtils.write(inputExpressions.size(), byteBuffer);
    for (Expression expression : inputExpressions) {
      Expression.serialize(expression, byteBuffer);
    }
  }

  public static AggregationDescriptor deserialize(ByteBuffer byteBuffer) {
    AggregationType aggregationType =
        AggregationType.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    AggregationStep step = AggregationStep.deserialize(byteBuffer);
    int inputExpressionsSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Expression> inputExpressions = new ArrayList<>(inputExpressionsSize);
    while (inputExpressionsSize > 0) {
      inputExpressions.add(Expression.deserialize(byteBuffer));
      inputExpressionsSize--;
    }
    return new AggregationDescriptor(aggregationType, step, inputExpressions);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AggregationDescriptor that = (AggregationDescriptor) o;
    return aggregationType == that.aggregationType
        && step == that.step
        && Objects.equals(inputExpressions, that.inputExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(aggregationType, step, inputExpressions);
  }

  public String toString() {
    return String.format("AggregationDescriptor(%s, %s)", aggregationType, step);
  }
}
