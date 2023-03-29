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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AggregationDescriptor {

  // aggregation function type
  protected final TAggregationType aggregationType;
  // In case user's input is case-sensitive, we should keep the origin string.
  protected final String aggregationFuncName;

  // indicate the input and output type
  protected AggregationStep step;

  /**
   * Input of aggregation function. Currently, we only support one series in the aggregation
   * function.
   *
   * <p>example: select sum(s1) from root.sg.d1; expression [root.sg.d1.s1] will be in this field.
   *
   * <p>example: select sum(s1) from root.** group by level = 1; expression [root.sg.*.s1] may be in
   * this field if the data is in different DataRegion</>
   */
  protected List<Expression> inputExpressions;

  protected final Map<String, String> inputAttributes;

  protected String parametersString;

  public AggregationDescriptor(
      String aggregationFuncName,
      AggregationStep step,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes) {
    this.aggregationFuncName = aggregationFuncName;
    this.aggregationType = TAggregationType.valueOf(aggregationFuncName.toUpperCase());
    this.step = step;
    this.inputExpressions = inputExpressions;
    this.inputAttributes = inputAttributes;
  }

  // Old method, please don't use it any more
  // keep here for compatibility of old Test before introduce of count_if
  @TestOnly
  @Deprecated
  public AggregationDescriptor(
      String aggregationFuncName, AggregationStep step, List<Expression> inputExpressions) {
    this.aggregationFuncName = aggregationFuncName;
    this.aggregationType = TAggregationType.valueOf(aggregationFuncName.toUpperCase());
    this.step = step;
    this.inputExpressions = inputExpressions;
    this.inputAttributes = Collections.emptyMap();
  }

  public AggregationDescriptor(AggregationDescriptor other) {
    this.aggregationFuncName = other.aggregationFuncName;
    this.aggregationType = other.getAggregationType();
    this.step = other.getStep();
    this.inputExpressions = other.getInputExpressions();
    this.inputAttributes = other.inputAttributes;
  }

  public String getAggregationFuncName() {
    return aggregationFuncName;
  }

  public List<String> getOutputColumnNames() {
    List<String> outputAggregationNames = getActualAggregationNames(step.isOutputPartial());
    List<String> outputColumnNames = new ArrayList<>();
    for (String funcName : outputAggregationNames) {
      outputColumnNames.add(funcName + "(" + getParametersString() + ")");
    }
    return outputColumnNames;
  }

  public List<List<String>> getInputColumnNamesList() {
    if (step.isInputRaw()) {
      return Collections.singletonList(
          Collections.singletonList(inputExpressions.get(0).getExpressionString()));
    }

    return Collections.singletonList(getInputColumnNames());
  }

  public List<String> getInputColumnNames() {
    List<String> inputAggregationNames = getActualAggregationNames(step.isInputPartial());
    List<String> inputColumnNames = new ArrayList<>();
    for (String funcName : inputAggregationNames) {
      inputColumnNames.add(funcName + "(" + getParametersString() + ")");
    }
    return inputColumnNames;
  }

  /** Keep the lower case of function name for partial result, and origin value for others. */
  protected List<String> getActualAggregationNames(boolean isPartial) {
    List<String> outputAggregationNames = new ArrayList<>();
    if (isPartial) {
      switch (aggregationType) {
        case AVG:
          outputAggregationNames.add(SqlConstant.COUNT);
          outputAggregationNames.add(SqlConstant.SUM);
          break;
        case FIRST_VALUE:
          outputAggregationNames.add(SqlConstant.FIRST_VALUE);
          outputAggregationNames.add(SqlConstant.MIN_TIME);
          break;
        case LAST_VALUE:
          outputAggregationNames.add(SqlConstant.LAST_VALUE);
          outputAggregationNames.add(SqlConstant.MAX_TIME);
          break;
        case TIME_DURATION:
          outputAggregationNames.add(SqlConstant.MAX_TIME);
          outputAggregationNames.add(SqlConstant.MIN_TIME);
          break;
        default:
          outputAggregationNames.add(aggregationFuncName);
      }
    } else {
      outputAggregationNames.add(aggregationFuncName);
    }
    return outputAggregationNames;
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
  protected String getParametersString() {
    if (parametersString == null) {
      StringBuilder builder = new StringBuilder();
      if (!inputExpressions.isEmpty()) {
        builder.append(inputExpressions.get(0).toString());
        for (int i = 1; i < inputExpressions.size(); ++i) {
          builder.append(", ").append(inputExpressions.get(i).toString());
        }
      }
      appendAttributes(builder);
      parametersString = builder.toString();
    }
    return parametersString;
  }

  protected void appendAttributes(StringBuilder builder) {
    if (!inputAttributes.isEmpty()) {
      builder.append(", ");

      Iterator<Map.Entry<String, String>> iterator = inputAttributes.entrySet().iterator();
      Map.Entry<String, String> entry = iterator.next();
      builder
          .append("\"")
          .append(entry.getKey())
          .append("\"=\"")
          .append(entry.getValue())
          .append("\"");
      while (iterator.hasNext()) {
        entry = iterator.next();
        builder
            .append(", ")
            .append("\"")
            .append(entry.getKey())
            .append("\"=\"")
            .append(entry.getValue())
            .append("\"");
      }
    }
  }

  public List<Expression> getInputExpressions() {
    return inputExpressions;
  }

  public Map<String, String> getInputAttributes() {
    return inputAttributes;
  }

  public TAggregationType getAggregationType() {
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
    return new AggregationDescriptor(this);
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(aggregationFuncName, byteBuffer);
    step.serialize(byteBuffer);
    ReadWriteIOUtils.write(inputExpressions.size(), byteBuffer);
    for (Expression expression : inputExpressions) {
      Expression.serialize(expression, byteBuffer);
    }
    ReadWriteIOUtils.write(inputAttributes, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(aggregationFuncName, stream);
    step.serialize(stream);
    ReadWriteIOUtils.write(inputExpressions.size(), stream);
    for (Expression expression : inputExpressions) {
      Expression.serialize(expression, stream);
    }
    ReadWriteIOUtils.write(inputAttributes, stream);
  }

  public static AggregationDescriptor deserialize(ByteBuffer byteBuffer) {
    String aggregationFuncName = ReadWriteIOUtils.readString(byteBuffer);
    AggregationStep step = AggregationStep.deserialize(byteBuffer);
    int inputExpressionsSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Expression> inputExpressions = new ArrayList<>(inputExpressionsSize);
    while (inputExpressionsSize > 0) {
      inputExpressions.add(Expression.deserialize(byteBuffer));
      inputExpressionsSize--;
    }
    Map<String, String> inputAttributes = ReadWriteIOUtils.readMap(byteBuffer);
    return new AggregationDescriptor(aggregationFuncName, step, inputExpressions, inputAttributes);
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
    return String.format("AggregationDescriptor(%s, %s)", aggregationFuncName, step);
  }
}
