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

package org.apache.iotdb.db.queryengine.plan.expression.multi;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinScalarFunction;
import org.apache.iotdb.commons.udf.utils.TreeUDFUtils;
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.BuiltInScalarFunctionHelperFactory;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFInformationInferrer;
import org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class FunctionExpression extends Expression {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FunctionExpression.class);

  private FunctionType functionType;

  private final String functionName;
  private final LinkedHashMap<String, String> functionAttributes;

  /**
   * example: select udf(a, b, udf(c)) from root.sg.d;
   *
   * <p>3 expressions [root.sg.d.a, root.sg.d.b, udf(root.sg.d.c)] will be in this field.
   */
  private List<Expression> expressions;

  private List<PartialPath> paths;

  private List<Expression> countTimeExpressions;

  private String parametersString;

  public FunctionExpression(String functionName) {
    this.functionName = functionName;
    functionAttributes = new LinkedHashMap<>();
    expressions = new ArrayList<>();
  }

  public FunctionExpression(
      String functionName,
      LinkedHashMap<String, String> functionAttributes,
      List<Expression> expressions) {
    this.functionName = functionName;
    this.functionAttributes = functionAttributes;
    this.expressions = expressions;
    this.countTimeExpressions = null;
  }

  public FunctionExpression(
      String functionName,
      LinkedHashMap<String, String> functionAttributes,
      List<Expression> expressions,
      List<Expression> countTimeExpressions) {
    this.functionName = functionName;
    this.functionAttributes = functionAttributes;
    this.expressions = expressions;
    this.countTimeExpressions = countTimeExpressions;
  }

  public FunctionExpression(ByteBuffer byteBuffer) {
    functionName = ReadWriteIOUtils.readString(byteBuffer);

    functionAttributes = ReadWriteIOUtils.readLinkedHashMap(byteBuffer);

    int expressionSize = ReadWriteIOUtils.readInt(byteBuffer);
    expressions = new ArrayList<>();
    for (int i = 0; i < expressionSize; i++) {
      expressions.add(Expression.deserialize(byteBuffer));
    }
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitFunctionExpression(this, context);
  }

  private void initializeFunctionType() {
    final String lowerCaseFunctionName = this.functionName.toLowerCase();
    if (BuiltinAggregationFunction.getNativeFunctionNames().contains(lowerCaseFunctionName)) {
      functionType = FunctionType.BUILT_IN_AGGREGATION_FUNCTION;
    } else if (BuiltinScalarFunction.getNativeFunctionNames().contains(lowerCaseFunctionName)) {
      functionType = FunctionType.BUILT_IN_SCALAR_FUNCTION;
    } else if (TreeUDFUtils.isUDAF(functionName)) {
      functionType = FunctionType.UDAF;
    } else {
      functionType = FunctionType.UDTF;
    }
  }

  @Override
  public boolean isBuiltInAggregationFunctionExpression() {
    if (functionType == null) {
      initializeFunctionType();
    }
    return functionType == FunctionType.BUILT_IN_AGGREGATION_FUNCTION;
  }

  public boolean isBuiltInScalarFunctionExpression() {
    if (functionType == null) {
      initializeFunctionType();
    }
    return functionType == FunctionType.BUILT_IN_SCALAR_FUNCTION;
  }

  @Override
  public boolean isExternalAggregationFunctionExpression() {
    if (functionType == null) {
      initializeFunctionType();
    }
    return functionType == FunctionType.UDAF;
  }

  @Override
  public boolean isConstantOperandInternal() {
    if (isConstantOperandCache == null) {
      isConstantOperandCache = true;
      for (Expression inputExpression : expressions) {
        if (!inputExpression.isConstantOperand()) {
          isConstantOperandCache = false;
          break;
        }
      }
    }
    return isConstantOperandCache;
  }

  public boolean isCountStar() {
    if (!isAggregationFunctionExpression()) {
      return false;
    }
    return getPaths().size() == 1
        && paths.get(0) != null
        && (paths.get(0).getTailNode().equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
            || paths.get(0).getTailNode().equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD))
        && functionName.equals(IoTDBConstant.COLUMN_COUNT);
  }

  public void addAttribute(String key, String value) {
    functionAttributes.put(key, value);
  }

  public void addExpression(Expression expression) {
    expressions.add(expression);
  }

  public void setExpressions(List<Expression> expressions) {
    this.expressions = expressions;
  }

  public FunctionType getFunctionType() {
    return functionType;
  }

  public void setFunctionType(FunctionType functionType) {
    this.functionType = functionType;
  }

  public String getFunctionName() {
    return functionName;
  }

  public LinkedHashMap<String, String> getFunctionAttributes() {
    return functionAttributes;
  }

  @Override
  public List<Expression> getExpressions() {
    return expressions;
  }

  @Override
  public String getOutputSymbolInternal() {
    StringBuilder builder = new StringBuilder();
    if (!expressions.isEmpty()) {
      builder.append(expressions.get(0).getOutputSymbol());
      for (int i = 1; i < expressions.size(); ++i) {
        builder.append(", ").append(expressions.get(i).getOutputSymbol());
      }
    }
    if (!functionAttributes.isEmpty()) {
      // Some built-in scalar functions may have different header.
      if (BuiltinScalarFunction.contains(functionName)) {
        BuiltInScalarFunctionHelperFactory.createHelper(functionName)
            .appendFunctionAttributes(!expressions.isEmpty(), builder, functionAttributes);
      } else {
        appendAttributes(!expressions.isEmpty(), builder, functionAttributes);
      }
    }
    return functionName + "(" + builder + ")";
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    String expressionString = getExpressionString();
    if (expressionName2Executor.containsKey(expressionString)) {
      return;
    }

    for (Expression expression : expressions) {
      expression.constructUdfExecutors(expressionName2Executor, zoneId);
    }
    expressionName2Executor.put(expressionString, new UDTFExecutor(functionName, zoneId));
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    for (Expression expression : expressions) {
      expression.bindInputLayerColumnIndexWithExpression(inputLocations);
    }

    final String digest = getExpressionString();
    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    for (Expression expression : expressions) {
      expression.updateStatisticsForMemoryAssigner(memoryAssigner);
    }
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
  public boolean isMappable(Map<NodeRef<Expression>, TSDataType> expressionTypes) {
    if (!isAggregationFunctionExpression() && !isBuiltInScalarFunctionExpression()) {
      // this is a UDF function
      boolean isCurrentMappable =
          new UDTFInformationInferrer(functionName)
              .getAccessStrategy(
                  expressions.stream()
                      .map(Expression::getExpressionString)
                      .collect(Collectors.toList()),
                  expressions.stream()
                      .map(f -> expressionTypes.get(NodeRef.of(f)))
                      .collect(Collectors.toList()),
                  functionAttributes)
              .getAccessStrategyType()
              .equals(AccessStrategy.AccessStrategyType.MAPPABLE_ROW_BY_ROW);
      if (!isCurrentMappable) {
        return false;
      }
    }

    // Function expression is mappable only when all its child expressions are mappable
    boolean hasNonMappableChild = false;
    for (Expression child : expressions) {
      if (!child.isMappable(expressionTypes)) {
        hasNonMappableChild = true;
        break;
      }
    }
    return !hasNonMappableChild;
  }

  public List<PartialPath> getPaths() {
    if (paths == null) {
      paths = new ArrayList<>();
      for (Expression expression : expressions) {
        paths.add(
            expression instanceof TimeSeriesOperand
                ? ((TimeSeriesOperand) expression).getPath()
                : null);
      }
    }
    return paths;
  }

  public List<Expression> getCountTimeExpressions() {
    return this.countTimeExpressions;
  }

  @Override
  public String getExpressionStringInternal() {
    return functionName + "(" + getParametersString() + ")";
  }

  /**
   * Generates the parameter part of the function column name.
   *
   * <p>Example:
   *
   * <p>Full column name -> udf(root.sg.d.s1, sin(root.sg.d.s1), 'key1'='value1', 'key2'='value2')
   *
   * <p>The parameter part -> root.sg.d.s1, sin(root.sg.d.s1), 'key1'='value1', 'key2'='value2'
   */
  private String getParametersString() {
    if (parametersString == null) {
      StringBuilder builder = new StringBuilder();
      if (!expressions.isEmpty()) {
        builder.append(expressions.get(0).getExpressionString());
        for (int i = 1; i < expressions.size(); ++i) {
          builder.append(", ").append(expressions.get(i).getExpressionString());
        }
      }
      if (!functionAttributes.isEmpty()) {
        // Some built-in scalar functions may have different header.
        if (BuiltinScalarFunction.contains(functionName)) {
          BuiltInScalarFunctionHelperFactory.createHelper(functionName)
              .appendFunctionAttributes(!expressions.isEmpty(), builder, functionAttributes);
        } else {
          appendAttributes(!expressions.isEmpty(), builder, functionAttributes);
        }
      }
      parametersString = builder.toString();
    }
    return parametersString;
  }

  public static void appendAttributes(
      boolean hasExpression, StringBuilder builder, Map<String, String> functionAttributes) {
    if (hasExpression) {
      builder.append(", ");
    }
    Iterator<Entry<String, String>> iterator = functionAttributes.entrySet().iterator();
    Entry<String, String> entry = iterator.next();
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

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.FUNCTION;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(functionName, byteBuffer);
    ReadWriteIOUtils.write(functionAttributes, byteBuffer);
    ReadWriteIOUtils.write(expressions.size(), byteBuffer);
    for (Expression expression : expressions) {
      Expression.serialize(expression, byteBuffer);
    }
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(functionName, stream);
    ReadWriteIOUtils.write(functionAttributes, stream);
    ReadWriteIOUtils.write(expressions.size(), stream);
    for (Expression expression : expressions) {
      Expression.serialize(expression, stream);
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOf(functionName)
        + RamUsageEstimator.sizeOf(parametersString)
        + RamUsageEstimator.sizeOfMap(functionAttributes)
        + (expressions == null
            ? 0
            : expressions.stream()
                .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
                .sum())
        + (paths == null
            ? 0
            : paths.stream().mapToLong(MemoryEstimationHelper::getEstimatedSizeOfPartialPath).sum())
        + (countTimeExpressions == null
            ? 0
            : countTimeExpressions.stream()
                .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
                .sum());
  }
}
