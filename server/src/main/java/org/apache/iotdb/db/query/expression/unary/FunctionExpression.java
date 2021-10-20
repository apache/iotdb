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

package org.apache.iotdb.db.query.expression.unary;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.MultiInputColumnIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.layer.SingleInputColumnMultiReferenceIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.SingleInputColumnSingleReferenceIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryRowTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryRowWindowTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class FunctionExpression extends Expression {

  /**
   * true: aggregation function<br>
   * false: time series generating function
   */
  private final boolean isAggregationFunctionExpression;

  private final String functionName;
  private final Map<String, String> functionAttributes;

  /**
   * example: select udf(a, b, udf(c)) from root.sg.d;
   *
   * <p>3 expressions [root.sg.d.a, root.sg.d.b, udf(root.sg.d.c)] will be in this field.
   */
  private List<Expression> expressions;

  private List<TSDataType> inputExpressionTypes;

  private List<PartialPath> paths;

  private String parametersString;

  public FunctionExpression(String functionName) {
    this.functionName = functionName;
    functionAttributes = new LinkedHashMap<>();
    expressions = new ArrayList<>();
    isAggregationFunctionExpression =
        SQLConstant.getNativeFunctionNames().contains(functionName.toLowerCase());
  }

  public FunctionExpression(
      String functionName, Map<String, String> functionAttributes, List<Expression> expressions) {
    this.functionName = functionName;
    this.functionAttributes = functionAttributes;
    this.expressions = expressions;
    isAggregationFunctionExpression =
        SQLConstant.getNativeFunctionNames().contains(functionName.toLowerCase());
  }

  @Override
  public boolean isAggregationFunctionExpression() {
    return isAggregationFunctionExpression;
  }

  @Override
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return !isAggregationFunctionExpression;
  }

  public boolean isCountStar() {
    return getPaths().size() == 1
        && paths.get(0).getTailNode().equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
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

  public String getFunctionName() {
    return functionName;
  }

  public Map<String, String> getFunctionAttributes() {
    return functionAttributes;
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    List<List<Expression>> resultExpressionsForRecursionList = new ArrayList<>();

    for (Expression suffixExpression : expressions) {
      List<Expression> resultExpressionsForRecursion = new ArrayList<>();
      suffixExpression.concat(prefixPaths, resultExpressionsForRecursion);
      resultExpressionsForRecursionList.add(resultExpressionsForRecursion);
    }

    List<List<Expression>> functionExpressions = new ArrayList<>();
    ConcatPathOptimizer.cartesianProduct(
        resultExpressionsForRecursionList, functionExpressions, 0, new ArrayList<>());
    for (List<Expression> functionExpression : functionExpressions) {
      resultExpressions.add(
          new FunctionExpression(functionName, functionAttributes, functionExpression));
    }
  }

  @Override
  public void removeWildcards(WildcardsRemover wildcardsRemover, List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    for (List<Expression> functionExpression : wildcardsRemover.removeWildcardsFrom(expressions)) {
      resultExpressions.add(
          new FunctionExpression(functionName, functionAttributes, functionExpression));
    }
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    for (Expression expression : expressions) {
      expression.collectPaths(pathSet);
    }
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
    expressionName2Executor.put(expressionString, new UDTFExecutor(this, zoneId));
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    for (Expression expression : expressions) {
      expression.updateStatisticsForMemoryAssigner(memoryAssigner);
      memoryAssigner.increaseExpressionReference(this);
    }
  }

  @Override
  public IntermediateLayer constructIntermediateLayer(
      long queryId,
      UDTFPlan udtfPlan,
      RawQueryInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      float memoryBudgetInMB = memoryAssigner.assign();

      IntermediateLayer udfInputIntermediateLayer =
          constructUdfInputIntermediateLayer(
              queryId,
              udtfPlan,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      Transformer transformer =
          constructUdfTransformer(
              queryId, udtfPlan, expressionDataTypeMap, memoryAssigner, udfInputIntermediateLayer);
      expressionDataTypeMap.put(this, transformer.getDataType());

      expressionIntermediateLayerMap.put(
          this,
          memoryAssigner.getReference(this) == 1
              ? new SingleInputColumnSingleReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer)
              : new SingleInputColumnMultiReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer));
    }

    return expressionIntermediateLayerMap.get(this);
  }

  private IntermediateLayer constructUdfInputIntermediateLayer(
      long queryId,
      UDTFPlan udtfPlan,
      RawQueryInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    List<IntermediateLayer> intermediateLayers = new ArrayList<>();
    for (Expression expression : expressions) {
      intermediateLayers.add(
          expression.constructIntermediateLayer(
              queryId,
              udtfPlan,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner));
    }
    return intermediateLayers.size() == 1
        ? intermediateLayers.get(0)
        : new MultiInputColumnIntermediateLayer(
            this,
            queryId,
            memoryAssigner.assign(),
            intermediateLayers.stream()
                .map(IntermediateLayer::constructPointReader)
                .collect(Collectors.toList()));
  }

  private UDFQueryTransformer constructUdfTransformer(
      long queryId,
      UDTFPlan udtfPlan,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner,
      IntermediateLayer udfInputIntermediateLayer)
      throws QueryProcessException, IOException {
    UDTFExecutor executor = udtfPlan.getExecutorByFunctionExpression(this);

    executor.beforeStart(queryId, memoryAssigner.assign(), expressionDataTypeMap);

    AccessStrategy accessStrategy = executor.getConfigurations().getAccessStrategy();
    switch (accessStrategy.getAccessStrategyType()) {
      case ROW_BY_ROW:
        return new UDFQueryRowTransformer(udfInputIntermediateLayer.constructRowReader(), executor);
      case SLIDING_SIZE_WINDOW:
      case SLIDING_TIME_WINDOW:
        return new UDFQueryRowWindowTransformer(
            udfInputIntermediateLayer.constructRowWindowReader(
                accessStrategy, memoryAssigner.assign()),
            executor);
      default:
        throw new UnsupportedOperationException("Unsupported transformer access strategy");
    }
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

  @Override
  public String toString() {
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
  public String getParametersString() {
    if (parametersString == null) {
      StringBuilder builder = new StringBuilder();
      if (!expressions.isEmpty()) {
        builder.append(expressions.get(0).toString());
        for (int i = 1; i < expressions.size(); ++i) {
          builder.append(", ").append(expressions.get(i).toString());
        }
      }
      if (!functionAttributes.isEmpty()) {
        if (!expressions.isEmpty()) {
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
      parametersString = builder.toString();
    }
    return parametersString;
  }
}
