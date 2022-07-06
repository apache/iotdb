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

package org.apache.iotdb.db.mpp.plan.expression.multi;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.IntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.MultiInputColumnIntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.SingleInputColumnMultiReferenceIntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.SingleInputColumnSingleReferenceIntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.multi.UDFQueryRowTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.multi.UDFQueryRowWindowTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.multi.UDFQueryTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.unary.TransparentTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFTypeInferrer;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy;

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
import java.util.Set;
import java.util.stream.Collectors;

public class FunctionExpression extends Expression {

  /**
   * true: aggregation function<br>
   * false: time series generating function
   */
  private final boolean isBuiltInAggregationFunctionExpression;

  private boolean isUserDefinedAggregationFunctionExpression;

  private final String functionName;
  private final Map<String, String> functionAttributes;

  /**
   * example: select udf(a, b, udf(c)) from root.sg.d;
   *
   * <p>3 expressions [root.sg.d.a, root.sg.d.b, udf(root.sg.d.c)] will be in this field.
   */
  private List<Expression> expressions;

  private List<PartialPath> paths;

  private String parametersString;

  public FunctionExpression(String functionName) {
    this.functionName = functionName;
    functionAttributes = new LinkedHashMap<>();
    expressions = new ArrayList<>();

    isBuiltInAggregationFunctionExpression =
        BuiltinAggregationFunction.getNativeFunctionNames().contains(functionName.toLowerCase());
    isConstantOperandCache = true;
  }

  public FunctionExpression(
      String functionName, Map<String, String> functionAttributes, List<Expression> expressions) {
    this.functionName = functionName;
    this.functionAttributes = functionAttributes;
    this.expressions = expressions;

    isBuiltInAggregationFunctionExpression =
        BuiltinAggregationFunction.getNativeFunctionNames().contains(functionName.toLowerCase());
    isConstantOperandCache = expressions.stream().anyMatch(Expression::isConstantOperand);
    isUserDefinedAggregationFunctionExpression =
        expressions.stream()
            .anyMatch(
                v ->
                    v.isUserDefinedAggregationFunctionExpression()
                        || v.isBuiltInAggregationFunctionExpression());
  }

  public FunctionExpression(ByteBuffer byteBuffer) {
    functionName = ReadWriteIOUtils.readString(byteBuffer);

    Map<String, String> deserializedFunctionAttributes = ReadWriteIOUtils.readMap(byteBuffer);
    functionAttributes =
        deserializedFunctionAttributes != null
            ? deserializedFunctionAttributes
            : new LinkedHashMap<>();

    int expressionSize = ReadWriteIOUtils.readInt(byteBuffer);
    expressions = new ArrayList<>();
    for (int i = 0; i < expressionSize; i++) {
      expressions.add(Expression.deserialize(byteBuffer));
    }

    isBuiltInAggregationFunctionExpression =
        BuiltinAggregationFunction.getNativeFunctionNames().contains(functionName);
    isConstantOperandCache = expressions.stream().anyMatch(Expression::isConstantOperand);
    isUserDefinedAggregationFunctionExpression =
        expressions.stream()
            .anyMatch(
                v ->
                    v.isUserDefinedAggregationFunctionExpression()
                        || v.isBuiltInAggregationFunctionExpression());
  }

  @Override
  public boolean isBuiltInAggregationFunctionExpression() {
    return isBuiltInAggregationFunctionExpression;
  }

  @Override
  public boolean isConstantOperandInternal() {
    return isConstantOperandCache;
  }

  @Override
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return !isBuiltInAggregationFunctionExpression()
        && !isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public boolean isUserDefinedAggregationFunctionExpression() {
    return isUserDefinedAggregationFunctionExpression;
  }

  public boolean isCountStar() {
    return getPaths().size() == 1
        && (paths.get(0).getTailNode().equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
            || paths.get(0).getTailNode().equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD))
        && functionName.equals(IoTDBConstant.COLUMN_COUNT);
  }

  public void addAttribute(String key, String value) {
    functionAttributes.put(key, value);
  }

  public void addExpression(Expression expression) {
    isConstantOperandCache = isConstantOperandCache && expression.isConstantOperand();
    isUserDefinedAggregationFunctionExpression =
        isUserDefinedAggregationFunctionExpression
            || expression.isUserDefinedAggregationFunctionExpression()
            || expression.isBuiltInAggregationFunctionExpression();
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

  @Override
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
  public void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions)
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
    expressionName2Executor.put(expressionString, new UDTFExecutor(functionName, zoneId));
  }

  @Override
  public TSDataType inferTypes(TypeProvider typeProvider) {
    final String expressionString = toString();

    if (!typeProvider.containsTypeInfoOf(expressionString)) {
      for (Expression expression : expressions) {
        expression.inferTypes(typeProvider);
      }

      if (!isBuiltInAggregationFunctionExpression()) {
        typeProvider.setType(
            expressionString,
            new UDTFTypeInferrer(functionName)
                .inferOutputType(
                    expressions.stream().map(Expression::toString).collect(Collectors.toList()),
                    expressions.stream()
                        .map(f -> typeProvider.getType(f.toString()))
                        .collect(Collectors.toList()),
                    functionAttributes));
      } else {
        if (expressions.size() != 1) {
          throw new SemanticException(
              String.format(
                  "Builtin aggregation function only accepts 1 input expression. Actual %d input expressions.",
                  expressions.size()));
        }
        typeProvider.setType(
            expressionString,
            TypeInferenceUtils.getAggrDataType(
                functionName, typeProvider.getType(expressions.get(0).toString())));
      }
    }

    return typeProvider.getType(expressionString);
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    for (Expression expression : expressions) {
      expression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    }
    inputColumnIndex = udtfPlan.getReaderIndexByExpressionName(toString());
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    for (Expression expression : expressions) {
      expression.bindInputLayerColumnIndexWithExpression(inputLocations);
    }

    final String digest = toString();
    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
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
      UDTFContext udtfContext,
      QueryDataSetInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      TypeProvider typeProvider,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      float memoryBudgetInMB = memoryAssigner.assign();
      Transformer transformer;
      if (isBuiltInAggregationFunctionExpression) {
        transformer =
            new TransparentTransformer(
                rawTimeSeriesInputLayer.constructValuePointReader(inputColumnIndex));
      } else {
        IntermediateLayer udfInputIntermediateLayer =
            constructUdfInputIntermediateLayer(
                queryId,
                udtfContext,
                rawTimeSeriesInputLayer,
                expressionIntermediateLayerMap,
                typeProvider,
                memoryAssigner);
        transformer =
            constructUdfTransformer(
                queryId, udtfContext, typeProvider, memoryAssigner, udfInputIntermediateLayer);
      }
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
      UDTFContext udtfContext,
      QueryDataSetInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      TypeProvider typeProvider,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    List<IntermediateLayer> intermediateLayers = new ArrayList<>();
    for (Expression expression : expressions) {
      intermediateLayers.add(
          expression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              typeProvider,
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
      UDTFContext udtfContext,
      TypeProvider typeProvider,
      LayerMemoryAssigner memoryAssigner,
      IntermediateLayer udfInputIntermediateLayer)
      throws QueryProcessException, IOException {
    UDTFExecutor executor = udtfContext.getExecutorByFunctionExpression(this);

    executor.beforeStart(
        queryId,
        memoryAssigner.assign(),
        expressions.stream().map(Expression::toString).collect(Collectors.toList()),
        expressions.stream()
            .map(f -> typeProvider.getType(f.toString()))
            .collect(Collectors.toList()),
        functionAttributes);

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

  @Override
  public IntermediateLayer constructIntermediateLayer(
      long queryId,
      UDTFContext udtfContext,
      QueryDataSetInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      float memoryBudgetInMB = memoryAssigner.assign();
      Transformer transformer;
      if (isBuiltInAggregationFunctionExpression) {
        transformer =
            new TransparentTransformer(
                rawTimeSeriesInputLayer.constructValuePointReader(inputColumnIndex));
      } else {
        IntermediateLayer udfInputIntermediateLayer =
            constructUdfInputIntermediateLayer(
                queryId,
                udtfContext,
                rawTimeSeriesInputLayer,
                expressionIntermediateLayerMap,
                expressionDataTypeMap,
                memoryAssigner);
        transformer =
            constructUdfTransformer(
                queryId,
                udtfContext,
                expressionDataTypeMap,
                memoryAssigner,
                udfInputIntermediateLayer);
      }
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

  // TODO: remove it after MPP finished
  @Deprecated
  private IntermediateLayer constructUdfInputIntermediateLayer(
      long queryId,
      UDTFContext udtfContext,
      QueryDataSetInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    List<IntermediateLayer> intermediateLayers = new ArrayList<>();
    for (Expression expression : expressions) {
      intermediateLayers.add(
          expression.constructIntermediateLayer(
              queryId,
              udtfContext,
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

  // TODO: remove it after MPP finished
  @Deprecated
  private UDFQueryTransformer constructUdfTransformer(
      long queryId,
      UDTFContext udtfContext,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner,
      IntermediateLayer udfInputIntermediateLayer)
      throws QueryProcessException, IOException {
    UDTFExecutor executor = udtfContext.getExecutorByFunctionExpression(this);

    executor.beforeStart(
        queryId,
        memoryAssigner.assign(),
        expressions.stream().map(Expression::toString).collect(Collectors.toList()),
        expressions.stream().map(expressionDataTypeMap::get).collect(Collectors.toList()),
        functionAttributes);

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
}
