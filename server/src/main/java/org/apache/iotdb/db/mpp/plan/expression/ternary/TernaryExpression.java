/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.db.mpp.plan.expression.ternary;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.IntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.SingleInputColumnMultiReferenceIntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.SingleInputColumnSingleReferenceIntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary.TernaryTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class TernaryExpression extends Expression {
  protected final Expression firstExpression;
  protected final Expression secondExpression;
  protected final Expression thirdExpression;
  // protected final boolean isNot = false;

  public Expression getFirstExpression() {
    return firstExpression;
  }

  public Expression getSecondExpression() {
    return secondExpression;
  }

  public Expression getThirdExpression() {
    return thirdExpression;
  }

  protected TernaryExpression(
      Expression firstExpression, Expression secondExpression, Expression thirdExpression) {
    this.firstExpression = firstExpression;
    this.secondExpression = secondExpression;
    this.thirdExpression = thirdExpression;
  }

  protected TernaryExpression(ByteBuffer byteBuffer) {
    this.firstExpression = Expression.deserialize(byteBuffer);
    this.secondExpression = Expression.deserialize(byteBuffer);
    this.thirdExpression = Expression.deserialize(byteBuffer);
  }

  @Override
  public boolean isConstantOperandInternal() {
    return firstExpression.isConstantOperand()
        && secondExpression.isConstantOperand()
        && thirdExpression.isConstantOperand();
  }

  @Override
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return !isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public boolean isUserDefinedAggregationFunctionExpression() {
    return firstExpression.isBuiltInAggregationFunctionExpression()
        || secondExpression.isBuiltInAggregationFunctionExpression()
        || thirdExpression.isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public List<Expression> getExpressions() {
    return Arrays.asList(firstExpression, secondExpression, thirdExpression);
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    firstExpression.collectPaths(pathSet);
    secondExpression.collectPaths(pathSet);
    thirdExpression.collectPaths(pathSet);
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    firstExpression.constructUdfExecutors(expressionName2Executor, zoneId);
    secondExpression.constructUdfExecutors(expressionName2Executor, zoneId);
    thirdExpression.constructUdfExecutors(expressionName2Executor, zoneId);
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    firstExpression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    secondExpression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    thirdExpression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    inputColumnIndex = udtfPlan.getReaderIndexByExpressionName(toString());
  }

  @Override
  public final void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    firstExpression.bindInputLayerColumnIndexWithExpression(inputLocations);
    secondExpression.bindInputLayerColumnIndexWithExpression(inputLocations);
    thirdExpression.bindInputLayerColumnIndexWithExpression(inputLocations);

    final String digest = toString();
    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    firstExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    secondExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    thirdExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    memoryAssigner.increaseExpressionReference(this);
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

      IntermediateLayer firstParentIntermediateLayer =
          firstExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      IntermediateLayer secondParentIntermediateLayer =
          secondExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      IntermediateLayer thirdParentIntermediateLayer =
          thirdExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      Transformer transformer =
          constructTransformer(
              firstParentIntermediateLayer.constructPointReader(),
              secondParentIntermediateLayer.constructPointReader(),
              thirdParentIntermediateLayer.constructPointReader());
      expressionDataTypeMap.put(this, transformer.getDataType());

      // SingleInputColumnMultiReferenceIntermediateLayer doesn't support ConstantLayerPointReader
      // yet. And since a ConstantLayerPointReader won't produce too much IO,
      // SingleInputColumnSingleReferenceIntermediateLayer could be a better choice.
      expressionIntermediateLayerMap.put(
          this,
          memoryAssigner.getReference(this) == 1 || isConstantOperand()
              ? new SingleInputColumnSingleReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer)
              : new SingleInputColumnMultiReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer));
    }

    return expressionIntermediateLayerMap.get(this);
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

      IntermediateLayer firstParentIntermediateLayer =
          firstExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              typeProvider,
              memoryAssigner);
      IntermediateLayer secondParentIntermediateLayer =
          secondExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              typeProvider,
              memoryAssigner);
      IntermediateLayer thirdParentIntermediateLayer =
          thirdExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              typeProvider,
              memoryAssigner);
      Transformer transformer =
          constructTransformer(
              firstParentIntermediateLayer.constructPointReader(),
              secondParentIntermediateLayer.constructPointReader(),
              thirdParentIntermediateLayer.constructPointReader());

      // SingleInputColumnMultiReferenceIntermediateLayer doesn't support ConstantLayerPointReader
      // yet. And since a ConstantLayerPointReader won't produce too much IO,
      // SingleInputColumnSingleReferenceIntermediateLayer could be a better choice.
      expressionIntermediateLayerMap.put(
          this,
          memoryAssigner.getReference(this) == 1 || isConstantOperand()
              ? new SingleInputColumnSingleReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer)
              : new SingleInputColumnMultiReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer));
    }

    return expressionIntermediateLayerMap.get(this);
  }

  protected abstract TernaryTransformer constructTransformer(
      LayerPointReader firstParentLayerPointReader,
      LayerPointReader secondParentLayerPointReader,
      LayerPointReader thirdParentLayerPointReader);

  @Override
  public final String getExpressionStringInternal() {
    StringBuilder builder = new StringBuilder();
    if (firstExpression.getExpressionType().getPriority()
        < this.getExpressionType().getPriority()) {
      builder.append("(").append(firstExpression.getExpressionString()).append(")");
    } else {
      builder.append(firstExpression.getExpressionString());
    }
    builder.append(" ").append(operator()).append(" ");
    if (secondExpression.getExpressionType().getPriority()
        < this.getExpressionType().getPriority()) {
      builder.append("(").append(secondExpression.getExpressionString()).append(")");
    } else {
      builder.append(secondExpression.getExpressionString());
    }
    builder.append(" ").append(operator()).append(" ");
    if (thirdExpression.getExpressionType().getPriority()
        < this.getExpressionType().getPriority()) {
      builder.append("(").append(thirdExpression.getExpressionString()).append(")");
    } else {
      builder.append(thirdExpression.getExpressionString());
    }
    return builder.toString();
  }

  protected abstract String operator();

  // protected abstract void reconstruct();

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    Expression.serialize(firstExpression, byteBuffer);
    Expression.serialize(secondExpression, byteBuffer);
    Expression.serialize(thirdExpression, byteBuffer);
  }
}
