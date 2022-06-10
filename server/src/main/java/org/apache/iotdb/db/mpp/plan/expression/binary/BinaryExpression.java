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

package org.apache.iotdb.db.mpp.plan.expression.binary;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
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
import org.apache.iotdb.db.mpp.transformation.dag.transformer.binary.BinaryTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BinaryExpression extends Expression {

  protected Expression leftExpression;
  protected Expression rightExpression;

  protected BinaryExpression(Expression leftExpression, Expression rightExpression) {
    this.leftExpression = leftExpression;
    this.rightExpression = rightExpression;
  }

  protected BinaryExpression(ByteBuffer byteBuffer) {
    this.leftExpression = Expression.deserialize(byteBuffer);
    this.rightExpression = Expression.deserialize(byteBuffer);
  }

  public Expression getLeftExpression() {
    return leftExpression;
  }

  public Expression getRightExpression() {
    return rightExpression;
  }

  public void setLeftExpression(Expression leftExpression) {
    this.leftExpression = leftExpression;
  }

  public void setRightExpression(Expression rightExpression) {
    this.rightExpression = rightExpression;
  }

  @Override
  public boolean isConstantOperandInternal() {
    return leftExpression.isConstantOperand() && rightExpression.isConstantOperand();
  }

  @Override
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return !isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public boolean isUserDefinedAggregationFunctionExpression() {
    return leftExpression.isBuiltInAggregationFunctionExpression()
        || rightExpression.isBuiltInAggregationFunctionExpression()
        || leftExpression.isUserDefinedAggregationFunctionExpression()
        || rightExpression.isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public List<Expression> getExpressions() {
    return Arrays.asList(leftExpression, rightExpression);
  }

  @Override
  public final void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    List<Expression> leftExpressions = new ArrayList<>();
    leftExpression.concat(prefixPaths, leftExpressions);

    List<Expression> rightExpressions = new ArrayList<>();
    rightExpression.concat(prefixPaths, rightExpressions);

    reconstruct(leftExpressions, rightExpressions, resultExpressions);
  }

  @Override
  public final void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    List<Expression> leftExpressions = new ArrayList<>();
    leftExpression.removeWildcards(wildcardsRemover, leftExpressions);

    List<Expression> rightExpressions = new ArrayList<>();
    rightExpression.removeWildcards(wildcardsRemover, rightExpressions);

    reconstruct(leftExpressions, rightExpressions, resultExpressions);
  }

  private void reconstruct(
      List<Expression> leftExpressions,
      List<Expression> rightExpressions,
      List<Expression> resultExpressions) {
    for (Expression le : leftExpressions) {
      for (Expression re : rightExpressions) {
        switch (operator()) {
          case "+":
            resultExpressions.add(new AdditionExpression(le, re));
            break;
          case "-":
            resultExpressions.add(new SubtractionExpression(le, re));
            break;
          case "*":
            resultExpressions.add(new MultiplicationExpression(le, re));
            break;
          case "/":
            resultExpressions.add(new DivisionExpression(le, re));
            break;
          case "%":
            resultExpressions.add(new ModuloExpression(le, re));
            break;
          case "<":
            resultExpressions.add(new LessThanExpression(le, re));
            break;
          case "<=":
            resultExpressions.add(new LessEqualExpression(le, re));
            break;
          case ">":
            resultExpressions.add(new GreaterThanExpression(le, re));
            break;
          case ">=":
            resultExpressions.add(new GreaterEqualExpression(le, re));
            break;
          case "=":
            resultExpressions.add(new EqualToExpression(le, re));
            break;
          case "!=":
            resultExpressions.add(new NonEqualExpression(le, re));
            break;
          case "&":
            resultExpressions.add(new LogicAndExpression(le, re));
            break;
          case "|":
            resultExpressions.add(new LogicOrExpression(le, re));
            break;
          default:
            throw new UnsupportedOperationException();
        }
      }
    }
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    leftExpression.collectPaths(pathSet);
    rightExpression.collectPaths(pathSet);
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    leftExpression.constructUdfExecutors(expressionName2Executor, zoneId);
    rightExpression.constructUdfExecutors(expressionName2Executor, zoneId);
  }

  @Override
  public final void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    leftExpression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    rightExpression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    inputColumnIndex = udtfPlan.getReaderIndexByExpressionName(toString());
  }

  @Override
  public final void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    leftExpression.bindInputLayerColumnIndexWithExpression(inputLocations);
    rightExpression.bindInputLayerColumnIndexWithExpression(inputLocations);

    final String digest = toString();
    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    leftExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    rightExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
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

      IntermediateLayer leftParentIntermediateLayer =
          leftExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      IntermediateLayer rightParentIntermediateLayer =
          rightExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      Transformer transformer =
          constructTransformer(
              leftParentIntermediateLayer.constructPointReader(),
              rightParentIntermediateLayer.constructPointReader());
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

      IntermediateLayer leftParentIntermediateLayer =
          leftExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              typeProvider,
              memoryAssigner);
      IntermediateLayer rightParentIntermediateLayer =
          rightExpression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              typeProvider,
              memoryAssigner);
      Transformer transformer =
          constructTransformer(
              leftParentIntermediateLayer.constructPointReader(),
              rightParentIntermediateLayer.constructPointReader());

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

  protected abstract BinaryTransformer constructTransformer(
      LayerPointReader leftParentLayerPointReader, LayerPointReader rightParentLayerPointReader);

  @Override
  public final String getExpressionStringInternal() {
    StringBuilder builder = new StringBuilder();
    if (leftExpression.getExpressionType().getPriority() < this.getExpressionType().getPriority()) {
      builder.append("(").append(leftExpression.getExpressionString()).append(")");
    } else {
      builder.append(leftExpression.getExpressionString());
    }
    builder.append(" ").append(operator()).append(" ");
    if (rightExpression.getExpressionType().getPriority()
        < this.getExpressionType().getPriority()) {
      builder.append("(").append(rightExpression.getExpressionString()).append(")");
    } else {
      builder.append(rightExpression.getExpressionString());
    }

    return builder.toString();
  }

  protected abstract String operator();

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    Expression.serialize(leftExpression, byteBuffer);
    Expression.serialize(rightExpression, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(leftExpression, stream);
    Expression.serialize(rightExpression, stream);
  }
}
