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

package org.apache.iotdb.db.queryengine.plan.expression.visitor;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.BuiltInScalarFunctionHelperFactory;
import org.apache.iotdb.db.queryengine.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.queryengine.transformation.dag.intermediate.ConstantIntermediateLayer;
import org.apache.iotdb.db.queryengine.transformation.dag.intermediate.IntermediateLayer;
import org.apache.iotdb.db.queryengine.transformation.dag.intermediate.MultiInputLayer;
import org.apache.iotdb.db.queryengine.transformation.dag.intermediate.SingleInputMultiReferenceLayer;
import org.apache.iotdb.db.queryengine.transformation.dag.intermediate.SingleInputSingleReferenceLayer;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.ArithmeticAdditionTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.ArithmeticDivisionTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.ArithmeticModuloTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.ArithmeticMultiplicationTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.ArithmeticSubtractionTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.CompareEqualToTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.CompareGreaterEqualTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.CompareGreaterThanTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.CompareLessEqualTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.CompareLessThanTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.CompareNonEqualTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.LogicAndTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary.LogicOrTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.multi.MappableUDFQueryRowTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.multi.UDFQueryRowTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.multi.UDFQueryRowWindowTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.multi.UDFQueryTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.ternary.BetweenTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.ArithmeticNegationTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.InTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.IsNullTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.LikeTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.LogicNotTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.RegularTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.TransparentTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.udf.api.customizer.strategy.AccessStrategy;

import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Responsible for constructing {@link IntermediateLayer} through Expression. */
public class IntermediateLayerVisitor
    extends ExpressionVisitor<
        IntermediateLayer, IntermediateLayerVisitor.IntermediateLayerVisitorContext> {

  @Override
  public IntermediateLayer visitExpression(
      Expression expression, IntermediateLayerVisitorContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + expression.getClass().getName());
  }

  @Override
  public IntermediateLayer visitUnaryExpression(
      UnaryExpression unaryExpression, IntermediateLayerVisitorContext context) {
    if (!context.expressionIntermediateLayerMap.containsKey(unaryExpression)) {
      float memoryBudgetInMB = context.memoryAssigner.assign();

      IntermediateLayer intermediateLayer = this.process(unaryExpression.getExpression(), context);

      Transformer transformer =
          getConcreteUnaryTransformer(unaryExpression, intermediateLayer.constructReader());

      // SingleInputColumnMultiReferenceIntermediateLayer doesn't support ConstantLayerPointReader
      // yet. And since a ConstantLayerPointReader won't produce too much IO,
      // SingleInputColumnSingleReferenceIntermediateLayer could be a better choice.
      context.expressionIntermediateLayerMap.put(
          unaryExpression,
          context.memoryAssigner.getReference(unaryExpression) == 1
                  || unaryExpression.isConstantOperand()
              ? new SingleInputSingleReferenceLayer(
                  unaryExpression, context.queryId, memoryBudgetInMB, transformer)
              : new SingleInputMultiReferenceLayer(
                  unaryExpression, context.queryId, memoryBudgetInMB, transformer));
    }

    return context.expressionIntermediateLayerMap.get(unaryExpression);
  }

  @Override
  public IntermediateLayer visitBinaryExpression(
      BinaryExpression binaryExpression, IntermediateLayerVisitorContext context) {
    if (!context.expressionIntermediateLayerMap.containsKey(binaryExpression)) {
      float memoryBudgetInMB = context.memoryAssigner.assign();

      IntermediateLayer leftParentIntermediateLayer =
          this.process(binaryExpression.getLeftExpression(), context);
      IntermediateLayer rightParentIntermediateLayer =
          this.process(binaryExpression.getRightExpression(), context);

      Transformer transformer =
          getConcreteBinaryTransformer(
              binaryExpression,
              leftParentIntermediateLayer.constructReader(),
              rightParentIntermediateLayer.constructReader());

      // SingleInputColumnMultiReferenceIntermediateLayer doesn't support ConstantLayerPointReader
      // yet. And since a ConstantLayerPointReader won't produce too much IO,
      // SingleInputColumnSingleReferenceIntermediateLayer could be a better choice.
      context.expressionIntermediateLayerMap.put(
          binaryExpression,
          context.memoryAssigner.getReference(binaryExpression) == 1
                  || binaryExpression.isConstantOperand()
              ? new SingleInputSingleReferenceLayer(
                  binaryExpression, context.queryId, memoryBudgetInMB, transformer)
              : new SingleInputMultiReferenceLayer(
                  binaryExpression, context.queryId, memoryBudgetInMB, transformer));
    }

    return context.expressionIntermediateLayerMap.get(binaryExpression);
  }

  @Override
  public IntermediateLayer visitTernaryExpression(
      TernaryExpression ternaryExpression, IntermediateLayerVisitorContext context) {
    if (!context.expressionIntermediateLayerMap.containsKey(ternaryExpression)) {
      float memoryBudgetInMB = context.memoryAssigner.assign();

      IntermediateLayer firstParentIntermediateLayer =
          this.process(ternaryExpression.getFirstExpression(), context);
      IntermediateLayer secondParentIntermediateLayer =
          this.process(ternaryExpression.getSecondExpression(), context);
      IntermediateLayer thirdParentIntermediateLayer =
          this.process(ternaryExpression.getThirdExpression(), context);
      Transformer transformer =
          getConcreteTernaryTransformer(
              ternaryExpression,
              firstParentIntermediateLayer.constructReader(),
              secondParentIntermediateLayer.constructReader(),
              thirdParentIntermediateLayer.constructReader());

      // SingleInputColumnMultiReferenceIntermediateLayer doesn't support ConstantLayerPointReader
      // yet. And since a ConstantLayerPointReader won't produce too much IO,
      // SingleInputColumnSingleReferenceIntermediateLayer could be a better choice.
      context.expressionIntermediateLayerMap.put(
          ternaryExpression,
          context.memoryAssigner.getReference(ternaryExpression) == 1
                  || ternaryExpression.isConstantOperand()
              ? new SingleInputSingleReferenceLayer(
                  ternaryExpression, context.queryId, memoryBudgetInMB, transformer)
              : new SingleInputMultiReferenceLayer(
                  ternaryExpression, context.queryId, memoryBudgetInMB, transformer));
    }

    return context.expressionIntermediateLayerMap.get(ternaryExpression);
  }

  @Override
  public IntermediateLayer visitFunctionExpression(
      FunctionExpression functionExpression, IntermediateLayerVisitorContext context) {
    if (!context.expressionIntermediateLayerMap.containsKey(functionExpression)) {
      float memoryBudgetInMB = context.memoryAssigner.assign();
      Transformer transformer;
      if (functionExpression.isAggregationFunctionExpression()) {
        transformer =
            new TransparentTransformer(
                context.rawTimeSeriesInputLayer.constructValueReader(
                    functionExpression.getInputColumnIndex()));
      } else if (functionExpression.isBuiltInScalarFunctionExpression()) {
        transformer = getBuiltInScalarFunctionTransformer(functionExpression, context);
      } else {
        try {
          IntermediateLayer udfInputIntermediateLayer =
              constructUdfInputIntermediateLayer(functionExpression, context);
          transformer = getUdfTransformer(functionExpression, context, udfInputIntermediateLayer);
        } catch (QueryProcessException | IOException e) {
          throw new RuntimeException(e);
        }
      }
      context.expressionIntermediateLayerMap.put(
          functionExpression,
          context.memoryAssigner.getReference(functionExpression) == 1
              ? new SingleInputSingleReferenceLayer(
                  functionExpression, context.queryId, memoryBudgetInMB, transformer)
              : new SingleInputMultiReferenceLayer(
                  functionExpression, context.queryId, memoryBudgetInMB, transformer));
    }

    return context.expressionIntermediateLayerMap.get(functionExpression);
  }

  private Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, IntermediateLayerVisitorContext context) {

    LayerReader childReader =
        this.process(expression.getExpressions().get(0), context).constructReader();
    return BuiltInScalarFunctionHelperFactory.createHelper(expression.getFunctionName())
        .getBuiltInScalarFunctionTransformer(expression, childReader);
  }

  @Override
  public IntermediateLayer visitTimeStampOperand(
      TimestampOperand timestampOperand, IntermediateLayerVisitorContext context) {
    if (!context.expressionIntermediateLayerMap.containsKey(timestampOperand)) {
      float memoryBudgetInMB = context.memoryAssigner.assign();

      LayerReader parentLayerReader = context.rawTimeSeriesInputLayer.constructTimeReader();

      context.expressionIntermediateLayerMap.put(
          timestampOperand,
          context.memoryAssigner.getReference(timestampOperand) == 1
              ? new SingleInputSingleReferenceLayer(
                  timestampOperand, context.queryId, memoryBudgetInMB, parentLayerReader)
              : new SingleInputMultiReferenceLayer(
                  timestampOperand, context.queryId, memoryBudgetInMB, parentLayerReader));
    }

    return context.expressionIntermediateLayerMap.get(timestampOperand);
  }

  @Override
  public IntermediateLayer visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, IntermediateLayerVisitorContext context) {
    if (!context.expressionIntermediateLayerMap.containsKey(timeSeriesOperand)) {
      float memoryBudgetInMB = context.memoryAssigner.assign();

      LayerReader parentLayerReader =
          context.rawTimeSeriesInputLayer.constructValueReader(
              timeSeriesOperand.getInputColumnIndex());

      context.expressionIntermediateLayerMap.put(
          timeSeriesOperand,
          context.memoryAssigner.getReference(timeSeriesOperand) == 1
              ? new SingleInputSingleReferenceLayer(
                  timeSeriesOperand, context.queryId, memoryBudgetInMB, parentLayerReader)
              : new SingleInputMultiReferenceLayer(
                  timeSeriesOperand, context.queryId, memoryBudgetInMB, parentLayerReader));
    }

    return context.expressionIntermediateLayerMap.get(timeSeriesOperand);
  }

  @Override
  public IntermediateLayer visitConstantOperand(
      ConstantOperand constantOperand, IntermediateLayerVisitorContext context) {
    if (!context.expressionIntermediateLayerMap.containsKey(constantOperand)) {
      try {
        IntermediateLayer intermediateLayer =
            new ConstantIntermediateLayer(
                constantOperand, context.queryId, context.memoryAssigner.assign());
        context.expressionIntermediateLayerMap.put(constantOperand, intermediateLayer);
      } catch (QueryProcessException e) {
        throw new RuntimeException(e);
      }
    }

    return context.expressionIntermediateLayerMap.get(constantOperand);
  }

  @Override
  public IntermediateLayer visitCaseWhenThenExpression(
      CaseWhenThenExpression caseWhenThenExpression, IntermediateLayerVisitorContext context) {
    throw new UnsupportedOperationException("CASE expression cannot be used with non-mappable UDF");
  }

  private Transformer getConcreteUnaryTransformer(Expression expression, LayerReader parentReader) {
    switch (expression.getExpressionType()) {
      case IN:
        InExpression inExpression = (InExpression) expression;
        return new InTransformer(parentReader, inExpression.isNotIn(), inExpression.getValues());
      case IS_NULL:
        IsNullExpression isNullExpression = (IsNullExpression) expression;
        return new IsNullTransformer(parentReader, isNullExpression.isNot());
      case LOGIC_NOT:
        return new LogicNotTransformer(parentReader);
      case NEGATION:
        return new ArithmeticNegationTransformer(parentReader);
      case LIKE:
        LikeExpression likeExpression = (LikeExpression) expression;
        LikePattern pattern =
            LikePattern.compile(likeExpression.getPattern(), likeExpression.getEscape());
        return new LikeTransformer(parentReader, pattern);
      case REGEXP:
        RegularExpression regularExpression = (RegularExpression) expression;
        return new RegularTransformer(parentReader, regularExpression.getPattern());
      default:
        throw new UnsupportedOperationException(
            "Unsupported Expression Type: " + expression.getExpressionType());
    }
  }

  private Transformer getConcreteBinaryTransformer(
      Expression expression,
      LayerReader leftParentLayerReader,
      LayerReader rightParentLayerReader) {
    switch (expression.getExpressionType()) {
      case ADDITION:
        return new ArithmeticAdditionTransformer(leftParentLayerReader, rightParentLayerReader);
      case SUBTRACTION:
        return new ArithmeticSubtractionTransformer(leftParentLayerReader, rightParentLayerReader);
      case MULTIPLICATION:
        return new ArithmeticMultiplicationTransformer(
            leftParentLayerReader, rightParentLayerReader);
      case DIVISION:
        return new ArithmeticDivisionTransformer(leftParentLayerReader, rightParentLayerReader);
      case MODULO:
        return new ArithmeticModuloTransformer(leftParentLayerReader, rightParentLayerReader);
      case EQUAL_TO:
        return new CompareEqualToTransformer(leftParentLayerReader, rightParentLayerReader);
      case NON_EQUAL:
        return new CompareNonEqualTransformer(leftParentLayerReader, rightParentLayerReader);
      case GREATER_THAN:
        return new CompareGreaterThanTransformer(leftParentLayerReader, rightParentLayerReader);
      case GREATER_EQUAL:
        return new CompareGreaterEqualTransformer(leftParentLayerReader, rightParentLayerReader);
      case LESS_THAN:
        return new CompareLessThanTransformer(leftParentLayerReader, rightParentLayerReader);
      case LESS_EQUAL:
        return new CompareLessEqualTransformer(leftParentLayerReader, rightParentLayerReader);
      case LOGIC_AND:
        return new LogicAndTransformer(leftParentLayerReader, rightParentLayerReader);
      case LOGIC_OR:
        return new LogicOrTransformer(leftParentLayerReader, rightParentLayerReader);
      default:
        throw new UnsupportedOperationException(
            "Unsupported Expression Type: " + expression.getExpressionType());
    }
  }

  private Transformer getConcreteTernaryTransformer(
      Expression expression,
      LayerReader firstParentLayerReader,
      LayerReader secondParentLayerReader,
      LayerReader thirdParentLayerReader) {
    if (expression.getExpressionType() == ExpressionType.BETWEEN) {
      BetweenExpression betweenExpression = (BetweenExpression) expression;
      return new BetweenTransformer(
          firstParentLayerReader,
          secondParentLayerReader,
          thirdParentLayerReader,
          betweenExpression.isNotBetween());
    }
    throw new UnsupportedOperationException(
        "Unsupported Expression Type: " + expression.getExpressionType());
  }

  private UDFQueryTransformer getUdfTransformer(
      FunctionExpression functionExpression,
      IntermediateLayerVisitorContext context,
      IntermediateLayer udfInputIntermediateLayer)
      throws QueryProcessException, IOException {
    UDTFExecutor executor = context.udtfContext.getExecutorByFunctionExpression(functionExpression);
    List<Expression> expressions = functionExpression.getExpressions();

    executor.beforeStart(
        context.queryId,
        context.memoryAssigner.assign(),
        expressions.stream().map(Expression::getExpressionString).collect(Collectors.toList()),
        expressions.stream().map(context::getType).collect(Collectors.toList()),
        functionExpression.getFunctionAttributes());

    AccessStrategy accessStrategy = executor.getConfigurations().getAccessStrategy();
    switch (accessStrategy.getAccessStrategyType()) {
      case MAPPABLE_ROW_BY_ROW:
        return new MappableUDFQueryRowTransformer(
            udfInputIntermediateLayer.constructReader(), executor);
      case ROW_BY_ROW:
        return new UDFQueryRowTransformer(udfInputIntermediateLayer.constructReader(), executor);
      case SLIDING_SIZE_WINDOW:
      case SLIDING_TIME_WINDOW:
      case SESSION_TIME_WINDOW:
      case STATE_WINDOW:
        return new UDFQueryRowWindowTransformer(
            udfInputIntermediateLayer.constructRowWindowReader(
                accessStrategy, context.memoryAssigner.assign()),
            executor);
      default:
        throw new UnsupportedOperationException("Unsupported transformer access strategy");
    }
  }

  private IntermediateLayer constructUdfInputIntermediateLayer(
      FunctionExpression functionExpression, IntermediateLayerVisitorContext context) {
    List<IntermediateLayer> intermediateLayers = new ArrayList<>();
    for (Expression expression : functionExpression.getExpressions()) {
      intermediateLayers.add(this.process(expression, context));
    }
    return intermediateLayers.size() == 1
        ? intermediateLayers.get(0)
        : new MultiInputLayer(
            functionExpression,
            context.queryId,
            context.memoryAssigner.assign(),
            intermediateLayers.stream()
                .map(IntermediateLayer::constructReader)
                .collect(Collectors.toList()));
  }

  public static class IntermediateLayerVisitorContext {
    String queryId;

    UDTFContext udtfContext;

    QueryDataSetInputLayer rawTimeSeriesInputLayer;

    Map<Expression, IntermediateLayer> expressionIntermediateLayerMap;

    Map<NodeRef<Expression>, TSDataType> expressionTypes;

    LayerMemoryAssigner memoryAssigner;

    public IntermediateLayerVisitorContext(
        String queryId,
        UDTFContext udtfContext,
        QueryDataSetInputLayer rawTimeSeriesInputLayer,
        Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
        Map<NodeRef<Expression>, TSDataType> expressionTypes,
        LayerMemoryAssigner memoryAssigner) {
      this.queryId = queryId;
      this.udtfContext = udtfContext;
      this.rawTimeSeriesInputLayer = rawTimeSeriesInputLayer;
      this.expressionIntermediateLayerMap = expressionIntermediateLayerMap;
      this.expressionTypes = expressionTypes;
      this.memoryAssigner = memoryAssigner;
    }

    public TSDataType getType(Expression expression) {
      return expressionTypes.get(NodeRef.of(expression));
    }
  }
}
