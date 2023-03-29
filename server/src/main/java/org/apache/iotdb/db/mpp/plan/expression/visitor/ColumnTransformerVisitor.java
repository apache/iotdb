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

package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.BuiltInScalarFunctionHelperFactory;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.ArithmeticAdditionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.ArithmeticDivisionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.ArithmeticModuloColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.ArithmeticMultiplicationColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.ArithmeticSubtractionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.CompareEqualToColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.CompareGreaterEqualColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.CompareLessEqualColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.CompareLessThanColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.CompareNonEqualColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.LogicAndColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.LogicOrColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.NullColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.TimeColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.multi.MappableUDFColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.ternary.BetweenColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.ArithmeticNegationColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.InColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.IsNullColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.LogicNotColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.RegularColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.db.mpp.transformation.dag.util.TransformUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.mpp.plan.expression.ExpressionType.BETWEEN;

/** Responsible for constructing {@link ColumnTransformer} through Expression. */
public class ColumnTransformerVisitor
    extends ExpressionVisitor<
        ColumnTransformer, ColumnTransformerVisitor.ColumnTransformerVisitorContext> {

  @Override
  public ColumnTransformer visitExpression(
      Expression expression, ColumnTransformerVisitorContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + expression.getClass().getName());
  }

  @Override
  public ColumnTransformer visitUnaryExpression(
      UnaryExpression unaryExpression, ColumnTransformerVisitorContext context) {
    if (!context.cache.containsKey(unaryExpression)) {
      if (context.hasSeen.containsKey(unaryExpression)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                TypeFactory.getType(context.getType(unaryExpression)),
                context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(unaryExpression);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(context.getType(unaryExpression));
        context.cache.put(unaryExpression, identity);
      } else {
        ColumnTransformer childColumnTransformer =
            this.process(unaryExpression.getExpression(), context);
        context.cache.put(
            unaryExpression,
            getConcreteUnaryColumnTransformer(
                unaryExpression,
                childColumnTransformer,
                TypeFactory.getType(context.getType(unaryExpression))));
      }
    }
    ColumnTransformer res = context.cache.get(unaryExpression);
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitBinaryExpression(
      BinaryExpression binaryExpression, ColumnTransformerVisitorContext context) {
    if (!context.cache.containsKey(binaryExpression)) {
      if (context.hasSeen.containsKey(binaryExpression)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                TypeFactory.getType(context.getType(binaryExpression)),
                context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(binaryExpression);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(context.getType(binaryExpression));
        context.cache.put(binaryExpression, identity);
      } else {
        ColumnTransformer leftColumnTransformer =
            this.process(binaryExpression.getLeftExpression(), context);
        ColumnTransformer rightColumnTransformer =
            this.process(binaryExpression.getRightExpression(), context);
        context.cache.put(
            binaryExpression,
            getConcreteBinaryColumnTransformer(
                binaryExpression,
                leftColumnTransformer,
                rightColumnTransformer,
                TypeFactory.getType(context.getType(binaryExpression))));
      }
    }

    ColumnTransformer res = context.cache.get(binaryExpression);
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitTernaryExpression(
      TernaryExpression ternaryExpression, ColumnTransformerVisitorContext context) {
    if (!context.cache.containsKey(ternaryExpression)) {
      if (context.hasSeen.containsKey(ternaryExpression)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                TypeFactory.getType(context.getType(ternaryExpression)),
                context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(ternaryExpression);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(context.getType(ternaryExpression));
        context.cache.put(ternaryExpression, identity);
      } else {
        ColumnTransformer firstColumnTransformer =
            this.process(ternaryExpression.getFirstExpression(), context);
        ColumnTransformer secondColumnTransformer =
            this.process(ternaryExpression.getSecondExpression(), context);
        ColumnTransformer thirdColumnTransformer =
            this.process(ternaryExpression.getThirdExpression(), context);
        context.cache.put(
            ternaryExpression,
            getConcreteTernaryTransformer(
                ternaryExpression,
                firstColumnTransformer,
                secondColumnTransformer,
                thirdColumnTransformer,
                TypeFactory.getType(context.getType(ternaryExpression))));
      }
    }

    ColumnTransformer res = context.cache.get(ternaryExpression);
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitFunctionExpression(
      FunctionExpression functionExpression, ColumnTransformerVisitorContext context) {
    List<Expression> expressions = functionExpression.getExpressions();
    if (!context.cache.containsKey(functionExpression)) {
      if (context.hasSeen.containsKey(functionExpression)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                TypeFactory.getType(context.getType(functionExpression)),
                context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(functionExpression);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.inputDataTypes.add(context.getType(functionExpression));
        context.leafList.add(identity);
        context.cache.put(functionExpression, identity);
      } else {
        if (functionExpression.isBuiltInAggregationFunctionExpression()) {
          IdentityColumnTransformer identity =
              new IdentityColumnTransformer(
                  TypeFactory.getType(context.getType(functionExpression)),
                  context
                      .inputLocations
                      .get(functionExpression.getExpressionString())
                      .get(0)
                      .getValueColumnIndex());
          context.leafList.add(identity);
          context.cache.put(functionExpression, identity);
        } else if (functionExpression.isBuiltInScalarFunction()) {
          context.cache.put(
              functionExpression, getBuiltInScalarFunctionTransformer(functionExpression, context));
        } else {
          ColumnTransformer[] inputColumnTransformers =
              expressions.stream()
                  .map(expression -> this.process(expression, context))
                  .toArray(ColumnTransformer[]::new);

          TSDataType[] inputTransformerDataTypes =
              expressions.stream().map(context::getType).toArray(TSDataType[]::new);

          UDTFExecutor executor =
              context.udtfContext.getExecutorByFunctionExpression(functionExpression);

          // Mappable UDF does not need PointCollector, so memoryBudget and queryId is not
          // needed.
          executor.beforeStart(
              0,
              0,
              expressions.stream().map(Expression::toString).collect(Collectors.toList()),
              expressions.stream().map(context::getType).collect(Collectors.toList()),
              functionExpression.getFunctionAttributes());

          context.cache.put(
              functionExpression,
              new MappableUDFColumnTransformer(
                  TypeFactory.getType(context.getType(functionExpression)),
                  inputColumnTransformers,
                  inputTransformerDataTypes,
                  context.udtfContext.getExecutorByFunctionExpression(functionExpression)));
        }
      }
    }
    ColumnTransformer res = context.cache.get(functionExpression);
    res.addReferenceCount();
    return res;
  }

  private ColumnTransformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, ColumnTransformerVisitorContext context) {
    ColumnTransformer childColumnTransformer =
        this.process(expression.getExpressions().get(0), context);
    return BuiltInScalarFunctionHelperFactory.createHelper(expression.getFunctionName())
        .getBuiltInScalarFunctionColumnTransformer(expression, childColumnTransformer);
  }

  @Override
  public ColumnTransformer visitTimeStampOperand(
      TimestampOperand timestampOperand, ColumnTransformerVisitorContext context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            timestampOperand,
            e -> {
              TimeColumnTransformer timeColumnTransformer =
                  new TimeColumnTransformer(TypeFactory.getType(context.getType(timestampOperand)));
              context.leafList.add(timeColumnTransformer);
              return timeColumnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, ColumnTransformerVisitorContext context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            timeSeriesOperand,
            e -> {
              IdentityColumnTransformer identity =
                  new IdentityColumnTransformer(
                      TypeFactory.getType(context.getType(timeSeriesOperand)),
                      context
                          .inputLocations
                          .get(timeSeriesOperand.getExpressionString())
                          .get(0)
                          .getValueColumnIndex());
              context.leafList.add(identity);
              return identity;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitConstantOperand(
      ConstantOperand constantOperand, ColumnTransformerVisitorContext context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            constantOperand,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      TypeFactory.getType(context.getType(constantOperand)),
                      TransformUtils.transformConstantOperandToColumn(constantOperand));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitNullOperand(
      NullOperand nullOperand, ColumnTransformerVisitorContext context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            nullOperand,
            e -> {
              NullColumnTransformer columnTransformer = new NullColumnTransformer();
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  private ColumnTransformer getConcreteUnaryColumnTransformer(
      Expression expression, ColumnTransformer childColumnTransformer, Type returnType) {
    switch (expression.getExpressionType()) {
      case IN:
        InExpression inExpression = (InExpression) expression;
        return new InColumnTransformer(
            returnType, childColumnTransformer, inExpression.isNotIn(), inExpression.getValues());
      case IS_NULL:
        IsNullExpression isNullExpression = (IsNullExpression) expression;
        return new IsNullColumnTransformer(
            returnType, childColumnTransformer, isNullExpression.isNot());
      case LOGIC_NOT:
        return new LogicNotColumnTransformer(returnType, childColumnTransformer);
      case NEGATION:
        return new ArithmeticNegationColumnTransformer(returnType, childColumnTransformer);
      case LIKE:
        LikeExpression likeExpression = (LikeExpression) expression;
        return new RegularColumnTransformer(
            returnType, childColumnTransformer, likeExpression.getPattern());
      case REGEXP:
        RegularExpression regularExpression = (RegularExpression) expression;
        return new RegularColumnTransformer(
            returnType, childColumnTransformer, regularExpression.getPattern());
      default:
        throw new UnsupportedOperationException(
            "Unsupported Expression Type: " + expression.getExpressionType());
    }
  }

  private ColumnTransformer getConcreteBinaryColumnTransformer(
      Expression expression,
      ColumnTransformer leftColumnTransformer,
      ColumnTransformer rightColumnTransformer,
      Type returnType) {
    switch (expression.getExpressionType()) {
      case ADDITION:
        return new ArithmeticAdditionColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case SUBTRACTION:
        return new ArithmeticSubtractionColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case MULTIPLICATION:
        return new ArithmeticMultiplicationColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case DIVISION:
        return new ArithmeticDivisionColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case MODULO:
        return new ArithmeticModuloColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case EQUAL_TO:
        return new CompareEqualToColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case NON_EQUAL:
        return new CompareNonEqualColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case GREATER_THAN:
        return new CompareGreaterThanColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case GREATER_EQUAL:
        return new CompareGreaterEqualColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case LESS_THAN:
        return new CompareLessThanColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case LESS_EQUAL:
        return new CompareLessEqualColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case LOGIC_AND:
        return new LogicAndColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      case LOGIC_OR:
        return new LogicOrColumnTransformer(
            returnType, leftColumnTransformer, rightColumnTransformer);
      default:
        throw new UnsupportedOperationException(
            "Unsupported Expression Type: " + expression.getExpressionType());
    }
  }

  private ColumnTransformer getConcreteTernaryTransformer(
      Expression expression,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer,
      Type returnType) {
    if (expression.getExpressionType() == BETWEEN) {
      BetweenExpression betweenExpression = (BetweenExpression) expression;
      return new BetweenColumnTransformer(
          returnType,
          firstColumnTransformer,
          secondColumnTransformer,
          thirdColumnTransformer,
          betweenExpression.isNotBetween());
    } else {
      throw new UnsupportedOperationException(
          "Unsupported Expression Type: " + expression.getExpressionType());
    }
  }

  public static class ColumnTransformerVisitorContext {
    // UDTFContext of expression
    UDTFContext udtfContext;

    // TypeProvider of expression
    Map<NodeRef<Expression>, TSDataType> expressionTypes;

    // LeafColumnTransformer for LeafOperand
    List<LeafColumnTransformer> leafList;

    // Index of input column
    Map<String, List<InputLocation>> inputLocations;

    // cache for constructing ColumnTransformer tree
    Map<Expression, ColumnTransformer> cache;

    // Sub expressions that has been seen in filter
    Map<Expression, ColumnTransformer> hasSeen;

    // Common Transformer between filter and project
    List<ColumnTransformer> commonTransformerList;

    List<TSDataType> inputDataTypes;

    int originSize;

    public ColumnTransformerVisitorContext(
        UDTFContext udtfContext,
        Map<NodeRef<Expression>, TSDataType> expressionTypes,
        List<LeafColumnTransformer> leafList,
        Map<String, List<InputLocation>> inputLocations,
        Map<Expression, ColumnTransformer> cache,
        Map<Expression, ColumnTransformer> hasSeen,
        List<ColumnTransformer> commonTransformerList,
        List<TSDataType> inputDataTypes,
        int originSize) {
      this.udtfContext = udtfContext;
      this.expressionTypes = expressionTypes;
      this.leafList = leafList;
      this.inputLocations = inputLocations;
      this.cache = cache;
      this.hasSeen = hasSeen;
      this.commonTransformerList = commonTransformerList;
      this.inputDataTypes = inputDataTypes;
      this.originSize = originSize;
    }

    public TSDataType getType(Expression expression) {
      return expressionTypes.get(NodeRef.of(expression));
    }
  }
}
