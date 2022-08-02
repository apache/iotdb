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

import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
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

/** Responsible for constructing {@link ColumnTransformer} through Expression. */
public class ColumnTransformerVisitor extends ExpressionVisitor<ColumnTransformer> {

  private final UDTFContext udtfContext;

  private final TypeProvider typeProvider;

  private final List<LeafColumnTransformer> leafList;

  private final Map<String, List<InputLocation>> inputLocations;

  private final Map<Expression, ColumnTransformer> cache;

  private final Map<Expression, ColumnTransformer> hasSeen;

  private final List<ColumnTransformer> commonTransformerList;

  private final List<TSDataType> inputDataTypes;

  private final int originSize;

  public ColumnTransformerVisitor(
      UDTFContext udtfContext,
      TypeProvider typeProvider,
      List<LeafColumnTransformer> leafList,
      Map<String, List<InputLocation>> inputLocations,
      Map<Expression, ColumnTransformer> cache,
      Map<Expression, ColumnTransformer> hasSeen,
      List<ColumnTransformer> commonTransformerList,
      List<TSDataType> inputDataTypes,
      int originSize) {
    this.udtfContext = udtfContext;
    this.typeProvider = typeProvider;
    this.leafList = leafList;
    this.inputLocations = inputLocations;
    this.cache = cache;
    this.hasSeen = hasSeen;
    this.commonTransformerList = commonTransformerList;
    this.inputDataTypes = inputDataTypes;
    this.originSize = originSize;
  }

  @Override
  public ColumnTransformer visitExpression(Expression expression) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + expression.getClass().getName());
  }

  @Override
  public ColumnTransformer visitUnaryExpression(UnaryExpression unaryExpression) {
    if (!cache.containsKey(unaryExpression)) {
      if (hasSeen.containsKey(unaryExpression)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                TypeFactory.getType(typeProvider.getType(unaryExpression.getExpressionString())),
                originSize + commonTransformerList.size());
        ColumnTransformer columnTransformer = hasSeen.get(unaryExpression);
        columnTransformer.addReferenceCount();
        commonTransformerList.add(columnTransformer);
        leafList.add(identity);
        inputDataTypes.add(typeProvider.getType(unaryExpression.getExpressionString()));
        cache.put(unaryExpression, identity);
      } else {
        ColumnTransformer childColumnTransformer = this.process(unaryExpression.getExpression());
        cache.put(
            unaryExpression,
            getConcreteUnaryColumnTransformer(
                unaryExpression,
                childColumnTransformer,
                TypeFactory.getType(typeProvider.getType(unaryExpression.getExpressionString()))));
      }
    }
    ColumnTransformer res = cache.get(unaryExpression);
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitBinaryExpression(BinaryExpression binaryExpression) {
    if (!cache.containsKey(binaryExpression)) {
      if (hasSeen.containsKey(binaryExpression)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                TypeFactory.getType(typeProvider.getType(binaryExpression.getExpressionString())),
                originSize + commonTransformerList.size());
        ColumnTransformer columnTransformer = hasSeen.get(binaryExpression);
        columnTransformer.addReferenceCount();
        commonTransformerList.add(columnTransformer);
        leafList.add(identity);
        inputDataTypes.add(typeProvider.getType(binaryExpression.getExpressionString()));
        cache.put(binaryExpression, identity);
      } else {
        ColumnTransformer leftColumnTransformer =
            this.process(binaryExpression.getLeftExpression());
        ColumnTransformer rightColumnTransformer =
            this.process(binaryExpression.getRightExpression());
        cache.put(
            binaryExpression,
            getConcreteBinaryColumnTransformer(
                binaryExpression,
                leftColumnTransformer,
                rightColumnTransformer,
                TypeFactory.getType(typeProvider.getType(binaryExpression.getExpressionString()))));
      }
    }

    ColumnTransformer res = cache.get(binaryExpression);
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitTernaryExpression(TernaryExpression ternaryExpression) {
    if (!cache.containsKey(ternaryExpression)) {
      if (hasSeen.containsKey(ternaryExpression)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                TypeFactory.getType(typeProvider.getType(ternaryExpression.getExpressionString())),
                originSize + commonTransformerList.size());
        ColumnTransformer columnTransformer = hasSeen.get(ternaryExpression);
        columnTransformer.addReferenceCount();
        commonTransformerList.add(columnTransformer);
        leafList.add(identity);
        inputDataTypes.add(typeProvider.getType(ternaryExpression.getExpressionString()));
        cache.put(ternaryExpression, identity);
      } else {
        ColumnTransformer firstColumnTransformer =
            this.process(ternaryExpression.getFirstExpression());
        ColumnTransformer secondColumnTransformer =
            this.process(ternaryExpression.getSecondExpression());
        ColumnTransformer thirdColumnTransformer =
            this.process(ternaryExpression.getThirdExpression());
        cache.put(
            ternaryExpression,
            getConcreteTernaryTransformer(
                ternaryExpression,
                firstColumnTransformer,
                secondColumnTransformer,
                thirdColumnTransformer,
                TypeFactory.getType(
                    typeProvider.getType(ternaryExpression.getExpressionString()))));
      }
    }

    ColumnTransformer res = cache.get(ternaryExpression);
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitFunctionExpression(FunctionExpression functionExpression) {
    List<Expression> expressions = functionExpression.getExpressions();
    if (!cache.containsKey(functionExpression)) {
      if (hasSeen.containsKey(functionExpression)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                TypeFactory.getType(typeProvider.getType(functionExpression.getExpressionString())),
                originSize + commonTransformerList.size());
        ColumnTransformer columnTransformer = hasSeen.get(functionExpression);
        columnTransformer.addReferenceCount();
        commonTransformerList.add(columnTransformer);
        inputDataTypes.add(typeProvider.getType(functionExpression.getExpressionString()));
        leafList.add(identity);
        cache.put(functionExpression, identity);
      } else {
        if (functionExpression.isBuiltInAggregationFunctionExpression()) {
          IdentityColumnTransformer identity =
              new IdentityColumnTransformer(
                  TypeFactory.getType(
                      typeProvider.getType(functionExpression.getExpressionString())),
                  inputLocations
                      .get(functionExpression.getExpressionString())
                      .get(0)
                      .getValueColumnIndex());
          leafList.add(identity);
          cache.put(functionExpression, identity);
        } else {
          ColumnTransformer[] inputColumnTransformers =
              expressions.stream().map(this::process).toArray(ColumnTransformer[]::new);

          TSDataType[] inputTransformerDataTypes =
              expressions.stream()
                  .map(expression -> expression.inferTypes(typeProvider))
                  .toArray(TSDataType[]::new);

          UDTFExecutor executor = udtfContext.getExecutorByFunctionExpression(functionExpression);

          // Mappable UDF does not need PointCollector, so memoryBudget and queryId is not
          // needed.
          executor.beforeStart(
              0,
              0,
              expressions.stream().map(Expression::toString).collect(Collectors.toList()),
              expressions.stream()
                  .map(f -> typeProvider.getType(f.toString()))
                  .collect(Collectors.toList()),
              functionExpression.getFunctionAttributes());

          cache.put(
              functionExpression,
              new MappableUDFColumnTransformer(
                  TypeFactory.getType(
                      typeProvider.getType(functionExpression.getExpressionString())),
                  inputColumnTransformers,
                  inputTransformerDataTypes,
                  udtfContext.getExecutorByFunctionExpression(functionExpression)));
        }
      }
    }
    ColumnTransformer res = cache.get(functionExpression);
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitTimeStampOperand(TimestampOperand timestampOperand) {
    ColumnTransformer res =
        cache.computeIfAbsent(
            timestampOperand,
            e -> {
              TimeColumnTransformer timeColumnTransformer =
                  new TimeColumnTransformer(
                      TypeFactory.getType(
                          typeProvider.getType(timestampOperand.getExpressionString())));
              leafList.add(timeColumnTransformer);
              return timeColumnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand) {
    ColumnTransformer res =
        cache.computeIfAbsent(
            timeSeriesOperand,
            e -> {
              IdentityColumnTransformer identity =
                  new IdentityColumnTransformer(
                      TypeFactory.getType(
                          typeProvider.getType(timeSeriesOperand.getExpressionString())),
                      inputLocations
                          .get(timeSeriesOperand.getExpressionString())
                          .get(0)
                          .getValueColumnIndex());
              leafList.add(identity);
              return identity;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  public ColumnTransformer visitConstantOperand(ConstantOperand constantOperand) {
    ColumnTransformer res =
        cache.computeIfAbsent(
            constantOperand,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      TypeFactory.getType(
                          typeProvider.getType(constantOperand.getExpressionString())),
                      TransformUtils.transformConstantOperandToColumn(constantOperand));
              leafList.add(columnTransformer);
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
    switch (expression.getExpressionType()) {
      case BETWEEN:
        BetweenExpression betweenExpression = (BetweenExpression) expression;
        return new BetweenColumnTransformer(
            returnType,
            firstColumnTransformer,
            secondColumnTransformer,
            thirdColumnTransformer,
            betweenExpression.isNotBetween());
      default:
        throw new UnsupportedOperationException(
            "Unsupported Expression Type: " + expression.getExpressionType());
    }
  }
}
