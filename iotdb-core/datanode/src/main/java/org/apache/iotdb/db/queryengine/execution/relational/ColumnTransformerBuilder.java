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

package org.apache.iotdb.db.queryengine.execution.relational;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticAdditionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticDivisionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticModuloColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticMultiplicationColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticSubtractionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareEqualToColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareGreaterEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareLessEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareLessThanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareNonEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.NullColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.TimeColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.LogicalAndMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.LogicalOrMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.ArithmeticNegationColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.InColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.IsNullColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.LogicNotColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.RegularColumnTransformer;
import org.apache.iotdb.db.relational.sql.tree.ArithmeticBinaryExpression;
import org.apache.iotdb.db.relational.sql.tree.ArithmeticUnaryExpression;
import org.apache.iotdb.db.relational.sql.tree.AstVisitor;
import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.BinaryLiteral;
import org.apache.iotdb.db.relational.sql.tree.BooleanLiteral;
import org.apache.iotdb.db.relational.sql.tree.Cast;
import org.apache.iotdb.db.relational.sql.tree.CoalesceExpression;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.CurrentDatabase;
import org.apache.iotdb.db.relational.sql.tree.CurrentTime;
import org.apache.iotdb.db.relational.sql.tree.CurrentUser;
import org.apache.iotdb.db.relational.sql.tree.DecimalLiteral;
import org.apache.iotdb.db.relational.sql.tree.DoubleLiteral;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.relational.sql.tree.IfExpression;
import org.apache.iotdb.db.relational.sql.tree.InListExpression;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.relational.sql.tree.Literal;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.LongLiteral;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.NullLiteral;
import org.apache.iotdb.db.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SimpleCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.StringLiteral;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;
import org.apache.iotdb.db.relational.sql.tree.Trim;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.utils.RegexUtils.compileRegex;
import static org.apache.tsfile.utils.RegexUtils.parseLikePatternToRegex;

public class ColumnTransformerBuilder
    extends AstVisitor<ColumnTransformer, ColumnTransformerBuilder.Context> {

  private static final String UNSUPPORTED_EXPRESSION = "Unsupported expression: %s";

  @Override
  public ColumnTransformer visitExpression(Expression expression, Context context) {
    throw new IllegalArgumentException(
        String.format(UNSUPPORTED_EXPRESSION, expression.getClass().getSimpleName()));
  }

  @Override
  protected ColumnTransformer visitArithmeticBinary(
      ArithmeticBinaryExpression node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            n -> {
              if (context.hasSeen.containsKey(node)) {
                IdentityColumnTransformer identity =
                    new IdentityColumnTransformer(
                        DOUBLE, context.originSize + context.commonTransformerList.size());
                ColumnTransformer columnTransformer = context.hasSeen.get(node);
                columnTransformer.addReferenceCount();
                context.commonTransformerList.add(columnTransformer);
                context.leafList.add(identity);
                context.inputDataTypes.add(TSDataType.DOUBLE);
                return identity;
              } else {
                ColumnTransformer left = process(node.getLeft(), context);
                ColumnTransformer right = process(node.getRight(), context);
                switch (node.getOperator()) {
                  case ADD:
                    return new ArithmeticAdditionColumnTransformer(DOUBLE, left, right);
                  case SUBTRACT:
                    return new ArithmeticSubtractionColumnTransformer(DOUBLE, left, right);
                  case MULTIPLY:
                    return new ArithmeticMultiplicationColumnTransformer(DOUBLE, left, right);
                  case DIVIDE:
                    return new ArithmeticDivisionColumnTransformer(DOUBLE, left, right);
                  case MODULUS:
                    return new ArithmeticModuloColumnTransformer(DOUBLE, left, right);
                  default:
                    throw new UnsupportedOperationException(
                        String.format(UNSUPPORTED_EXPRESSION, node.getOperator()));
                }
              }
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitArithmeticUnary(
      ArithmeticUnaryExpression node, Context context) {
    switch (node.getSign()) {
      case PLUS:
        return process(node.getValue(), context);
      case MINUS:
        ColumnTransformer res =
            context.cache.computeIfAbsent(
                node,
                n -> {
                  if (context.hasSeen.containsKey(node)) {
                    IdentityColumnTransformer identity =
                        new IdentityColumnTransformer(
                            DOUBLE, context.originSize + context.commonTransformerList.size());
                    ColumnTransformer columnTransformer = context.hasSeen.get(node);
                    columnTransformer.addReferenceCount();
                    context.commonTransformerList.add(columnTransformer);
                    context.leafList.add(identity);
                    context.inputDataTypes.add(TSDataType.DOUBLE);
                    return identity;
                  } else {
                    ColumnTransformer childColumnTransformer = process(node.getValue(), context);
                    return new ArithmeticNegationColumnTransformer(DOUBLE, childColumnTransformer);
                  }
                });
        res.addReferenceCount();
        return res;
      default:
        throw new UnsupportedOperationException("Unknown sign: " + node.getSign());
    }
  }

  @Override
  protected ColumnTransformer visitBetweenPredicate(BetweenPredicate node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitCast(Cast node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitBooleanLiteral(BooleanLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      BOOLEAN,
                      new BooleanColumn(1, Optional.empty(), new boolean[] {node.getValue()}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitBinaryLiteral(BinaryLiteral node, Context context) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected ColumnTransformer visitStringLiteral(StringLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      TEXT,
                      new BinaryColumn(
                          1,
                          Optional.empty(),
                          new Binary[] {new Binary(node.getValue(), TSFileConfig.STRING_CHARSET)}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitLongLiteral(LongLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      INT64,
                      new LongColumn(1, Optional.empty(), new long[] {node.getParsedValue()}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitDoubleLiteral(DoubleLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      DOUBLE,
                      new DoubleColumn(1, Optional.empty(), new double[] {node.getValue()}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitDecimalLiteral(DecimalLiteral node, Context context) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected ColumnTransformer visitNullLiteral(NullLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              NullColumnTransformer columnTransformer = new NullColumnTransformer();
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitComparisonExpression(
      ComparisonExpression node, Context context) {
    // fixme why using computeIfAbsent throw npe
    ColumnTransformer comparisonTransformer;
    if (!context.cache.containsKey(node)) {
      comparisonTransformer = getColumnTransformer(node, context);
    } else {
      comparisonTransformer = getColumnTransformer(node, context);
      context.cache.put(node, comparisonTransformer);
    }
    comparisonTransformer.addReferenceCount();
    return comparisonTransformer;
  }

  private ColumnTransformer getColumnTransformer(ComparisonExpression node, Context context) {
    if (context.hasSeen.containsKey(node)) {
      IdentityColumnTransformer identity =
          new IdentityColumnTransformer(
              BOOLEAN, context.originSize + context.commonTransformerList.size());
      ColumnTransformer columnTransformer = context.hasSeen.get(node);
      columnTransformer.addReferenceCount();
      context.commonTransformerList.add(columnTransformer);
      context.leafList.add(identity);
      context.inputDataTypes.add(TSDataType.BOOLEAN);
      return identity;
    } else {
      ColumnTransformer left = process(node.getLeft(), context);
      ColumnTransformer right = process(node.getRight(), context);
      switch (node.getOperator()) {
        case EQUAL:
          return new CompareEqualToColumnTransformer(BOOLEAN, left, right);
        case NOT_EQUAL:
          return new CompareNonEqualColumnTransformer(BOOLEAN, left, right);
        case GREATER_THAN:
          return new CompareGreaterThanColumnTransformer(BOOLEAN, left, right);
        case GREATER_THAN_OR_EQUAL:
          return new CompareGreaterEqualColumnTransformer(BOOLEAN, left, right);
        case LESS_THAN:
          return new CompareLessThanColumnTransformer(BOOLEAN, left, right);
        case LESS_THAN_OR_EQUAL:
          return new CompareLessEqualColumnTransformer(BOOLEAN, left, right);
        default:
          throw new UnsupportedOperationException(
              String.format(UNSUPPORTED_EXPRESSION, node.getOperator()));
      }
    }
  }

  @Override
  protected ColumnTransformer visitCurrentDatabase(CurrentDatabase node, Context context) {
    Optional<String> currentDatabase = context.sessionInfo.getDatabaseName();
    ColumnTransformer res;
    res =
        currentDatabase
            .map(
                s ->
                    context.cache.computeIfAbsent(
                        node,
                        e -> {
                          ConstantColumnTransformer columnTransformer =
                              new ConstantColumnTransformer(
                                  TEXT,
                                  new BinaryColumn(
                                      1,
                                      Optional.empty(),
                                      new Binary[] {new Binary(s, TSFileConfig.STRING_CHARSET)}));
                          context.leafList.add(columnTransformer);
                          return columnTransformer;
                        }))
            .orElseGet(
                () ->
                    context.cache.computeIfAbsent(
                        node,
                        e -> {
                          NullColumnTransformer columnTransformer = new NullColumnTransformer();
                          context.leafList.add(columnTransformer);
                          return columnTransformer;
                        }));
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitCurrentTime(CurrentTime node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitCurrentUser(CurrentUser node, Context context) {
    String currentUser = context.sessionInfo.getUserName();
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      TEXT,
                      new BinaryColumn(
                          1,
                          Optional.empty(),
                          new Binary[] {new Binary(currentUser, TSFileConfig.STRING_CHARSET)}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitFunctionCall(FunctionCall node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitInPredicate(InPredicate node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            n -> {
              if (context.hasSeen.containsKey(node)) {
                IdentityColumnTransformer identity =
                    new IdentityColumnTransformer(
                        BOOLEAN, context.originSize + context.commonTransformerList.size());
                ColumnTransformer columnTransformer = context.hasSeen.get(node);
                columnTransformer.addReferenceCount();
                context.commonTransformerList.add(columnTransformer);
                context.leafList.add(identity);
                context.inputDataTypes.add(TSDataType.BOOLEAN);
                return identity;
              } else {
                ColumnTransformer childColumnTransformer = process(node.getValue(), context);
                InListExpression inListExpression = (InListExpression) node.getValueList();
                List<Expression> expressionList = inListExpression.getValues();
                List<Literal> values = new ArrayList<>();
                for (Expression expression : expressionList) {
                  checkArgument(expression instanceof Literal);
                  values.add((Literal) expression);
                }
                return new InColumnTransformer(BOOLEAN, childColumnTransformer, values);
              }
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitNotExpression(NotExpression node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            n -> {
              if (context.hasSeen.containsKey(node)) {
                IdentityColumnTransformer identity =
                    new IdentityColumnTransformer(
                        BOOLEAN, context.originSize + context.commonTransformerList.size());
                ColumnTransformer columnTransformer = context.hasSeen.get(node);
                columnTransformer.addReferenceCount();
                context.commonTransformerList.add(columnTransformer);
                context.leafList.add(identity);
                context.inputDataTypes.add(TSDataType.BOOLEAN);
                return identity;
              } else {
                ColumnTransformer childColumnTransformer = process(node.getValue(), context);
                return new LogicNotColumnTransformer(BOOLEAN, childColumnTransformer);
              }
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitLikePredicate(LikePredicate node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            n -> {
              if (context.hasSeen.containsKey(node)) {
                IdentityColumnTransformer identity =
                    new IdentityColumnTransformer(
                        BOOLEAN, context.originSize + context.commonTransformerList.size());
                ColumnTransformer columnTransformer = context.hasSeen.get(node);
                columnTransformer.addReferenceCount();
                context.commonTransformerList.add(columnTransformer);
                context.leafList.add(identity);
                context.inputDataTypes.add(TSDataType.BOOLEAN);
                return identity;
              } else {
                ColumnTransformer childColumnTransformer = process(node.getValue(), context);
                return new RegularColumnTransformer(
                    BOOLEAN,
                    childColumnTransformer,
                    compileRegex(
                        parseLikePatternToRegex(((StringLiteral) node.getPattern()).getValue())));
              }
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            n -> {
              if (context.hasSeen.containsKey(node)) {
                IdentityColumnTransformer identity =
                    new IdentityColumnTransformer(
                        BOOLEAN, context.originSize + context.commonTransformerList.size());
                ColumnTransformer columnTransformer = context.hasSeen.get(node);
                columnTransformer.addReferenceCount();
                context.commonTransformerList.add(columnTransformer);
                context.leafList.add(identity);
                context.inputDataTypes.add(TSDataType.BOOLEAN);
                return identity;
              } else {
                ColumnTransformer childColumnTransformer = process(node.getValue(), context);
                return new IsNullColumnTransformer(BOOLEAN, childColumnTransformer, true);
              }
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitIsNullPredicate(IsNullPredicate node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            n -> {
              if (context.hasSeen.containsKey(node)) {
                IdentityColumnTransformer identity =
                    new IdentityColumnTransformer(
                        BOOLEAN, context.originSize + context.commonTransformerList.size());
                ColumnTransformer columnTransformer = context.hasSeen.get(node);
                columnTransformer.addReferenceCount();
                context.commonTransformerList.add(columnTransformer);
                context.leafList.add(identity);
                context.inputDataTypes.add(TSDataType.BOOLEAN);
                return identity;
              } else {
                ColumnTransformer childColumnTransformer = process(node.getValue(), context);
                return new IsNullColumnTransformer(BOOLEAN, childColumnTransformer, false);
              }
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitLogicalExpression(LogicalExpression node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            n -> {
              if (context.hasSeen.containsKey(node)) {
                IdentityColumnTransformer identity =
                    new IdentityColumnTransformer(
                        BOOLEAN, context.originSize + context.commonTransformerList.size());
                ColumnTransformer columnTransformer = context.hasSeen.get(node);
                columnTransformer.addReferenceCount();
                context.commonTransformerList.add(columnTransformer);
                context.leafList.add(identity);
                context.inputDataTypes.add(TSDataType.BOOLEAN);
                return identity;
              } else {
                List<ColumnTransformer> children =
                    node.getChildren().stream()
                        .map(c -> process(c, context))
                        .collect(Collectors.toList());
                switch (node.getOperator()) {
                  case OR:
                    return new LogicalOrMultiColumnTransformer(BOOLEAN, children);
                  case AND:
                    return new LogicalAndMultiColumnTransformer(BOOLEAN, children);
                  default:
                    throw new UnsupportedOperationException(
                        String.format(UNSUPPORTED_EXPRESSION, node.getOperator()));
                }
              }
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitSymbolReference(SymbolReference node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              int valueIdx =
                  context.inputLocations.get(Symbol.from(node)).get(0).getValueColumnIndex();
              LeafColumnTransformer leafColumnTransformer;
              if (valueIdx == -1) {
                leafColumnTransformer = new TimeColumnTransformer(INT64);
              } else {
                leafColumnTransformer =
                    new IdentityColumnTransformer(context.getType(node), valueIdx);
              }

              context.leafList.add(leafColumnTransformer);
              return leafColumnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitSimpleCaseExpression(
      SimpleCaseExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitSearchedCaseExpression(
      SearchedCaseExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitTrim(Trim node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitIfExpression(IfExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitNullIfExpression(NullIfExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitCoalesceExpression(CoalesceExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  public static class Context {

    private final SessionInfo sessionInfo;

    // LeafColumnTransformer for LeafOperand
    private final List<LeafColumnTransformer> leafList;

    // Index of input column
    private final Map<Symbol, List<InputLocation>> inputLocations;

    // cache for constructing ColumnTransformer tree
    private final Map<Expression, ColumnTransformer> cache;

    // Sub expressions that has been seen in filter
    private final Map<Expression, ColumnTransformer> hasSeen;

    // Common Transformer between filter and project
    private final List<ColumnTransformer> commonTransformerList;

    private final List<TSDataType> inputDataTypes;

    private final int originSize;

    private final TypeProvider typeProvider;

    public Context(
        SessionInfo sessionInfo,
        List<LeafColumnTransformer> leafList,
        Map<Symbol, List<InputLocation>> inputLocations,
        Map<Expression, ColumnTransformer> cache,
        Map<Expression, ColumnTransformer> hasSeen,
        List<ColumnTransformer> commonTransformerList,
        List<TSDataType> inputDataTypes,
        int originSize,
        TypeProvider typeProvider) {
      this.sessionInfo = sessionInfo;
      this.leafList = leafList;
      this.inputLocations = inputLocations;
      this.cache = cache;
      this.hasSeen = hasSeen;
      this.commonTransformerList = commonTransformerList;
      this.inputDataTypes = inputDataTypes;
      this.originSize = originSize;
      this.typeProvider = typeProvider;
    }

    public Type getType(SymbolReference symbolReference) {
      return typeProvider.getTableModelType(Symbol.from(symbolReference));
    }
  }
}
